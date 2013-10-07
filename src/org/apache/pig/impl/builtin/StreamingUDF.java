/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.impl.builtin;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.ExecTypeProvider;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.streaming.InputHandler;
import org.apache.pig.impl.streaming.OutputHandler;
import org.apache.pig.impl.streaming.PigStreamingUDF;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingUDFException;
import org.apache.pig.impl.streaming.StreamingUDFInputHandler;
import org.apache.pig.impl.streaming.StreamingUDFOutputHandler;
import org.apache.pig.impl.streaming.StreamingUDFOutputSchemaException;
import org.apache.pig.impl.streaming.StreamingUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.apache.pig.scripting.ScriptingOutputCapturer;

import com.google.common.base.Charsets;

public class StreamingUDF extends EvalFunc<Object> {
    private static final Log log = LogFactory.getLog(StreamingUDF.class);

    private static final String PYTHON_CONTROLLER_JAR_PATH = "/python/streaming/controller.py"; //Relative to root of pig jar.
    private static final String PYTHON_PIG_UTIL_PATH = "/python/streaming/pig_util.py"; //Relative to root of pig jar.

    //Indexes for arguments being passed to external process
    private static final int UDF_LANGUAGE = 0;
    private static final int PATH_TO_CONTROLLER_FILE = 1;
    private static final int UDF_FILE_NAME = 2; //Name of file where UDF function is defined
    private static final int UDF_FILE_PATH = 3; //Path to directory containing file where UDF function is defined
    private static final int UDF_NAME = 4; //Name of UDF function being called.
    private static final int PATH_TO_FILE_CACHE = 5; //Directory where required files (like pig_util) are cached on cluster nodes.
    private static final int STD_OUT_OUTPUT_PATH = 6; //File for output from when user writes to standard output.
    private static final int STD_ERR_OUTPUT_PATH = 7; //File for output from when user writes to standard error.
    private static final int CONTROLLER_LOG_FILE_PATH = 8; //Controller log file logs progress through the controller script not user code.
    private static final int IS_ILLUSTRATE = 9; //Controller captures output differently in illustrate vs running.
    
    private String language;
    private String filePath;
    private String funcName;
    private Schema schema;
    private ExecType execType;
    private String isIllustrate;
    
    private boolean initialized = false;
    private ScriptingOutputCapturer soc;

    private Process process; // Handle to the external process
    private ProcessErrorThread stderrThread; // thread to get process stderr
    private ProcessInputThread stdinThread; // thread to send input to process
    private ProcessOutputThread stdoutThread; //thread to read output from process
    
    private InputHandler inputHandler;
    private OutputHandler outputHandler;

    private BlockingQueue<Tuple> inputQueue;
    private BlockingQueue<Object> outputQueue;

    private DataOutputStream stdin; // stdin of the process
    private InputStream stdout; // stdout of the process
    private InputStream stderr; // stderr of the process

    private static final Object ERROR_OUTPUT = new Object();
    private static final Object NULL_OBJECT = new Object(); //BlockingQueue can't have null.  Use place holder object instead.
    
    private volatile StreamingUDFException outerrThreadsError;
    
    public static final String TURN_ON_OUTPUT_CAPTURING = "TURN_ON_OUTPUT_CAPTURING";

    public StreamingUDF(String language, 
                        String filePath, String funcName, 
                        String outputSchemaString, String schemaLineNumber,
                        String execType, String isIllustrate)
                                throws StreamingUDFOutputSchemaException, ExecException {
        this.language = language;
        this.filePath = filePath;
        this.funcName = funcName;
        try {
            this.schema = Utils.getSchemaFromString(outputSchemaString);
            //ExecTypeProvider.fromString doesn't seem to load the ExecTypes in 
            //mapreduce mode so we'll try to figure out the exec type ourselves.
            if (execType.equals("local")) {
                this.execType = ExecType.LOCAL;
            } else if (execType.equals("mapreduce")) {
                this.execType = ExecType.MAPREDUCE;
            } else {
                //Not sure what exec type - try to get it from the string.
                this.execType = ExecTypeProvider.fromString(execType);
            }
        } catch (ParserException pe) {
            throw new StreamingUDFOutputSchemaException(pe.getMessage(), Integer.valueOf(schemaLineNumber));
        } catch (IOException ioe) {
            String errorMessage = "Invalid exectype passed to StreamingUDF. Should be local or mapreduce";
            log.error(errorMessage, ioe);
            throw new ExecException(errorMessage, ioe);
        }
        this.isIllustrate = isIllustrate;
    }
    
    @Override
    public Object exec(Tuple input) throws IOException {
        if (!initialized) {
            initialize();
            initialized = true;
        }
        return getOutput(input);
    }

    private void initialize() throws ExecException, IOException {
        inputQueue = new ArrayBlockingQueue<Tuple>(1);
        outputQueue = new ArrayBlockingQueue<Object>(2);
        soc = new ScriptingOutputCapturer(execType);
        startUdfController();
        createInputHandlers();
        setStreams();
        startThreads();
    }

    private StreamingCommand startUdfController() throws IOException {
        StreamingCommand sc = new StreamingCommand(null, constructCommand());
        ProcessBuilder processBuilder = StreamingUtil.createProcess(sc);
        process = processBuilder.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new ProcessKiller() ) );
        return sc;
    }

    private String[] constructCommand() throws IOException {
        String[] command = new String[10];
        Configuration conf = UDFContext.getUDFContext().getJobConf();

        String jarPath = conf.get("mapred.jar");
        String jobDir;
        if (jarPath != null) {
            jobDir = new File(jarPath).getParent();
        } else {
            jobDir = "";
        }
        
        String standardOutputRootWriteLocation = soc.getStandardOutputRootWriteLocation();
        String controllerLogFileName, outFileName, errOutFileName;

        if (execType.isLocal()) {
            controllerLogFileName = standardOutputRootWriteLocation + funcName + "_python.log";
            outFileName = standardOutputRootWriteLocation + "cpython_" + funcName + "_" + ScriptingOutputCapturer.getRunId() + ".out";
            errOutFileName = standardOutputRootWriteLocation + "cpython_" + funcName + "_" + ScriptingOutputCapturer.getRunId() + ".err";
        } else {
            controllerLogFileName = standardOutputRootWriteLocation + funcName + "_python.log";
            outFileName = standardOutputRootWriteLocation + funcName + ".out";
            errOutFileName = standardOutputRootWriteLocation + funcName + ".err";
        }

        soc.registerOutputLocation(funcName, outFileName);

        command[UDF_LANGUAGE] = language;
        command[PATH_TO_CONTROLLER_FILE] = getControllerPath(jobDir);
        int lastSeparator = filePath.lastIndexOf(File.separator) + 1;
        command[UDF_FILE_NAME] = filePath.substring(lastSeparator);
        command[UDF_FILE_PATH] = lastSeparator <= 0 ? 
                "." : 
                filePath.substring(0, lastSeparator - 1);
        command[UDF_NAME] = funcName;
        command[PATH_TO_FILE_CACHE] = "\"" + jobDir + filePath.substring(0, lastSeparator) + "\"";
        command[STD_OUT_OUTPUT_PATH] = outFileName;
        command[STD_ERR_OUTPUT_PATH] = errOutFileName;
        command[CONTROLLER_LOG_FILE_PATH] = controllerLogFileName;
        command[IS_ILLUSTRATE] = isIllustrate;
        return command;
    }

    private void createInputHandlers() throws ExecException, FrontendException {
        PigStreamingUDF serializer = new PigStreamingUDF();
        this.inputHandler = new StreamingUDFInputHandler(serializer);
        PigStreamingUDF deserializer = new PigStreamingUDF(schema.getField(0));
        this.outputHandler = new StreamingUDFOutputHandler(deserializer);
    }

    private void setStreams() throws IOException { 
        stdout = new DataInputStream(new BufferedInputStream(process
                .getInputStream()));
        outputHandler.bindTo("", new BufferedPositionedInputStream(stdout),
                0, Long.MAX_VALUE);
        
        stdin = new DataOutputStream(new BufferedOutputStream(process
                .getOutputStream()));
        inputHandler.bindTo(stdin); 
        
        stderr = new DataInputStream(new BufferedInputStream(process
                .getErrorStream()));
    }

    private void startThreads() {
        stdinThread = new ProcessInputThread();
        stdinThread.start();
        
        stdoutThread = new ProcessOutputThread();
        stdoutThread.start();
        
        stderrThread = new ProcessErrorThread();
        stderrThread.start();
    }

    /**
     * Find the path to the controller file for the streaming language.
     *
     * First check path to job jar and if the file is not found (like in the
     * case of running hadoop in standalone mode) write the necessary files
     * to temporary files and return that path.
     *
     * @param language
     * @param jarPath
     * @return
     * @throws IOException
     */
    private String getControllerPath(String jarPath) throws IOException {
        if (language.toLowerCase().equals("python")) {
            String controllerPath = jarPath + PYTHON_CONTROLLER_JAR_PATH;
            File controller = new File(controllerPath);
            if (!controller.exists()) {
                File controllerFile = File.createTempFile("controller", ".py");
                InputStream pythonControllerStream = Launcher.class.getResourceAsStream(PYTHON_CONTROLLER_JAR_PATH);
                try {
                    FileUtils.copyInputStreamToFile(pythonControllerStream, controllerFile);
                } finally {
                    pythonControllerStream.close();
                }
                controllerFile.deleteOnExit();
                File pigUtilFile = new File(controllerFile.getParent() + "/pig_util.py");
                pigUtilFile.deleteOnExit();
                InputStream pythonUtilStream = Launcher.class.getResourceAsStream(PYTHON_PIG_UTIL_PATH);
                try {
                    FileUtils.copyInputStreamToFile(pythonUtilStream, pigUtilFile);
                } finally {
                    pythonUtilStream.close();
                }
                controllerPath = controllerFile.getAbsolutePath();
            }
            return controllerPath;
        } else {
            throw new ExecException("Invalid language: " + language);
        }
    }

    /**
     * Returns a list of file names (relative to root of pig jar) of files that need to be
     * included in the jar shipped to the cluster.
     *
     * Will need to be smarter as more languages are added and the controller files are large.
     *
     * @return
     */
    public static List<String> getResourcesForJar() {
        List<String> files = new ArrayList<String>();
        files.add(PYTHON_CONTROLLER_JAR_PATH);
        files.add(PYTHON_PIG_UTIL_PATH);
        return files;
    }

    private Object getOutput(Tuple input) throws ExecException {
        if (outputQueue == null) {
            throw new ExecException("Process has already been shut down.  No way to retrieve output for input: " + input);
        }

        if (ScriptingOutputCapturer.isClassCapturingOutput() && 
                !soc.isInstanceCapturingOutput()) {
            Tuple t = TupleFactory.getInstance().newTuple(TURN_ON_OUTPUT_CAPTURING);
            try {
                inputQueue.put(t);
            } catch (InterruptedException e) {
                throw new ExecException("Failed adding capture input flag to inputQueue");
            }
            soc.setInstanceCapturingOutput(true);
        }

        try {
            if (this.getInputSchema() == null || this.getInputSchema().size() == 0) {
                //When nothing is passed into the UDF the tuple 
                //being sent is the full tuple for the relation.
                //We want it to be nothing (since that's what the user wrote).
                input = TupleFactory.getInstance().newTuple(0);
            }
            inputQueue.put(input);
        } catch (Exception e) {
            throw new ExecException("Failed adding input to inputQueue", e);
        }
        Object o = null;
        try {
            if (outputQueue != null) {
                o = outputQueue.take();
                if (o == NULL_OBJECT) {
                    o = null;
                }
            }
        } catch (Exception e) {
            throw new ExecException("Problem getting output", e);
        }

        if (o == ERROR_OUTPUT) {
            outputQueue = null;
            if (outerrThreadsError == null) {
                outerrThreadsError = new StreamingUDFException(this.language, "Problem with streaming udf.  Can't recreate exception.");
            }
            throw outerrThreadsError;
        }

        return o;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return this.schema;
    }

    /**
     * The thread which consumes input and feeds it to the the Process
     */
    class ProcessInputThread extends Thread {
        ProcessInputThread() {
            setDaemon(true);
        }

        public void run() {
            try {
                log.debug("Starting PIT");
                while (true) {
                    Tuple inputTuple = inputQueue.take();
                    inputHandler.putNext(inputTuple);
                    try {
                        stdin.flush();
                    } catch(Exception e) {
                        return;
                    }
                }
            } catch (Exception e) {
                log.error(e);
            }
        }
    }
    
    private static final int WAIT_FOR_ERROR_LENGTH = 500;
    private static final int MAX_WAIT_FOR_ERROR_ATTEMPTS = 5;
    
    /**
     * The thread which consumes output from process
     */
    class ProcessOutputThread extends Thread {
        ProcessOutputThread() {
            setDaemon(true);
        }

        public void run() {
            Object o = null;
            try{
                log.debug("Starting POT");
                //StreamUDFToPig wraps object in single element tuple
                o = outputHandler.getNext().get(0);
                while (o != OutputHandler.END_OF_OUTPUT) {
                    if (o != null)
                        outputQueue.put(o);
                    else
                        outputQueue.put(NULL_OBJECT);
                    o = outputHandler.getNext().get(0);
                }
            } catch(Exception e) {
                if (outputQueue != null) {
                    try {
                        //Give error thread a chance to check the standard error output
                        //for an exception message.
                        int attempt = 0;
                        while (stderrThread.isAlive() && attempt < MAX_WAIT_FOR_ERROR_ATTEMPTS) {
                            Thread.sleep(WAIT_FOR_ERROR_LENGTH);
                            attempt++;
                        }
                        //Only write this if no other error.  Don't want to overwrite
                        //an error from the error thread.
                        if (outerrThreadsError == null) {
                            outerrThreadsError = new StreamingUDFException(language, "Error deserializing output.  Please check that the declared outputSchema for function " +
                                                                        funcName + " matches the data type being returned.", e);
                        }
                        outputQueue.put(ERROR_OUTPUT); //Need to wake main thread.
                    } catch(InterruptedException ie) {
                        log.error(ie);
                    }
                }
            }
        }
    }

    class ProcessErrorThread extends Thread {
        public ProcessErrorThread() {
            setDaemon(true);
        }

        public void run() {
            try {
                log.debug("Starting PET");
                Integer lineNumber = null;
                StringBuffer error = new StringBuffer();
                String errInput;
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(stderr, Charsets.UTF_8));
                while ((errInput = reader.readLine()) != null) {
                    //First line of error stream is usually the line number of error.
                    //If its not a number just treat it as first line of error message.
                    if (lineNumber == null) {
                        try {
                            lineNumber = Integer.valueOf(errInput);
                        } catch (NumberFormatException nfe) {
                            error.append(errInput + "\n");
                        }
                    } else {
                        error.append(errInput + "\n");
                    }
                }
                outerrThreadsError = new StreamingUDFException(language, error.toString(), lineNumber);
                if (outputQueue != null) {
                    outputQueue.put(ERROR_OUTPUT); //Need to wake main thread.
                }
                if (stderr != null) {
                    stderr.close();
                    stderr = null;
                }
            } catch (IOException e) {
                log.debug("Process Ended");
            } catch (Exception e) {
                log.error("standard error problem", e);
            }
        }
    }
    
    public class ProcessKiller implements Runnable {
        public void run() {
            process.destroy();
        }
    }
}
