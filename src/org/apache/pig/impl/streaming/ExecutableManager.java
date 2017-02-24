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
package org.apache.pig.impl.streaming;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.streaming.InputHandler.InputType;
import org.apache.pig.impl.streaming.OutputHandler.OutputType;
import org.apache.pig.impl.util.UDFContext;

/**
 * {@link ExecutableManager} manages an external executable which processes data
 * in a Pig query.
 *
 * The <code>ExecutableManager</code> is responsible for startup/teardown of
 * the external process and also for managing it. It feeds input records to the
 * executable via it's <code>stdin</code>, collects the output records from
 * the <code>stdout</code> and also diagnostic information from the
 * <code>stdout</code>.
 */
public class ExecutableManager {
    private static final Log LOG = LogFactory.getLog(ExecutableManager.class);
    private static final int SUCCESS = 0;
    private static final Result EOS_RESULT = new Result(POStatus.STATUS_EOS, null);

    protected StreamingCommand command; // Streaming command to be run

    Process process; // Handle to the process
    protected int exitCode = -127; // Exit code of the process

    protected DataOutputStream stdin; // stdin of the process
    ProcessInputThread stdinThread; // thread to send input to process

    ProcessOutputThread stdoutThread; // thread to get process stdout
    InputStream stdout; // stdout of the process

    ProcessErrorThread stderrThread; // thread to get process stderr
    InputStream stderr; // stderr of the process

    // Input/Output handlers
    InputHandler inputHandler;
    OutputHandler outputHandler;

    // Statistics
    protected long inputRecords = 0;
    protected long inputBytes = 0;
    protected long outputRecords = 0;
    protected long outputBytes = 0;

    protected volatile Throwable outerrThreadsError;
    private POStream poStream;
    private ProcessInputThread fileInputThread;

    /**
     * Create a new {@link ExecutableManager}.
     */
    public ExecutableManager() {
    }

    /**
     * Configure and initialize the {@link ExecutableManager}.
     *
     * @param stream POStream operator
     * @throws IOException
     * @throws ExecException
     */
    public void configure(POStream stream) throws IOException, ExecException {
        this.poStream = stream;
        this.command = stream.getCommand();

        // Create the input/output handlers
        this.inputHandler = HandlerFactory.createInputHandler(command);
        this.outputHandler = HandlerFactory.createOutputHandler(command);
    }

    /**
     * Close and cleanup the {@link ExecutableManager}.
     * @throws IOException
     */
    public void close() throws IOException {
        // Close the InputHandler, which in some cases lets the process
        // terminate
        inputHandler.close(process);

        // Check if we need to start the process now ...
        if (inputHandler.getInputType() == InputType.ASYNCHRONOUS) {
            exec();
        }

        // Wait for the process to exit
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException ie) {
            LOG.error("Unexpected exception while waiting for streaming binary to complete", ie);
            killProcess(process);
        }

        // Wait for stdout thread to complete
        try {
            if (stdoutThread != null) {
                stdoutThread.join(0);
            }
            stdoutThread = null;
        } catch (InterruptedException ie) {
            LOG.error("Unexpected exception while waiting for output thread for streaming binary to complete", ie);
            killProcess(process);
        }

        // Wait for stderr thread to complete
        try {
            if (stderrThread != null) {
                stderrThread.join(0);
            }
            stderrThread = null;
        } catch (InterruptedException ie) {
            LOG.error("Unexpected exception while waiting for input thread for streaming binary to complete", ie);
            killProcess(process);
        }

        LOG.debug("Process exited with: " + exitCode);
        if (exitCode != SUCCESS) {
            LOG.error(command + " failed with exit status: "
                    + exitCode);
        }

        if (outputHandler.getOutputType() == OutputType.ASYNCHRONOUS) {

            // Trigger the outputHandler
            outputHandler.bindTo("", null, 0, -1);

            // start thread to process output from executable's stdout
            stdoutThread = new ProcessOutputThread(outputHandler, poStream);
            stdoutThread.start();
        }

        // Check if there was a problem with the managed process
        if (outerrThreadsError != null) {
            LOG.error("Output/Error thread failed with: "
                    + outerrThreadsError);
        }

    }

    /**
     *  Helper function to close input and output streams
     *  to the process and kill it
     * @param process the process to be killed
     * @throws IOException
     */
    private void killProcess(Process process) throws IOException {
        if (process != null) {
            inputHandler.close(process);
            outputHandler.close();
            process.destroy();
        }
    }

    /**
     * Start execution of the external process.
     *
     * This takes care of setting up the environment of the process and also
     * starts ProcessErrorThread to process the <code>stderr</code> of
     * the managed process.
     *
     * @throws IOException
     */
    protected void exec() throws IOException {
        ProcessBuilder processBuilder = StreamingUtil.createProcess(this.command);
        process = processBuilder.start();
        LOG.debug("Started the process for command: " + command);

        // Pick up the process' stderr stream and start the thread to
        // process the stderr stream
        stderr = new DataInputStream(new BufferedInputStream(process
                .getErrorStream()));
        stderrThread = new ProcessErrorThread();
        stderrThread.start();

        // Check if we need to handle the process' stdout directly
        if (outputHandler.getOutputType() == OutputType.SYNCHRONOUS) {
            // Get hold of the stdout of the process
            stdout = new DataInputStream(new BufferedInputStream(process
                    .getInputStream()));

            // Bind the stdout to the OutputHandler
            outputHandler.bindTo("", new BufferedPositionedInputStream(stdout),
                    0, Long.MAX_VALUE);

            // start thread to process output from executable's stdout
            stdoutThread = new ProcessOutputThread(outputHandler, poStream);
            stdoutThread.start();
        }
    }

    /**
     * Start execution of the {@link ExecutableManager}.
     *
     * @throws IOException
     */
    public void run() throws IOException {
        // Check if we need to exec the process NOW ...
        if (inputHandler.getInputType() == InputType.ASYNCHRONOUS) {
            // start the thread to handle input. we pass the UDFContext to the
            // fileInputThread because when input type is asynchronous, the
            // exec() is called by fileInputThread, and it needs to access to
            // the UDFContext.
            fileInputThread = new ProcessInputThread(
                    inputHandler, poStream, UDFContext.getUDFContext());
            fileInputThread.start();

            // If Input type is ASYNCHRONOUS that means input to the
            // streaming binary is from a file - that means we cannot exec
            // the process till the input file is completely written. This
            // will be done in close() - so now we return
            return;
        }

        // Start the executable ...
        exec();
        // set up input to the executable
        stdin = new DataOutputStream(new BufferedOutputStream(process
                .getOutputStream()));
        inputHandler.bindTo(stdin);

        // Start the thread to send input to the executable's stdin
        stdinThread = new ProcessInputThread(inputHandler, poStream, null);
        stdinThread.start();
    }

    /**
     * The thread which consumes input from POStream's binaryInput queue
     * and feeds it to the the Process
     */
    class ProcessInputThread extends Thread {

        InputHandler inputHandler;
        private POStream poStream;
        private UDFContext udfContext;
        private BlockingQueue<Result> binaryInputQueue;

        ProcessInputThread(InputHandler inputHandler, POStream poStream, UDFContext udfContext) {
            setDaemon(true);
            this.inputHandler = inputHandler;
            this.poStream = poStream;
            // a copy of UDFContext passed from the ExecutableManager thread
            this.udfContext = udfContext;
            // the input queue from where this thread will read
            // input tuples
            this.binaryInputQueue = poStream.getBinaryInputQueue();
        }

        @Override
        public void run() {
            // If input type is asynchronous, set the udfContext of the current
            // thread to the copy of ExecutableManager thread's udfContext. This
            // is necessary because the exec() method is called by the current
            // thread (fileInputThread) instead of the ExecutableManager thread.
            if (inputHandler.getInputType() == InputType.ASYNCHRONOUS && udfContext != null) {
                UDFContext.setUdfContext(udfContext);
            }
            try {
                // Read tuples from the previous operator in the pipeline
                // and pass it to the executable
                while (true) {
                    Result inp = null;
                    inp = binaryInputQueue.take();
                    synchronized (poStream) {
                        // notify waiting producer
                        // the if check is to keep "findbugs"
                        // happy
                        if(inp != null)
                            poStream.notifyAll();
                    }
                    // We should receive an EOP only when *ALL* input
                    // for this process has already been sent and no
                    // more input is expected
                    if (inp != null && inp.returnStatus == POStatus.STATUS_EOP) {
                        // signal cleanup in ExecutableManager
                        close();
                        return;
                    }
                    if (inp != null && inp.returnStatus == POStatus.STATUS_OK) {
                        // Check if there was a problem with the managed process
                        if (outerrThreadsError != null) {
                            throw new IOException(
                                    "Output/Error thread failed with: "
                                            + outerrThreadsError);
                        }

                        // Pass the serialized tuple to the executable via the
                        // InputHandler
                        Tuple t = null;
                        try {
                            t = (Tuple) inp.result;
                            inputHandler.putNext(t);
                        } catch (IOException e) {
                            // if input type is synchronous then it could
                            // be related to the process terminating
                            if(inputHandler.getInputType() == InputType.SYNCHRONOUS) {
                                LOG.warn("Exception while trying to write to stream binary's input", e);
                                // could be because the process
                                // died OR closed the input stream
                                // we will only call close() here and not
                                // worry about deducing whether the process died
                                // normally or abnormally - if there was any real
                                // issue the ProcessOutputThread should see
                                // a non zero exit code from the process and send
                                // a POStatus.STATUS_ERR back - what if we got
                                // an IOException because there was only an issue with
                                // writing to input of the binary - hmm..hope that means
                                // the process died abnormally!!
                                close();
                                return;
                            } else {
                                // asynchronous case - then this is a real exception
                                LOG.error("Exception while trying to write to stream binary's input", e);
                                // send POStatus.STATUS_ERR to POStream to signal the error
                                // Generally the ProcessOutputThread would do this but now
                                // we should do it here since neither the process nor the
                                // ProcessOutputThread will ever be spawned
                                Result res = new Result(POStatus.STATUS_ERR,
                                        "Exception while trying to write to stream binary's input" + e.getMessage());
                                sendOutput(poStream.getBinaryOutputQueue(), res);
                                throw e;
                            }
                        }
                        inputBytes += t.getMemorySize();
                        inputRecords++;
                    }
                }
            } catch (Throwable t) {
                // Note that an error occurred
                outerrThreadsError = t;
                LOG.error( "Error while reading from POStream and " +
                           "passing it to the streaming process", t);
                try {
                    killProcess(process);
                } catch (IOException ioe) {
                    LOG.warn(ioe);
                }
            }
        }
    }

    private void sendOutput(BlockingQueue<Result> binaryOutputQueue, Result res) {
        try {
            binaryOutputQueue.put(res);
        } catch (InterruptedException e) {
            LOG.error("Error while sending binary output to POStream", e);
        }
        synchronized (poStream) {
            // notify waiting consumer
            // the if is to satisfy "findbugs"
            if(res != null) {
                poStream.notifyAll();
            }
        }
    }

    /**
     * The thread which gets output from the streaming binary and puts it onto
     * the binary output Queue of POStream
     */
    class ProcessOutputThread extends Thread {

        OutputHandler outputHandler;
        private BlockingQueue<Result> binaryOutputQueue;

        ProcessOutputThread(OutputHandler outputHandler, POStream poStream) {
            setDaemon(true);
            this.outputHandler = outputHandler;
            // the output queue where this thread will put
            // output tuples for POStream
            this.binaryOutputQueue = poStream.getBinaryOutputQueue();
        }

        @Override
        public void run() {
            try {
                // Read tuples from the executable and send it to
                // Queue of POStream
                Tuple tuple = null;
                while ((tuple = outputHandler.getNext()) != null) {
                    processOutput(tuple);
                    outputBytes += tuple.getMemorySize();
                }
                // output from binary is done
                processOutput(null);
                outputHandler.close();
            } catch (Throwable t) {
                // Note that an error occurred
                outerrThreadsError = t;
                LOG.error("Caught Exception in OutputHandler of Streaming binary, " +
                        "sending error signal to pipeline", t);
                // send ERROR to POStream
                try {
                    Result res = new Result();
                    res.result = "Error reading output from Streaming binary:" +
                            "'" + command.toString() + "':" + t.getMessage();
                    res.returnStatus = POStatus.STATUS_ERR;
                    sendOutput(binaryOutputQueue, res);
                    killProcess(process);
                } catch (Exception e) {
                    LOG.error("Error while trying to signal Error status to pipeline", e);
                }
            }
        }

        void processOutput(Tuple t) {
            Result res = new Result();

            if (t != null) {
                // we have a valid tuple to pass back
                res.result = t;
                res.returnStatus = POStatus.STATUS_OK;
                outputRecords++;
            } else {
                // t == null means end of output from
                // binary - wait for the process to exit
                // and harvest exit code
                try {
                    exitCode = process.waitFor();
                } catch (InterruptedException ie) {
                    try {
                        killProcess(process);
                    } catch (IOException e) {
                        LOG.warn("Exception trying to kill process while processing null output " +
                                "from binary", e);

                    }
                    // signal error
                    String errMsg = "Failure while waiting for process (" + command.toString() + ")" +
                            ie.getMessage();
                    LOG.error(errMsg, ie);
                    res.result = errMsg;
                    res.returnStatus = POStatus.STATUS_ERR;
                    sendOutput(binaryOutputQueue, res);
                    return;
                }
                if(exitCode == 0) {
                    // signal EOS (End Of Stream output)
                    res = EOS_RESULT;
                } else {
                    // signal Error

                    String errMsg = "'" + command.toString() + "'" + " failed with exit status: "
                            + exitCode;
                    LOG.error(errMsg);
                    res.result = errMsg;
                    res.returnStatus = POStatus.STATUS_ERR;
                }
            }
            sendOutput(binaryOutputQueue, res);

        }
    }



    /**
     * Workhorse to process the stderr stream of the managed process.
     *
     * By default <code>ExecuatbleManager</code> just sends out the received
     * error message to the <code>stderr</code> of itself.
     *
     * @param error
     *            error message from the managed process.
     */
    protected void processError(String error) {
        // Just send it out to our stderr
        System.err.print(error);
    }

    class ProcessErrorThread extends Thread {

        public ProcessErrorThread() {
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                String error;
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(stderr));
                while ((error = reader.readLine()) != null) {
                    processError(error + "\n");
                }

                if (stderr != null) {
                    stderr.close();
                    LOG.debug("ProcessErrorThread done");
                }
            } catch (Throwable t) {
                // Note that an error occurred
                outerrThreadsError = t;

                LOG.error(t);
                try {
                    if (stderr != null) {
                        stderr.close();
                    }
                } catch (IOException ioe) {
                    LOG.warn(ioe);
                }
                throw new RuntimeException(t);
            }
        }
    }

}
