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
package org.apache.pig.backend.hadoop.streaming;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapreduceExec.PigMapReduce;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.Handle;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

/**
 * {@link HadoopExecutableManager} is a specialization of 
 * {@link ExecutableManager} and provides HDFS-specific support for secondary
 * outputs, task-logs etc.
 * 
 * <code>HadoopExecutableManager</code> provides support for  secondary outputs
 * of the managed process and also persists the logs of the tasks on HDFS. 
 */
public class HadoopExecutableManager extends ExecutableManager {
    // The part-<partition> file name, similar to Hadoop's outputs
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
      NUMBER_FORMAT.setMinimumIntegerDigits(5);
      NUMBER_FORMAT.setGroupingUsed(false);
    }

    static String getOutputName(int partition) {
      return "part-" + NUMBER_FORMAT.format(partition);
    }

    JobConf job;
    
    String scriptOutputDir;
    String scriptLogDir;
    String taskId;
    
    FSDataOutputStream errorStream;
    
    boolean writeHeaderFooter = false;
    
    public HadoopExecutableManager() {}
    
    public void configure(Properties properties, StreamingCommand command, 
                          DataCollector endOfPipe) 
    throws IOException, ExecException {
        super.configure(properties, command, endOfPipe);
        
        // Chmod +x the executable
        File executable = new File(command.getExecutable());
        if (executable.isAbsolute()) {
            // we don't own it. Hope it is executable ...
        } else {
            try {
                FileUtil.chmod(executable.toString(), "a+x");
            } catch (InterruptedException ie) {
                throw new ExecException(ie);
            }
        }
        
        // Save the output directory for the Pig Script
        scriptOutputDir = properties.getProperty("pig.streaming.task.output.dir");
        scriptLogDir = properties.getProperty("pig.streaming.log.dir");
        
        // Save the taskid
        taskId = properties.getProperty("pig.streaming.task.id");
        
        // Save a copy of the JobConf
        job = PigMapReduce.getPigContext().getJobConf();
    }
    
    protected void exec() throws IOException {
        // Create the HDFS file for the stderr of the task, if necessary
        if (writeErrorToHDFS(command.getLogFilesLimit(), taskId)) {
            try {
                Path errorFile = 
                    new Path(new Path(scriptLogDir, command.getLogDir()), taskId);
                errorStream = 
                    errorFile.getFileSystem(job).create(errorFile);
            } catch (IOException ie) {
                // Don't fail the task if we couldn't save it's stderr on HDFS
                System.err.println("Failed to create stderr file of task: " +
                                   taskId + " in HDFS at " + scriptLogDir + 
                                   " with " + ie);
                errorStream = null;
            }
        }
        
        // Header for stderr file of the task
        writeDebugHeader();

        // Exec the command ...
        super.exec();
    }

    public void close() throws IOException, ExecException {
        try {
            super.close();

            // Copy the secondary outputs of the task to HDFS
            Path scriptOutputDir = new Path(this.scriptOutputDir);
            FileSystem fs = scriptOutputDir.getFileSystem(job);
            List<HandleSpec> outputSpecs = command.getHandleSpecs(Handle.OUTPUT);
            if (outputSpecs != null) {
                for (int i=1; i < outputSpecs.size(); ++i) {
                    String fileName = outputSpecs.get(i).getName();
                    try {
                        int partition = job.getInt("mapred.task.partition", -1);
                        fs.copyFromLocalFile(false, true, new Path(fileName), 
                                             new Path(
                                                     new Path(scriptOutputDir, 
                                                              fileName), 
                                                     getOutputName(partition))
                                            );
                    } catch (IOException ioe) {
                        System.err.println("Failed to save secondary output '" + 
                                           fileName + "' of task: " + taskId +
                                           " with " + ioe);
                        throw new ExecException(ioe);
                    }
                }
        }
        } finally {
            // Footer for stderr file of the task
            writeDebugFooter();
            
            // Close the stderr file on HDFS
            if (errorStream != null) {
                errorStream.close();
            }
        }
    }

    /**
     * Should the stderr data of this task be persisted on HDFS?
     * 
     * @param limit maximum number of tasks whose stderr log-files are persisted
     * @param taskId id of the task
     * @return <code>true</code> if stderr data of task should be persisted on 
     *         HDFS, <code>false</code> otherwise
     */
    private boolean writeErrorToHDFS(int limit, String taskId) {
        if (command.getPersistStderr()) {
            int tipId = TaskAttemptID.forName(taskId).getTaskID().getId();
            return tipId < command.getLogFilesLimit();
        }
        return false;
    }
    
    protected void processError(String error) {
        super.processError(error);
        
        try {
            if (errorStream != null) {
                errorStream.writeBytes(error);
            }
        } catch (IOException ioe) {
            super.processError("Failed to save error logs to HDFS with: " + 
                               ioe);
        }
    }

    private void writeDebugHeader() {
        processError("===== Task Information Header =====" );

        processError("\nCommand: " + command);
        processError("\nStart time: " + new Date(System.currentTimeMillis()));
        if (job.getBoolean("mapred.task.is.map", false)) {
            processError("\nInput-split file: " + job.get("map.input.file"));
            processError("\nInput-split start-offset: " + 
                         job.getLong("map.input.start", -1));
            processError("\nInput-split length: " + 
                         job.getLong("map.input.length", -1));
        }
        processError("\n=====          * * *          =====\n");
    }
    
    private void writeDebugFooter() {
        processError("===== Task Information Footer =====");

        processError("\nEnd time: " + new Date(System.currentTimeMillis()));
        processError("\nExit code: " + exitCode);

        List<HandleSpec> inputSpecs = command.getHandleSpecs(Handle.INPUT);
        HandleSpec inputSpec = 
            (inputSpecs != null) ? inputSpecs.get(0) : null;
        if (inputSpec == null || 
            !inputSpec.getSpec().contains("BinaryStorage")) {
            processError("\nInput records: " + inputRecords);
        }
        processError("\nInput bytes: " + inputBytes + " bytes " +
                    ((inputSpec != null) ? 
                            "(" + inputSpec.getName() + " using " + 
                                inputSpec.getSpec() + ")" 
                            : ""));

        List<HandleSpec> outputSpecs = command.getHandleSpecs(Handle.OUTPUT);
        HandleSpec outputSpec = 
            (outputSpecs != null) ? outputSpecs.get(0) : null;
        if (outputSpec == null || 
            !outputSpec.getSpec().contains("BinaryStorage")) {
            processError("\nOutput records: " + outputRecords);
        }
        processError("\nOutput bytes: " + outputBytes + " bytes " +
                     ((outputSpec != null) ? 
                         "(" + outputSpec.getName() + " using " + 
                             outputSpec.getSpec() + ")" 
                         : ""));
        if (outputSpecs != null) {
            for (int i=1; i < outputSpecs.size(); ++i) {
                HandleSpec spec = outputSpecs.get(i);
                processError("\n           " + new File(spec.getName()).length() 
                             + " bytes using " + spec.getSpec());
            }
        }

        processError("\n=====          * * *          =====\n");
    }
}

    
