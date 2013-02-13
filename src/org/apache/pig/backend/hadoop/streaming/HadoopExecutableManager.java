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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.impl.streaming.ExecutableManager;
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

    Configuration job;
    
    String scriptOutputDir;
    String scriptLogDir;
    String taskId;
    
    FSDataOutputStream errorStream;
    
    public HadoopExecutableManager() {}
    
    public void configure(POStream stream) 
    throws IOException, ExecException {
        super.configure(stream);
        
        // Chmod +x the executable
        File executable = new File(command.getExecutable());
        if (executable.isAbsolute()) {
            // we don't own it. Hope it is executable ...
        } else {
            try {
                FileUtil.chmod(executable.toString(), "a+x");
            } catch (InterruptedException ie) {
                int errCode = 6013;
                String msg = "Unable to chmod " + executable + " . Thread interrupted.";
                throw new ExecException(msg, errCode, PigException.REMOTE_ENVIRONMENT, ie);
            }
        }
        
        // Save a copy of the JobConf
        job = PigMapReduce.sJobConfInternal.get();
        
        // Save the output directory for the Pig Script
        scriptOutputDir = job.get("pig.streaming.task.output.dir");
        scriptLogDir = job.get("pig.streaming.log.dir", "_logs");
        
        // Save the taskid
        taskId = job.get("mapred.task.id");
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

    public void close() throws IOException {
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
                        int errCode = 6014; 
                        String msg = "Failed to save secondary output '" + 
                        fileName + "' of task: " + taskId;
                        throw new ExecException(msg, errCode, PigException.REMOTE_ENVIRONMENT, ioe);
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
            MapContext context = (MapContext)PigMapReduce.sJobContext;
            PigSplit pigSplit = (PigSplit)context.getInputSplit();
            int numPaths = pigSplit.getNumPaths();
            processError("\nPigSplit contains " + numPaths + " wrappedSplits.");

            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < numPaths; i++) {
              InputSplit wrappedSplit = pigSplit.getWrappedSplit(i);
              if (wrappedSplit instanceof FileSplit) {
                  FileSplit mapInputFileSplit = (FileSplit)wrappedSplit;
                  sb.append("\nInput-split: file=");
                  sb.append(mapInputFileSplit.getPath());
                  sb.append(" start-offset=");
                  sb.append(Long.toString(mapInputFileSplit.getStart()));
                  sb.append(" length=");
                  sb.append(Long.toString(mapInputFileSplit.getLength()));
                  processError(sb.toString());
                  sb.setLength(0);
              }
            }
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

    
