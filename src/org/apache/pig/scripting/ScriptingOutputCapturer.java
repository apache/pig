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
package org.apache.pig.scripting;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.base.Charsets;

/**
 * This class helps a scripting UDF capture user output by managing when to capture output and where
 * the output is written to.
 * 
 * For illustrate, we will only capture output for the last run (with the final set of data) and
 *   we need to keep track of the file containing that output for returning w/ the illustrate results.
 * For runs, all standard output is written to the user logs.
 */
public class ScriptingOutputCapturer {
    private static Log log = LogFactory.getLog(ScriptingOutputCapturer.class);
    
    private static Map<String, String> outputFileNames = new HashMap<String, String>();
    private static String runId = UUID.randomUUID().toString(); //Unique ID for this run to ensure udf output files aren't corrupted from previous runs.

    //Illustrate will set the static flag telling udf to start capturing its output.  It's up to each
    //instance to react to it and set its own flag.
    private static boolean captureOutput = false;
    private boolean instancedCapturingOutput = false;

    private ExecType execType;

    public ScriptingOutputCapturer(ExecType execType) {
        this.execType = execType;
    }

    public String getStandardOutputRootWriteLocation() throws IOException {
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        
        String jobId = conf.get("mapred.job.id");
        String taskId = conf.get("mapred.task.id");
        String hadoopLogDir = System.getProperty("hadoop.log.dir");
        if (hadoopLogDir == null) {
            hadoopLogDir = conf.get("hadoop.log.dir");
        }
        
        String tmpDir = conf.get("hadoop.tmp.dir");
        boolean fallbackToTmp = (hadoopLogDir == null);
        if (!fallbackToTmp) {
            try {
                if (!(new File(hadoopLogDir).canWrite())) {
                    fallbackToTmp = true;
                }
            }
            catch (SecurityException e) {
                fallbackToTmp = true;
            }
            finally {
                if (fallbackToTmp)
                    log.warn(String.format("Insufficient permission to write into %s. Change path to: %s", hadoopLogDir, tmpDir));
            }
        }
        if (fallbackToTmp) {
            hadoopLogDir = tmpDir;
        }
        log.debug("JobId: " + jobId);
        log.debug("TaskId: " + taskId);
        log.debug("hadoopLogDir: " + hadoopLogDir);

        if (execType.isLocal()) {
            String logDir = System.getProperty("pig.udf.scripting.log.dir");
            if (logDir == null)
                logDir = ".";
            return logDir + "/" + (taskId == null ? "" : (taskId + "_"));
        } else {
            String taskLogDir = getTaskLogDir(jobId, taskId, hadoopLogDir);
            return taskLogDir + "/";
        }
    }

    public String getTaskLogDir(String jobId, String taskId, String hadoopLogDir) throws IOException {
        String taskLogDir;
        String defaultUserLogDir = hadoopLogDir + File.separator + "userlogs";

        if ( new File(defaultUserLogDir + File.separator + jobId).exists() ) {
            taskLogDir = defaultUserLogDir + File.separator + jobId + File.separator + taskId;
        } else if ( new File(defaultUserLogDir + File.separator + taskId).exists() ) {
            taskLogDir = defaultUserLogDir + File.separator + taskId;
        } else if ( new File(defaultUserLogDir).exists() ){
            taskLogDir = defaultUserLogDir;
        } else {
            taskLogDir = hadoopLogDir + File.separator + "udfOutput";
            File dir = new File(taskLogDir);
            dir.mkdirs();
            if (!dir.exists()) {
                throw new IOException("Could not create directory: " + taskLogDir);
            }
        }
        return taskLogDir;
    }
    
    public static void startCapturingOutput() {
       ScriptingOutputCapturer.captureOutput = true;
    }
    
    public static Map<String, String> getUdfOutput() throws IOException {
        Map<String, String> udfFuncNameToOutput = new HashMap<String,String>();
        for (Map.Entry<String, String> funcToOutputFileName : outputFileNames.entrySet()) {
            StringBuffer udfOutput = new StringBuffer();
            FileInputStream fis = new FileInputStream(funcToOutputFileName.getValue());
            Reader fr = new InputStreamReader(fis, Charsets.UTF_8);
            BufferedReader br = new BufferedReader(fr);
            
            try {
                String line = br.readLine();
                while (line != null) {
                    udfOutput.append("\t" + line + "\n");
                    line = br.readLine();
                }
            } finally {
                br.close();
            }
            udfFuncNameToOutput.put(funcToOutputFileName.getKey(), udfOutput.toString());
        }
        return udfFuncNameToOutput;
    }
    
    public void registerOutputLocation(String functionName, String fileName) {
        outputFileNames.put(functionName, fileName);
    }
    
    public static String getRunId() {
        return runId;
    }
    
    public static boolean isClassCapturingOutput() {
        return ScriptingOutputCapturer.captureOutput;
    }
    
    public boolean isInstanceCapturingOutput() {
        return this.instancedCapturingOutput;
    }
    
    public void setInstanceCapturingOutput(boolean instanceCapturingOutput) {
        this.instancedCapturingOutput = instanceCapturingOutput;
    }
}
