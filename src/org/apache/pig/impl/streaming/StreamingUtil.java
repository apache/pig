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

import static org.apache.pig.PigConfiguration.PIG_STREAMING_ENVIRONMENT;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.impl.util.UDFContext;

public class StreamingUtil {
    private static Log LOG = LogFactory.getLog(StreamingUtil.class);
    
    private static final String BASH = "bash";
    private static final String PATH = "PATH";
    
    /**
     * Create an external process for StreamingCommand command.
     * 
     * @param command
     * @return
     */
    public static ProcessBuilder createProcess(StreamingCommand command) {
        // Set the actual command to run with 'bash -c exec ...'
        List<String> cmdArgs = new ArrayList<String>();
        String[] argv = command.getCommandArgs();

        StringBuffer argBuffer = new StringBuffer();
        for (String arg : argv) {
            argBuffer.append(arg);
            argBuffer.append(" ");
        }
        String argvAsString = argBuffer.toString();
        
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
          cmdArgs.add("cmd");
          cmdArgs.add("/c");
          cmdArgs.add(argvAsString);
        } else {
          cmdArgs.add(BASH);
          cmdArgs.add("-c");
          StringBuffer sb = new StringBuffer();
          sb.append("exec ");
          sb.append(argvAsString);
          cmdArgs.add(sb.toString());
        }

        // Start the external process
        ProcessBuilder processBuilder = new ProcessBuilder(cmdArgs
                .toArray(new String[cmdArgs.size()]));
        setupEnvironment(processBuilder);
        return processBuilder;
    }
    
    /**
     * Set up the run-time environment of the managed process.
     * 
     * @param pb
     *            {@link ProcessBuilder} used to exec the process
     */
    private static void setupEnvironment(ProcessBuilder pb) {
        String separator = ":";
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        Map<String, String> env = pb.environment();
        addJobConfToEnvironment(conf, env);

        // Add the current-working-directory to the $PATH
        File dir = pb.directory();
        String cwd = (dir != null) ? dir.getAbsolutePath() : System
                .getProperty("user.dir");

        String envPath = env.get(PATH);
        if (envPath == null) {
            envPath = cwd;
        } else {
            envPath = envPath + separator + cwd;
        }
        env.put(PATH, envPath);
    }
    
    protected static void addJobConfToEnvironment(Configuration conf, Map<String, String> env) {
        String propsToSend = conf.get(PIG_STREAMING_ENVIRONMENT);
        LOG.debug("Properties to ship to streaming environment set in "+PIG_STREAMING_ENVIRONMENT+": " + propsToSend);
        if (propsToSend == null) {
            return;
        }

        for (String prop : propsToSend.split(",")) {
            String value = conf.get(prop);
            if (value == null) {
                LOG.warn("Property set in "+PIG_STREAMING_ENVIRONMENT+" not found in Configuration: " + prop);
                continue;
            }
            LOG.debug("Setting property in streaming environment: " + prop);
            envPut(env, prop, value);
        }
    }
    
    private static void envPut(Map<String, String> env, String name, String value) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Add  env entry:" + name + "=" + value);
        }
        env.put(name, value);
    }
}
