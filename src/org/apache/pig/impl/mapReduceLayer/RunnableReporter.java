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
/**
 * 
 */
package org.apache.pig.impl.mapReduceLayer;

import org.apache.hadoop.mapred.Reporter;

public class RunnableReporter implements Runnable {
    Reporter rep;
    boolean done = false;
    private long sleepTime;
    String inputFile;

    public RunnableReporter(long sleepTime) {
        this.sleepTime = sleepTime;
    }
    
    /**
     * While the task is not done, keep reporting
     * status back every sleeptime millisecs
     */
    public void run() {
        while(!done){
            if(rep!=null)
                rep.setStatus("Processing input file: " + inputFile);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
        }
    }
    
    public void setReporter(Reporter rep){
        this.rep = rep;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public void setDone(boolean done) {
        this.done = done;
    }
}