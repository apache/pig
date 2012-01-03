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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;

public class ProgressableReporter implements PigProgressable {
    TaskAttemptContext rep;

    public ProgressableReporter(){
        
    }

    public ProgressableReporter(TaskAttemptContext rep) {
        super();
        this.rep = rep;
    }

    public void progress() {
        if(rep!=null)
            rep.progress();
    }

    public void progress(String msg) {
        try {
            rep.setStatus(msg);
        }catch (Exception e) {
            rep.progress();
        }
    }

    public void setRep(TaskAttemptContext rep) {
        this.rep = rep;
    }

}
