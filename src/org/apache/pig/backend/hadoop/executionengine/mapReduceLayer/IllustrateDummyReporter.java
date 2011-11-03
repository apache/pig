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

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

/**
 * Dummy implementation of StatusReporter for illustrate mode
 *
 */
@SuppressWarnings("deprecation")
public class IllustrateDummyReporter extends StatusReporter{


    Counters countGen = new Counters();
    
    @Override
    public Counter getCounter(Enum<?> arg0) {
        return countGen.findCounter(arg0);
    }

    @Override
    public Counter getCounter(String group, String name) {
        return countGen.findCounter(group, name);
    }

    @Override
    public void progress() {
        //no-op
    }

    @Override
    public void setStatus(String arg0) {
        //no-op
    }

    public float getProgress() {
        return 0;
    }
    
}