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
package org.apache.pig.test;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.junit.Test;


public class TestMRExecutionEngine {
    
    @Test(expected = ExecException.class)
    public void testJobConfGeneration() throws ExecException {
        Configuration conf = new Configuration(false);
        conf.set("foo", "bar");
        PigContext pigContext = new PigContext(ExecType.MAPREDUCE, conf);
        // This should fail as pig expects Hadoop configs are present in
        // classpath.
        pigContext.connect();
    } 
    
    @Test
    public void testJobConfGenerationWithUserConfigs() throws ExecException {
        Configuration conf = new Configuration(false);
        // This property allows Pig to depend on user Configuration 
        // and not the classpath
        conf.set("pig.use.overriden.hadoop.configs", "true");
        conf.set("mapred.job.tracker", "host:12345");
        conf.set("apache", "pig");
        PigContext pigContext = new PigContext(ExecType.MAPREDUCE, conf);
        pigContext.connect();
        JobConf jc = ((MRExecutionEngine)pigContext.getExecutionEngine()).getJobConf();
        Assert.assertEquals(jc.get("mapred.job.tracker"), "host:12345");
        Assert.assertEquals(jc.get("apache"), "pig");
    }
}
