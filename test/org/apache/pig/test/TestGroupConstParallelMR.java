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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ConfigurationValidator;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.junit.Assume;
import org.junit.BeforeClass;

public class TestGroupConstParallelMR extends TestGroupConstParallel {

    @BeforeClass
    public static void oneTimeSetup() throws Exception{
        String execType = System.getProperty("test.exec.type");
        Assume.assumeTrue("This test suite should only run in mr mode", execType.equalsIgnoreCase("mr"));
        TestGroupConstParallel.oneTimeSetup();
    }

    @Override
    public void checkGroupAllWithParallelGraphResult(JobGraph jGraph) {
        assertEquals(1, jGraph.size());
        // find added map-only concatenate job 
        MRJobStats js = (MRJobStats)jGraph.getSources().get(0);
        assertEquals(1, js.getNumberMaps());   
        assertEquals(1, js.getNumberReduces());
    }

    @Override
    public void checkGroupConstWithParallelResult(PhysicalPlan pp, PigContext pc) throws Exception {
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        ConfigurationValidator.validatePigProperties(pc.getProperties());
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        JobControlCompiler jcc = new JobControlCompiler(pc, conf);
        
        JobControl jobControl = jcc.compile(mrPlan, "Test");
        Job job = jobControl.getWaitingJobs().get(0);
        int parallel = job.getJobConf().getNumReduceTasks();

        assertEquals("parallism", 1, parallel);
    }

    @Override
    public void checkGroupNonConstWithParallelResult(PhysicalPlan pp, PigContext pc) throws Exception {
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        ConfigurationValidator.validatePigProperties(pc.getProperties());
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        JobControlCompiler jcc = new JobControlCompiler(pc, conf);
        
        JobControl jobControl = jcc.compile(mrPlan, "Test");
        Job job = jobControl.getWaitingJobs().get(0);
        int parallel = job.getJobConf().getNumReduceTasks();
        
        assertEquals("parallism", 100, parallel);
    }
}
