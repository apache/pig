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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ConfigurationValidator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.rules.GroupByConstParallelSetter;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGroupConstParallel {

    private static final String INPUT_FILE = "TestGroupConstParallelInp";
    private static MiniCluster cluster = MiniCluster.buildCluster();

    
    @BeforeClass
    public static void oneTimeSetup() throws Exception{
        String[] input = {
                "two",
                "one",
                "two",
        };
        Util.createInputFile(cluster, INPUT_FILE, input);

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    /**
     * Test parallelism for group all
     * @throws Exception
     */
    @Test
    public void testGroupAllWithParallel() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster
                .getProperties());
        
        
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:chararray);");
        pigServer.registerQuery("B = group A all parallel 5;");
        {
            Iterator<Tuple> iter = pigServer.openIterator("B");
            List<Tuple> expectedRes = 
                Util.getTuplesFromConstantTupleStrings(
                        new String[] {
                                "('all',{('one'),('two'),('two')})"
                        });
            Util.checkQueryOutputsAfterSort(iter, expectedRes);
            
            JobGraph jGraph = PigStats.get().getJobGraph();
            assertEquals(1, jGraph.size());
            // find added map-only concatenate job 
            MRJobStats js = (MRJobStats)jGraph.getSources().get(0);
            assertEquals(1, js.getNumberMaps());   
            assertEquals(1, js.getNumberReduces()); 
        }

    }
    
    
    /**
     * Test parallelism for group by constant
     * @throws Throwable
     */
    @Test
    public void testGroupConstWithParallel() throws Throwable {
        PigContext pc = new PigContext(ExecType.MAPREDUCE, cluster.getProperties());
        pc.defaultParallel = 100;
        pc.connect();
        
        String query = "a = load 'input';\n" + "b = group a by 1;" + "store b into 'output';";
        PigServer pigServer = new PigServer( ExecType.MAPREDUCE, cluster.getProperties() );
        PhysicalPlan pp = Util.buildPp( pigServer, query );
        
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        ConfigurationValidator.validatePigProperties(pc.getProperties());
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        JobControlCompiler jcc = new JobControlCompiler(pc, conf);
        
        JobControl jobControl = jcc.compile(mrPlan, "Test");
        Job job = jobControl.getWaitingJobs().get(0);
        int parallel = job.getJobConf().getNumReduceTasks();

        assertEquals("parallism", 1, parallel);
    }
    
    /**
     *  Test parallelism for group by column
     * @throws Throwable
     */
    @Test
    public void testGroupNonConstWithParallel() throws Throwable {
        PigContext pc = new PigContext(ExecType.MAPREDUCE, cluster.getProperties());
        pc.defaultParallel = 100;
        pc.connect();
        
        PigServer pigServer = new PigServer( ExecType.MAPREDUCE, cluster.getProperties() );
        String query =  "a = load 'input';\n" + "b = group a by $0;" + "store b into 'output';";
        
        PhysicalPlan pp = Util.buildPp( pigServer, query );
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        ConfigurationValidator.validatePigProperties(pc.getProperties());
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        JobControlCompiler jcc = new JobControlCompiler(pc, conf);
        
        JobControl jobControl = jcc.compile(mrPlan, "Test");
        Job job = jobControl.getWaitingJobs().get(0);
        int parallel = job.getJobConf().getNumReduceTasks();
        
        assertEquals("parallism", 100, parallel);
    }

    public class MyPlanOptimizer extends LogicalPlanOptimizer {

        protected MyPlanOptimizer(OperatorPlan p,  int iterations) {
            super(p, iterations, null);                 
        }
        
        protected List<Set<Rule>> buildRuleSets() {            
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
            
            Rule r = new GroupByConstParallelSetter("GroupByConstParallelSetter");
            Set<Rule> s = new HashSet<Rule>();
            s.add(r);            
            ls.add(s);
            
            return ls;
        }
    }    

}
