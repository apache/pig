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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.rules.GroupByConstParallelSetter;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public abstract class TestGroupConstParallel {

    private static final String INPUT_FILE = "TestGroupConstParallelInp";
    private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();

    
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
        PigServer pigServer = new PigServer(cluster.getExecType(), cluster
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
            checkGroupAllWithParallelGraphResult(jGraph);
        }
    }

    abstract protected void checkGroupAllWithParallelGraphResult(JobGraph jGraph);

    /**
     * Test parallelism for group by constant
     * @throws Throwable
     */
    @Test
    public void testGroupConstWithParallel() throws Throwable {
        PigContext pc = new PigContext(cluster.getExecType(), cluster.getProperties());
        pc.defaultParallel = 100;
        pc.connect();
        
        String query = "a = load '" + INPUT_FILE + "';\n" + "b = group a by 1;" + "store b into 'output';";
        PigServer pigServer = new PigServer( cluster.getExecType(), cluster.getProperties() );
        PhysicalPlan pp = Util.buildPp( pigServer, query );

        checkGroupConstWithParallelResult(pp, pc);
    }

    abstract protected void checkGroupConstWithParallelResult(PhysicalPlan pp, PigContext pc) throws Exception;
    
    /**
     *  Test parallelism for group by column
     * @throws Throwable
     */
    @Test
    public void testGroupNonConstWithParallel() throws Throwable {
        PigContext pc = new PigContext(cluster.getExecType(), cluster.getProperties());
        pc.defaultParallel = 100;
        pc.connect();
        
        PigServer pigServer = new PigServer( cluster.getExecType(), cluster.getProperties() );
        String query =  "a = load '" + INPUT_FILE + "';\n" + "b = group a by $0;" + "store b into 'output';";
        
        PhysicalPlan pp = Util.buildPp( pigServer, query );

        checkGroupNonConstWithParallelResult(pp, pc);
    }

    abstract protected void checkGroupNonConstWithParallelResult(PhysicalPlan pp, PigContext pc) throws Exception;

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
