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

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.test.TestMapSideCogroup.DummyCollectableLoader;
import org.apache.pig.test.TestMapSideCogroup.DummyIndexableLoader;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.After;
import org.junit.Before;


public class TestMergeJoinOuter extends TestCase{

    private static final String INPUT_FILE1 = "testMergeJoinInput.txt";
    private static final String INPUT_FILE2 = "testMergeJoinInput2.txt";
    private PigServer pigServer;
    private MiniCluster cluster = MiniCluster.buildCluster();
    
    public TestMergeJoinOuter() throws ExecException{
        
        Properties props = cluster.getProperties();
        props.setProperty("mapred.map.max.attempts", "1");
        props.setProperty("mapred.reduce.max.attempts", "1");
        pigServer = new PigServer(ExecType.MAPREDUCE, props);
    }
    
    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        int LOOP_SIZE = 3;
        String[] input = new String[LOOP_SIZE*LOOP_SIZE];
        int k = 0;
        for(int i = 1; i <= LOOP_SIZE; i++) {
            String si = i + "";
            for(int j=1;j<=LOOP_SIZE;j++)
                input[k++] = si + "\t" + j;
        }
        Util.createInputFile(cluster, INPUT_FILE1, input);
        
        Util.createInputFile(cluster, INPUT_FILE2, new String[]{"2\t2","2\t3","3\t1","4\t1","4\t3"});
    }
    
    @Override
    @After
    protected void tearDown() throws Exception {
        super.tearDown();
        Util.deleteFile(cluster, INPUT_FILE1);
        Util.deleteFile(cluster, INPUT_FILE2);
    }

    public void testCompilation(){
        try{
            LogicalPlanTester lpt = new LogicalPlanTester();
            lpt.buildPlan("A = LOAD 'data1' using "+ DummyCollectableLoader.class.getName() +"() as (id, name, grade);");
            lpt.buildPlan("B = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);");
            LogicalPlan lp = lpt.buildPlan("C = join A by id left, B by id using 'merge';");
            assertEquals(LOJoin.JOINTYPE.MERGE, ((LOJoin)lp.getLeaves().get(0)).getJoinType());

            PigContext pc = new PigContext(ExecType.MAPREDUCE,cluster.getProperties());
            pc.connect();
            PhysicalPlan phyP = Util.buildPhysicalPlan(lp, pc);
            PhysicalOperator phyOp = phyP.getLeaves().get(0);
            assertTrue(phyOp instanceof POForEach);
            assertEquals(1,phyOp.getInputs().size());
            assertTrue(phyOp.getInputs().get(0) instanceof POMergeCogroup);
            
            lp = lpt.buildPlan("store C into 'out';");
            MROperPlan mrPlan = Util.buildMRPlan(Util.buildPhysicalPlan(lp, pc),pc);            
            assertEquals(2,mrPlan.size());

            Iterator<MapReduceOper> itr = mrPlan.iterator();
            MapReduceOper oper = itr.next();
            assertTrue(oper.reducePlan.isEmpty());
            assertFalse(oper.mapPlan.isEmpty());

            oper = itr.next();
            assertFalse(oper.reducePlan.isEmpty());
            assertFalse(oper.mapPlan.isEmpty());


        } catch(Exception e){
            e.printStackTrace();
            fail("Compilation of merged cogroup failed.");
        }

    }

    public void testFailure() throws Exception{
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("A = LOAD 'data1' using "+ DummyCollectableLoader.class.getName() +"() as (id, name, grade);");
        lpt.buildPlan("E = group A by id;");
        lpt.buildPlan("B = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);");
        LogicalPlan lp = lpt.buildPlan("C = join E by A.id, B by id using 'merge';");
        assertEquals(LOJoin.JOINTYPE.MERGE, ((LOJoin)lp.getLeaves().get(0)).getJoinType());

        PigContext pc = new PigContext(ExecType.MAPREDUCE,cluster.getProperties());
        pc.connect();
        boolean exceptionCaught = false;
        try{
            Util.buildPhysicalPlan(lp, pc);   
        }catch (LogicalToPhysicalTranslatorException e){
            assertEquals(1103,e.getErrorCode());
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
    }
    
    public void testLeftOuter() throws IOException {
        
        pigServer.registerQuery("A = LOAD '"+INPUT_FILE1+"' using "+ DummyCollectableLoader.class.getName() +"() as (c1:chararray, c2:chararray);");
        pigServer.registerQuery("B = LOAD '"+INPUT_FILE2+"' using "+ DummyIndexableLoader.class.getName() +"() as (c1:chararray, c2:chararray);");

        pigServer.registerQuery("C = join A by c1 left, B by c1 using 'merge';");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        String[] results = {"(1,1,,)",
                            "(1,2,,)",
                            "(1,3,,)",
                            "(2,2,2,2)",
                            "(2,2,2,3)",
                            "(2,1,2,2)",
                            "(2,1,2,3)",
                            "(2,3,2,2)",
                            "(2,3,2,3)",
                            "(3,3,3,1)",
                            "(3,1,3,1)",
                            "(3,2,3,1)"};
        
        for(String result : results)
            assertEquals(result, iter.next().toString());
        
        assertFalse(iter.hasNext());

    }
    
    public void testRightOuter() throws IOException{
        
        pigServer.registerQuery("A = LOAD '"+INPUT_FILE1+"' using "+ DummyCollectableLoader.class.getName() +"() as (c1:chararray, c2:chararray);");
        pigServer.registerQuery("B = LOAD '"+INPUT_FILE2+"' using "+ DummyIndexableLoader.class.getName() +"() as (c1:chararray, c2:chararray);");
        pigServer.registerQuery("C = join A by c1 right, B by c1 using 'merge';");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        String[] results = {"(2,2,2,2)",
                            "(2,2,2,3)",
                            "(2,1,2,2)",
                            "(2,1,2,3)",
                            "(2,3,2,2)",
                            "(2,3,2,3)",
                            "(3,3,3,1)",
                            "(3,1,3,1)",
                            "(3,2,3,1)",
                            "(,,4,1)",
                            "(,,4,3)"};
        
        for(String result : results)
            assertEquals(result, iter.next().toString());
        
        assertFalse(iter.hasNext());

    }
    
    public void testFullOuter() throws IOException{
        
        pigServer.registerQuery("A = LOAD '"+INPUT_FILE1+"' using "+ DummyCollectableLoader.class.getName() +"() as (c1:chararray, c2:chararray);");
        pigServer.registerQuery("B = LOAD '"+INPUT_FILE2+"' using "+ DummyIndexableLoader.class.getName() +"() as (c1:chararray, c2:chararray);");
        pigServer.registerQuery("C = join A by c1 full, B by c1 using 'merge';");

        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        String[] results = {"(1,1,,)",
                            "(1,2,,)",
                            "(1,3,,)",
                            "(2,2,2,2)",
                            "(2,2,2,3)",
                            "(2,1,2,2)",
                            "(2,1,2,3)",
                            "(2,3,2,2)",
                            "(2,3,2,3)",
                            "(3,3,3,1)",
                            "(3,1,3,1)",
                            "(3,2,3,1)",
                            "(,,4,1)",
                            "(,,4,3)"};
        
        for(String result : results)
            assertEquals(result, iter.next().toString());
        
        assertFalse(iter.hasNext());
    }
}
