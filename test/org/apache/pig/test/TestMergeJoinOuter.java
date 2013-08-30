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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.test.TestMapSideCogroup.DummyCollectableLoader;
import org.apache.pig.test.TestMapSideCogroup.DummyIndexableLoader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestMergeJoinOuter {

    private static final String INPUT_FILE1 = "testMergeJoinInput.txt";
    private static final String INPUT_FILE2 = "testMergeJoinInput2.txt";
    private PigServer pigServer;
    private static MiniCluster cluster = MiniCluster.buildCluster();
    
    public TestMergeJoinOuter() throws ExecException{
        
        Properties props = cluster.getProperties();
        props.setProperty("mapred.map.max.attempts", "1");
        props.setProperty("mapred.reduce.max.attempts", "1");
        pigServer = new PigServer(ExecType.MAPREDUCE, props);
    }
    
    @Before
    public void setUp() throws Exception {
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
    
    @After
    public void tearDown() throws Exception {
        Util.deleteFile(cluster, INPUT_FILE1);
        Util.deleteFile(cluster, INPUT_FILE2);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    @Test
    public void testCompilation(){
        try{
            String query = "A = LOAD 'data1' using "+ DummyCollectableLoader.class.getName() +"() as (id, name, grade);" + 
            "B = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);" +
            "C = join A by id left, B by id using 'merge';" +
            "store C into 'out';";
            LogicalPlan lp = Util.buildLp(pigServer, query);
            LOStore store = (LOStore)lp.getSinks().get(0);
            LOJoin join = (LOJoin)lp.getPredecessors(store).get(0);
            assertEquals(LOJoin.JOINTYPE.MERGE, join.getJoinType());

            PigContext pc = new PigContext(ExecType.MAPREDUCE,cluster.getProperties());
            pc.connect();
            PhysicalPlan phyP = Util.buildPp(pigServer, query);
            PhysicalOperator phyOp = phyP.getLeaves().get(0);
            assertTrue(phyOp instanceof POStore);
            phyOp = phyOp.getInputs().get(0);
            assertTrue(phyOp instanceof POForEach);
            assertEquals(1,phyOp.getInputs().size());
            assertTrue(phyOp.getInputs().get(0) instanceof POMergeCogroup);
            
            MROperPlan mrPlan = Util.buildMRPlan(phyP,pc);            
            assertEquals(2,mrPlan.size());

            Iterator<MapReduceOper> itr = mrPlan.iterator();
            List<MapReduceOper> opers = new ArrayList<MapReduceOper>();
            opers.add(itr.next());
            opers.add(itr.next());
            //Order of entrySet is not guaranteed with jdk1.7
            Collections.sort(opers);
            
            assertTrue(opers.get(0).reducePlan.isEmpty());
            assertFalse(opers.get(0).mapPlan.isEmpty());
            
            assertFalse(opers.get(1).reducePlan.isEmpty());
            assertFalse(opers.get(1).mapPlan.isEmpty());


        } catch(Exception e){
            e.printStackTrace();
            fail("Compilation of merged cogroup failed.");
        }

    }

    @Test
    public void testFailure() throws Exception{
        String query = "A = LOAD 'data1' using "+ DummyCollectableLoader.class.getName() +"() as (id, name, grade);" +
        "E = group A by id;" +
        "B = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);" +
        "C = join E by A.id, B by id using 'merge';" +
        "store C into 'output';";
        LogicalPlan lp = Util.buildLp(pigServer, query);
        Operator op = lp.getSinks().get(0);
        LOJoin join = (LOJoin)lp.getPredecessors(op).get(0);
        assertEquals(LOJoin.JOINTYPE.MERGE, join.getJoinType());

        PigContext pc = new PigContext(ExecType.MAPREDUCE,cluster.getProperties());
        pc.connect();
        boolean exceptionCaught = false;
        try{
            Util.buildPp(pigServer, query);   
        }catch (FrontendException e){
            assertEquals(1103,e.getErrorCode());
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
    }
    
    @Test
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
    
    @Test
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
    
    @Test
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
