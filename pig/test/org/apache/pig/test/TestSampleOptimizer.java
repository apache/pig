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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.SampleOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestSampleOptimizer {

    static PigContext pc;
    static PigServer pigServer;
    static{
        pc = new PigContext(ExecType.MAPREDUCE,MiniCluster.buildCluster().getProperties());
        try {
            pc.connect();
            pigServer = new PigServer( pc );
        } catch (ExecException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        MiniCluster.buildCluster().shutDown();
    }
    
    @Test
    public void testOptimizerFired() throws Exception{
        String query = " A = load 'input' using PigStorage('\t');" +
        " B = order A by $0;" + "store B into 'output';";
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        int count = 1;
        MapReduceOper mrOper = mrPlan.getRoots().get(0);
        while(mrPlan.getSuccessors(mrOper) != null) {
            mrOper = mrPlan.getSuccessors(mrOper).get(0);
            ++count;
        }
        
        // Before optimizer visits, number of MR jobs = 3.
        assertEquals(3,count);   

        SampleOptimizer so = new SampleOptimizer(mrPlan, pc);
        so.visit();

        count = 1;
        mrOper = mrPlan.getRoots().get(0);
        while(mrPlan.getSuccessors(mrOper) != null) {
            mrOper = mrPlan.getSuccessors(mrOper).get(0);
            ++count;
        }
        
        // After optimizer visits, number of MR jobs = 2.
        assertEquals(2,count);

        // Test if RandomSampleLoader got pushed to top.
        mrOper = mrPlan.getRoots().get(0);
        List<PhysicalOperator> phyOps = mrOper.mapPlan.getRoots();
        assertEquals(1, phyOps.size());
        assertTrue(phyOps.get(0) instanceof POLoad);
        assertTrue(((POLoad)phyOps.get(0)).getLFile().getFuncName().equals("org.apache.pig.impl.builtin.RandomSampleLoader"));

        // Test RandomSampleLoader is not present anymore in second MR job.
        phyOps = mrPlan.getSuccessors(mrOper).get(0).mapPlan.getRoots();
        assertEquals(1, phyOps.size());
        assertTrue(phyOps.get(0) instanceof POLoad);
        assertFalse(((POLoad)phyOps.get(0)).getLFile().getFuncName().equals("org.apache.pig.impl.builtin.RandomSampleLoader"));
    }

    @Test
    public void testOptimizerNotFired() throws Exception{
        String query = " A = load 'input' using PigStorage('\t');" + "B = group A by $0;" +
        " C = order B by $0;" + "store C into 'output';";
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        int count = 1;
        MapReduceOper mrOper = mrPlan.getRoots().get(0);
        while(mrPlan.getSuccessors(mrOper) != null) {
            mrOper = mrPlan.getSuccessors(mrOper).get(0);
            ++count;
        }        
        // Before optimizer visits, number of MR jobs = 3.
        assertEquals(3,count);

        SampleOptimizer so = new SampleOptimizer(mrPlan, pc);
        so.visit();

        count = 1;
        mrOper = mrPlan.getRoots().get(0);
        while(mrPlan.getSuccessors(mrOper) != null) {
            mrOper = mrPlan.getSuccessors(mrOper).get(0);
            ++count;
        }        
        
        // After optimizer visits, number of MR jobs = 3. Since here
        // optimizer is not fired.
        assertEquals(3,count);

        // Test Sampler is not moved and is present in 2nd MR job.
        mrOper = mrPlan.getRoots().get(0);
        List<PhysicalOperator> phyOps = mrOper.mapPlan.getRoots();
        assertEquals(1, phyOps.size());
        assertTrue(phyOps.get(0) instanceof POLoad);
        assertFalse(((POLoad)phyOps.get(0)).getLFile().getFuncName().equals("org.apache.pig.impl.builtin.RandomSampleLoader"));

        phyOps = mrPlan.getSuccessors(mrOper).get(0).mapPlan.getRoots();
        assertEquals(1, phyOps.size());
        assertTrue(phyOps.get(0) instanceof POLoad);
        assertTrue(((POLoad)phyOps.get(0)).getLFile().getFuncName().equals("org.apache.pig.impl.builtin.RandomSampleLoader"));
    }


    // End to end is more comprehensively tested in TestOrderBy and TestOrderBy2. But since those tests are currently excluded
    // this simple end to end test is included.
    @Test
    public void testEndToEnd() throws Exception{

        PigServer pigServer = new PigServer(pc);
        int LOOP_COUNT = 40;
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random(3);
        int rand;
        for(int i = 0; i < LOOP_COUNT; i++) {
            rand = r.nextInt(100);
            ps.println(rand);
        }
        ps.close();

        pigServer.registerQuery("A = LOAD '" 
                + Util.generateURI(tmpFile.toString(), pigServer.getPigContext()) 
                + "' using PigStorage() AS (num:int);");
        pigServer.registerQuery("B = order A by num desc;");
        Iterator<Tuple> result = pigServer.openIterator("B");

        Integer prevNum = null;
        while (result.hasNext())
        {
            Integer curNum  = (Integer)result.next().get(0);
            if (null != prevNum) 
                assertTrue(curNum.compareTo(prevNum) <= 0 );

            prevNum = curNum;
        }
        tmpFile.delete();
    }
    
    @Test
    public void testPoissonSampleOptimizer() throws Exception {
        String query = " A = load 'input' using PigStorage('\t');" + 
        "B = load 'input' using PigStorage('\t');" + 
        " C = join A by $0, B by $0 using 'skewed';" +
        "store C into 'output';";
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        int count = 1;
        MapReduceOper mrOper = mrPlan.getRoots().get(0);
        while(mrPlan.getSuccessors(mrOper) != null) {
            mrOper = mrPlan.getSuccessors(mrOper).get(0);
            ++count;
        }        
        // Before optimizer visits, number of MR jobs = 3.
        assertEquals(3,count);

        SampleOptimizer so = new SampleOptimizer(mrPlan, pc);
        so.visit();

        count = 1;
        mrOper = mrPlan.getRoots().get(0);
        while(mrPlan.getSuccessors(mrOper) != null) {
            mrOper = mrPlan.getSuccessors(mrOper).get(0);
            ++count;
        }        
        // After optimizer visits, number of MR jobs = 2
        assertEquals(2,count);
    }
    
    @Test
    public void testOrderByUDFSet() throws Exception {
        String query = "a = load 'input1' using BinStorage();" + 
        "b = order a by $0;" + "store b into 'output';";
        
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        int count = 1;
        MapReduceOper mrOper = mrPlan.getRoots().get(0);
        while(mrPlan.getSuccessors(mrOper) != null) {
            mrOper = mrPlan.getSuccessors(mrOper).get(0);
            ++count;
        }        
        // Before optimizer visits, number of MR jobs = 3.
        assertEquals(3,count);

        SampleOptimizer so = new SampleOptimizer(mrPlan, pc);
        so.visit();

        count = 1;
        mrOper = mrPlan.getRoots().get(0);
        // the first mrOper should be the sampling job - it's udf list should only
        // contain BinStorage
        assertTrue(mrOper.UDFs.size()==1);
        assertTrue(mrOper.UDFs.contains("BinStorage"));
        while(mrPlan.getSuccessors(mrOper) != null) {
            mrOper = mrPlan.getSuccessors(mrOper).get(0);
            // the second mr oper is the real order by job - it's udf list should
            // contain BinStorage corresponding to the load and PigStorage
            // corresponding to the store
            assertTrue(mrOper.UDFs.size()==2);
            assertTrue(mrOper.UDFs.contains("BinStorage"));
            assertTrue(mrOper.UDFs.contains("org.apache.pig.builtin.PigStorage"));
            ++count;
        }        
        // After optimizer visits, number of MR jobs = 2
        assertEquals(2,count);
    }
}
