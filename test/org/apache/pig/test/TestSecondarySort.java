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

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.SecondaryKeyOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestSecondarySort extends TestCase {
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    static PigContext pc;
    static{
        pc = new PigContext(ExecType.MAPREDUCE,MiniCluster.buildCluster().getProperties());
        try {
            pc.connect();
        } catch (ExecException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    @Before
    @Override
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    @Test
    public void testDistinctOptimization1() throws Exception{
        // Limit in the foreach plan
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = LOAD 'input2' AS (b0, b1, b2);");
        planTester.buildPlan("C = cogroup A by a0, B by b0;");
        planTester.buildPlan("D = foreach C { E = limit A 10; F = E.a1; G = DISTINCT F; generate group, COUNT(G);};");
        
        LogicalPlan lp = planTester.buildPlan("store D into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==0);
        assertTrue(so.getDistinctChanged()==1);
    }
    
    @Test
    public void testDistinctOptimization2() throws Exception{
        // Distinct on one entire input 
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = distinct A; generate group, D;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==0);
        assertTrue(so.getDistinctChanged()==1);
    }
    
    @Test
    public void testDistinctOptimization3() throws Exception{
        // Distinct on the prefix of main sort key
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = A.a0; E = distinct D; generate group, E;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==0);
        assertTrue(so.getNumSortRemoved()==0);
        assertTrue(so.getDistinctChanged()==1);
    }
    
    @Test
    public void testDistinctOptimization4() throws Exception{
        // Distinct on secondary key again, should remove
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = A.a1; E = distinct D; F = distinct E; generate group, F;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==0);
        assertTrue(so.getDistinctChanged()==2);
    }
    
    @Test
    public void testDistinctOptimization5() throws Exception{
        // Filter in foreach plan
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = A.a1; E = distinct D; F = filter E by $0=='1'; generate group, F;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==0);
        assertTrue(so.getDistinctChanged()==1);
    }
    
    @Test
    public void testDistinctOptimization6() throws Exception{
        // group by * with no schema, and distinct key is not part of main key
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1';");
        planTester.buildPlan("B = group A by *;");
        planTester.buildPlan("C = foreach B { D = limit A 10; E = D.$1; F = DISTINCT E; generate group, COUNT(F);};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==0);
        assertTrue(so.getDistinctChanged()==1);
    }

    @Test
    public void testDistinctOptimization7() throws Exception{
        // group by * with no schema, distinct key is more specific than the main key
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1';");
        planTester.buildPlan("B = group A by *;");
        planTester.buildPlan("C = foreach B { D = limit A 10; E = D.$0; F = DISTINCT E; generate group, COUNT(F);};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==0);
        assertTrue(so.getDistinctChanged()==1);
    }
    
    @Test
    public void testDistinctOptimization8() throws Exception{
        // local arrange plan is an expression
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0+$1;");
        planTester.buildPlan("C = foreach B { D = limit A 10; E = D.$0; F = DISTINCT E; generate group, COUNT(F);};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==0);
        assertTrue(so.getDistinctChanged()==1);
    }
    
    @Test
    public void testDistinctOptimization9() throws Exception{
        // local arrange plan is nested project
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' as (a:tuple(a0:int, a1:chararray));");
        planTester.buildPlan("B = group A by a.a1;");
        planTester.buildPlan("C = foreach B { D = A.a; E = DISTINCT D; generate group, COUNT(E);};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==0);
        assertTrue(so.getDistinctChanged()==1);
    }
    
    @Test
    public void testSortOptimization1() throws Exception{
        // Sort on something other than the main key
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = limit A 10; E = order D by $1; generate group, E;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==1);
        assertTrue(so.getDistinctChanged()==0);
    }
    
    @Test
    public void testSortOptimization2() throws Exception{
        // Sort on the prefix of the main key
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = limit A 10; E = order D by $0; generate group, E;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==0);
        assertTrue(so.getNumSortRemoved()==1);
        assertTrue(so.getDistinctChanged()==0);
    }
    
    @Test
    public void testSortOptimization3() throws Exception{
        // Sort on the main key prefix / non main key prefix mixed
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = limit A 10; E = order D by $1; F = order E by $0; generate group, F;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==2);
        assertTrue(so.getDistinctChanged()==0);
    }
    
    @Test
    public void testSortOptimization4() throws Exception{
        // Sort on the main key again
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = limit A 10; E = order D by $0, $1, $2; generate group, E;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==1);
        assertTrue(so.getDistinctChanged()==0);
    }
    
    @Test
    public void testSortOptimization5() throws Exception{
        // Sort on the two keys, we can only take off 1
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = limit A 10; E = order D by $1; F = order E by $2; generate group, F;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==1);
        assertTrue(so.getDistinctChanged()==0);
    }
    
    @Test
    public void testSortOptimization6() throws Exception{
        // Sort desc
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by $0;");
        planTester.buildPlan("C = foreach B { D = order A by $0 desc; generate group, D;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==1);
        assertTrue(so.getDistinctChanged()==0);
    }
    
    @Test
    public void testSortOptimization7() throws Exception{
        // Sort asc on 1st key, desc on 2nd key
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0, a1, a2);");
        planTester.buildPlan("B = group A by ($0, $1);");
        planTester.buildPlan("C = foreach B { D = order A by $0, $1 desc; generate group, D;};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==1);
        assertTrue(so.getDistinctChanged()==0);
    }
    
    // See PIG-1193
    @Test
    public void testSortOptimization8() throws Exception{
        // Sort desc, used in UDF twice
        LogicalPlanTester planTester = new LogicalPlanTester() ;
        planTester.buildPlan("A = LOAD 'input1' AS (a0);");
        planTester.buildPlan("B = group A all;");
        planTester.buildPlan("C = foreach B { D = order A by $0 desc; generate DIFF(D, D);};");
        
        LogicalPlan lp = planTester.buildPlan("store C into '/tmp';");
        PhysicalPlan pp = Util.buildPhysicalPlan(lp, pc);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
        
        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();
        
        assertTrue(so.getNumMRUseSecondaryKey()==1);
        assertTrue(so.getNumSortRemoved()==2);
        assertTrue(so.getDistinctChanged()==0);
    }
    
    @Test
    public void testNestedDistinctEndToEnd1() throws Exception{
        File tmpFile1 = File.createTempFile("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("2\t3\t4");
        ps1.close();
        
        File tmpFile2 = File.createTempFile("test", "txt");
        PrintStream ps2 = new PrintStream(new FileOutputStream(tmpFile2));
        ps2.println("1\t4\t4");
        ps2.println("2\t3\t1");
        ps2.close();

        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile1.toString()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = LOAD '" + Util.generateURI(tmpFile2.toString()) + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = cogroup A by a0, B by b0 parallel 2;");
        pigServer.registerQuery("D = foreach C { E = limit A 10; F = E.a1; G = DISTINCT F; generate group, COUNT(G);};");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(1,2L)"));
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(2,1L)"));
        assertFalse(iter.hasNext());
        tmpFile1.delete();
        tmpFile2.delete();
    }
    
    @Test
    public void testNestedDistinctEndToEnd2() throws Exception{
        File tmpFile1 = File.createTempFile("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("2\t3\t4");
        ps1.close();
        
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile1.toString()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by $0 parallel 2;");
        pigServer.registerQuery("C = foreach B { D = distinct A; generate group, D;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(1,{(1,2,3),(1,2,4),(1,3,4)})"));
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(2,{(2,3,4)})"));
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void testNestedSortEndToEnd1() throws Exception{
        File tmpFile1 = File.createTempFile("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("2\t3\t4");
        ps1.close();
        
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile1.toString()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by $0 parallel 2;");
        pigServer.registerQuery("C = foreach B { D = limit A 10; E = order D by $1; generate group, E;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(1,{(1,2,3),(1,2,4),(1,2,4),(1,2,4),(1,3,4)})"));
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(2,{(2,3,4)})"));
        assertFalse(iter.hasNext());
        tmpFile1.delete();
    }
    
    @Test
    public void testNestedSortEndToEnd2() throws Exception{
        File tmpFile1 = File.createTempFile("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t4\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t8\t4");
        ps1.println("2\t3\t4");
        ps1.close();
        
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile1.toString()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by $0 parallel 2;");
        pigServer.registerQuery("C = foreach B { D = order A by a1 desc; generate group, D;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(1,{(1,8,4),(1,4,4),(1,3,4),(1,2,3),(1,2,4)})"));
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(2,{(2,3,4)})"));
        assertFalse(iter.hasNext());
        tmpFile1.delete();
    }
    
    @Test
    public void testNestedSortMultiQueryEndToEnd1() throws Exception{
        pigServer.setBatchOn();
        pigServer.registerQuery("a = load 'file:test/org/apache/pig/test/data/passwd'" +
                " using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
        pigServer.registerQuery("b = group a by uname parallel 2;");
        pigServer.registerQuery("c = group a by gid parallel 2;");
        pigServer.registerQuery("d = foreach b generate SUM(a.gid);");
        pigServer.registerQuery("e = foreach c { f = order a by uid; generate group, f; };");
        pigServer.registerQuery("store d into '/tmp/output1';");
        pigServer.registerQuery("store e into '/tmp/output2';");
        
        List<ExecJob> jobs = pigServer.executeBatch();
        for (ExecJob job : jobs) {
            assertTrue(job.getStatus() == ExecJob.JOB_STATUS.COMPLETED);
        }
        FileLocalizer.delete("/tmp/output1", pigServer.getPigContext());
        FileLocalizer.delete("/tmp/output2", pigServer.getPigContext());
    }
}
