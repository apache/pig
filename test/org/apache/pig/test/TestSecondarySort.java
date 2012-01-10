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
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

public class TestSecondarySort {
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    static PigContext pc;
    static {
        pc = new PigContext(ExecType.MAPREDUCE, MiniCluster.buildCluster().getProperties());
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
    public void setUp() throws Exception {
        FileLocalizer.setR(new Random());
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

//    @Test // Currently failing due to PIG-2009
//    public void testDistinctOptimization1() throws Exception {
//        // Limit in the foreach plan
//        String query = ("A=LOAD 'input1' AS (a0, a1, a2);"+
//        "B = LOAD 'input2' AS (b0, b1, b2);" +
//        "C = cogroup A by a0, B by b0;" +
//        "D = foreach C { E = limit A 10; F = E.a1; G = DISTINCT F; generate group, COUNT(G);};" +
//        "store D into 'output';");
//        PhysicalPlan pp = Util.buildPp(pigServer, query);
//        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);
//
//        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
//        so.visit();
//
//        assertEquals( 1, so.getNumMRUseSecondaryKey() );
//        assertTrue(so.getNumSortRemoved() == 0);
//        assertTrue(so.getDistinctChanged() == 1);
//    }

    @Test
    public void testDistinctOptimization2() throws Exception {
        // Distinct on one entire input
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);"+
        "B = group A by $0;"+
        "C = foreach B { D = distinct A; generate group, D;};"+

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 0);
        assertTrue(so.getDistinctChanged() == 1);
    }

    @Test
    public void testDistinctOptimization3() throws Exception {
        // Distinct on the prefix of main sort key
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);"+
        "B = group A by $0;"+
        "C = foreach B { D = A.a0; E = distinct D; generate group, E;};"+

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 0);
        assertTrue(so.getNumSortRemoved() == 0);
        assertTrue(so.getDistinctChanged() == 1);
    }

    @Test
    public void testDistinctOptimization4() throws Exception {
        // Distinct on secondary key again, should remove
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);"+
        "B = group A by $0;"+
        "C = foreach B { D = A.a1; E = distinct D; F = distinct E; generate group, F;};"+

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 0);
        assertTrue(so.getDistinctChanged() == 2);
    }

    @Test
    public void testDistinctOptimization5() throws Exception {
        // Filter in foreach plan
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);" +
        "B = group A by $0;" +
        "C = foreach B { D = A.a1; E = distinct D; F = filter E by $0=='1'; generate group, F;};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 0);
        assertTrue(so.getDistinctChanged() == 1);
    }

    @Test
    public void testDistinctOptimization6() throws Exception {
        // group by * with no schema, and distinct key is not part of main key
        String query = ("A=LOAD 'input1';" +
        "B = group A by *;" +
        "C = foreach B { D = limit A 10; E = D.$1; F = DISTINCT E; generate group, COUNT(F);};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 0);
        assertTrue(so.getDistinctChanged() == 1);
    }

    @Test
    public void testDistinctOptimization7() throws Exception {
        // group by * with no schema, distinct key is more specific than the main key
        String query = ("A=LOAD 'input1';" +
        "B = group A by *;" +
        "C = foreach B { D = limit A 10; E = D.$0; F = DISTINCT E; generate group, COUNT(F);};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 0);
        assertTrue(so.getDistinctChanged() == 1);
    }

    @Test
    public void testDistinctOptimization8() throws Exception {
        // local arrange plan is an expression
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);" +
        "B = group A by $0+$1;" +
        "C = foreach B { D = limit A 10; E = D.$0; F = DISTINCT E; generate group, COUNT(F);};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 0);
        assertTrue(so.getDistinctChanged() == 1);
    }

    @Test
    public void testDistinctOptimization9() throws Exception {
        // local arrange plan is nested project
        String query = ("A=LOAD 'input1' as (a:tuple(a0:int, a1:chararray));" +
        "B = group A by a.a1;" +
        "C = foreach B { D = A.a; E = DISTINCT D; generate group, COUNT(E);};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 0);
        assertTrue(so.getDistinctChanged() == 1);
    }

    @Test
    public void testSortOptimization1() throws Exception {
        // Sort on something other than the main key
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);" +
        "B = group A by $0;" +
        "C = foreach B { D = limit A 10; E = order D by $1; generate group, E;};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 1);
        assertTrue(so.getDistinctChanged() == 0);
    }

    @Test
    public void testSortOptimization2() throws Exception {
        // Sort on the prefix of the main key
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);" +
        "B = group A by $0;" +
        "C = foreach B { D = limit A 10; E = order D by $0; generate group, E;};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 0);
        assertTrue(so.getNumSortRemoved() == 1);
        assertTrue(so.getDistinctChanged() == 0);
    }

    @Test
    public void testSortOptimization3() throws Exception {
        // Sort on the main key prefix / non main key prefix mixed
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);" +
        "B = group A by $0;" +
        "C = foreach B { D = limit A 10; E = order D by $1; F = order E by $0; generate group, F;};"+

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 2);
        assertTrue(so.getDistinctChanged() == 0);
    }

    @Test
    public void testSortOptimization4() throws Exception {
        // Sort on the main key again
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);" +
        "B = group A by $0;" +
        "C = foreach B { D = limit A 10; E = order D by $0, $1, $2; generate group, E;};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 1);
        assertTrue(so.getDistinctChanged() == 0);
    }

    @Test
    public void testSortOptimization5() throws Exception {
        // Sort on the two keys, we can only take off 1
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);" +
        "B = group A by $0;" +
        "C = foreach B { D = limit A 10; E = order D by $1; F = order E by $2; generate group, F;};" +
        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 1);
        assertTrue(so.getDistinctChanged() == 0);
    }

    @Test
    public void testSortOptimization6() throws Exception {
        // Sort desc
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);" +
        "B = group A by $0;" +
        "C = foreach B { D = order A by $0 desc; generate group, D;};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 1);
        assertTrue(so.getDistinctChanged() == 0);
    }

    @Test
    public void testSortOptimization7() throws Exception {
        // Sort asc on 1st key, desc on 2nd key
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);" +
        "B = group A by ($0, $1);" +
        "C = foreach B { D = order A by $0, $1 desc; generate group, D;};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 1);
        assertTrue(so.getDistinctChanged() == 0);
    }

    // See PIG-1193
    @Test
    public void testSortOptimization8() throws Exception {
        // Sort desc, used in UDF twice
        String query = ("A=LOAD 'input1' AS (a0);" +
        "B = group A all;" +
        "C = foreach B { D = order A by $0 desc; generate DIFF(D, D);};" +

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertTrue(so.getNumMRUseSecondaryKey() == 1);
        assertTrue(so.getNumSortRemoved() == 2);
        assertTrue(so.getDistinctChanged() == 0);
    }

    @Test
    public void testNestedDistinctEndToEnd1() throws Exception {
        File tmpFile1 = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("2\t3\t4");
        ps1.close();

        File tmpFile2 = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps2 = new PrintStream(new FileOutputStream(tmpFile2));
        ps2.println("1\t4\t4");
        ps2.println("2\t3\t1");
        ps2.close();
        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), tmpFile1.getCanonicalPath());
        Util.copyFromLocalToCluster(cluster, tmpFile2.getCanonicalPath(), tmpFile2.getCanonicalPath());

        pigServer.registerQuery("A = LOAD '" + tmpFile1.getCanonicalPath() + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = LOAD '" + tmpFile2.getCanonicalPath() + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = cogroup A by a0, B by b0 parallel 2;");
        pigServer
                .registerQuery("D = foreach C { E = limit A 10; F = E.a1; G = DISTINCT F; generate group, COUNT(G);};");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(2,1)"));
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(1,2)"));
        assertFalse(iter.hasNext());
        Util.deleteFile(cluster, tmpFile1.getCanonicalPath());
        Util.deleteFile(cluster, tmpFile2.getCanonicalPath());
    }

    @Test
    public void testNestedDistinctEndToEnd2() throws Exception {
        File tmpFile1 = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("2\t3\t4");
        ps1.close();
        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), tmpFile1.getCanonicalPath());
        pigServer.registerQuery("A = LOAD '" + tmpFile1.getCanonicalPath() + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by $0 parallel 2;");
        pigServer.registerQuery("C = foreach B { D = distinct A; generate group, D;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(2,{(2,3,4)})"));
        assertTrue(iter.hasNext());
        assertTrue(iter.next().toString().equals("(1,{(1,2,3),(1,2,4),(1,3,4)})"));
        assertFalse(iter.hasNext());
        Util.deleteFile(cluster, tmpFile1.getCanonicalPath());
    }

    @Test
    public void testNestedSortEndToEnd1() throws Exception {
        File tmpFile1 = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("2\t3\t4");
        ps1.close();
        
        String expected[] = {
                "(2,{(2,3,4)})",
                "(1,{(1,2,3),(1,2,4),(1,2,4),(1,2,4),(1,3,4)})"
        };
        
        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), tmpFile1.getCanonicalPath());
        pigServer.registerQuery("A = LOAD '" + tmpFile1.getCanonicalPath() + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by $0 parallel 2;");
        pigServer.registerQuery("C = foreach B { D = limit A 10; E = order D by $1; generate group, E;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Schema s = pigServer.dumpSchema("C");
        
        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(s));
        Util.deleteFile(cluster, tmpFile1.getCanonicalPath());
    }

    @Test
    public void testNestedSortEndToEnd2() throws Exception {
        File tmpFile1 = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t4\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t8\t4");
        ps1.println("2\t3\t4");
        ps1.close();
        
        String expected[] = {
                "(2,{(2,3,4)})",
                "(1,{(1,8,4),(1,4,4),(1,3,4),(1,2,3),(1,2,4)})"
        };
        
        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), tmpFile1.getCanonicalPath());
        pigServer.registerQuery("A = LOAD '" + tmpFile1.getCanonicalPath() + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by $0 parallel 2;");
        pigServer.registerQuery("C = foreach B { D = order A by a1 desc; generate group, D;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Schema s = pigServer.dumpSchema("C");
        
        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(s));
        Util.deleteFile(cluster, tmpFile1.getCanonicalPath());
    }

//    @Test
    public void testNestedSortEndToEnd3() throws Exception {
        File tmpFile1 = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("2\t3\t4");
        ps1.close();
        File tmpFile2 = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps2 = new PrintStream(new FileOutputStream(tmpFile2));
        ps2.println("1\t4\t4");
        ps2.println("2\t3\t1");
        ps2.close();
        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), tmpFile1.getCanonicalPath());
        Util.copyFromLocalToCluster(cluster, tmpFile2.getCanonicalPath(), tmpFile2.getCanonicalPath());
        pigServer.registerQuery("A = LOAD '" + tmpFile1.getCanonicalPath() + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = LOAD '" + tmpFile2.getCanonicalPath() + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = cogroup A by (a0,a1), B by (b0,b1) parallel 2;");
        pigServer.registerQuery("D = ORDER C BY group;");
        pigServer.registerQuery("E = foreach D { F = limit A 10; G = ORDER F BY a2; generate group, COUNT(G);};");
        Iterator<Tuple> iter = pigServer.openIterator("E");
        assertTrue(iter.hasNext());
        assertEquals("((1,2),4)", iter.next().toString());
        assertTrue(iter.hasNext());
        assertEquals("((1,3),1)", iter.next().toString());
        assertTrue(iter.hasNext());
        assertEquals("((1,4),0)", iter.next().toString());
        assertTrue(iter.hasNext());
        assertEquals("((2,3),1)", iter.next().toString());
        assertFalse(iter.hasNext());
        Util.deleteFile(cluster, tmpFile1.getCanonicalPath());
        Util.deleteFile(cluster, tmpFile2.getCanonicalPath());
    }
    
    @Test
    public void testNestedSortMultiQueryEndToEnd1() throws Exception {
        pigServer.setBatchOn();
        Util.copyFromLocalToCluster(cluster, "test/org/apache/pig/test/data/passwd",
                "testNestedSortMultiQueryEndToEnd1-input.txt");
        pigServer.registerQuery("a = load 'testNestedSortMultiQueryEndToEnd1-input.txt'"
                + " using PigStorage(':') as (uname:chararray, passwd:chararray, uid:int, gid:int);");
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
        Util.deleteFile(cluster, "testNestedSortMultiQueryEndToEnd1-input.txt");
    }
    
    // See PIG-1978
    @Test
    public void testForEachTwoInput() throws Exception {
        File tmpFile1 = Util.createTempFileDelOnExit("test", "txt");
        PrintStream ps1 = new PrintStream(new FileOutputStream(tmpFile1));
        ps1.println("1\t2\t3");
        ps1.println("1\t3\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("1\t2\t4");
        ps1.println("2\t3\t4");
        ps1.close();
        
        String expected[] = {
                "((1,2),{(2,3),(2,4),(2,4),(2,4)})",
                "((1,3),{(3,4)})",
                "((2,3),{(3,4)})"
        };
        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), tmpFile1.getCanonicalPath());
        pigServer.registerQuery("A = LOAD '" + tmpFile1.getCanonicalPath() + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by (a0, a1);");
        pigServer.registerQuery("C = foreach B { C1 = A.(a1,a2); generate group, C1;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Schema s = pigServer.dumpSchema("C");
        
        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(s));
        
        Util.deleteFile(cluster, tmpFile1.getCanonicalPath());
    }
}