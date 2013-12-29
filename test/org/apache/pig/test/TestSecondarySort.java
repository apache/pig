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

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

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

public class TestSecondarySort {
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    static PigContext pc;
    static {
        pc = new PigContext(ExecType.MAPREDUCE, MiniCluster.buildCluster().getProperties());
        try {
            pc.connect();
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    @Test
    public void testDistinctOptimization1() throws Exception {
        // Distinct on one entire input
        String query = ("A=LOAD 'input1' AS (a0, a1, a2);"+
        "B = group A by $0;"+
        "C = foreach B { D = distinct A; generate group, D;};"+

        "store C into 'output';");
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        SecondaryKeyOptimizer so = new SecondaryKeyOptimizer(mrPlan);
        so.visit();

        assertEquals(1, so.getNumMRUseSecondaryKey());
        assertEquals(0, so.getNumSortRemoved());
        assertEquals(1, so.getDistinctChanged());
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

        assertEquals(1, so.getNumMRUseSecondaryKey());
        assertEquals(1, so.getNumSortRemoved());
        assertEquals(0, so.getDistinctChanged());
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

        assertEquals(0, so.getNumMRUseSecondaryKey());
        assertEquals(1, so.getNumSortRemoved());
        assertEquals(0, so.getDistinctChanged());
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

        assertEquals(1, so.getNumMRUseSecondaryKey());
        assertEquals(2, so.getNumSortRemoved());
        assertEquals(0, so.getDistinctChanged());
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

        assertEquals(1, so.getNumMRUseSecondaryKey());
        assertEquals(1, so.getNumSortRemoved());
        assertEquals(0, so.getDistinctChanged());
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

        assertEquals(1, so.getNumMRUseSecondaryKey());
        assertEquals(1, so.getNumSortRemoved());
        assertEquals(0, so.getDistinctChanged());
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

        assertEquals(1, so.getNumMRUseSecondaryKey());
        assertEquals(1, so.getNumSortRemoved());
        assertEquals(0, so.getDistinctChanged());
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

        assertEquals(1, so.getNumMRUseSecondaryKey());
        assertEquals(1, so.getNumSortRemoved());
        assertEquals(0, so.getDistinctChanged());
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

        assertEquals(1, so.getNumMRUseSecondaryKey());
        assertEquals(2, so.getNumSortRemoved());
        assertEquals(0, so.getDistinctChanged());
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
        String file1ClusterPath = Util.removeColon(tmpFile1.getCanonicalPath());
        String file2ClusterPath = Util.removeColon(tmpFile2.getCanonicalPath());

        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), file1ClusterPath);
        Util.copyFromLocalToCluster(cluster, tmpFile2.getCanonicalPath(), file2ClusterPath);

        pigServer.registerQuery("A = LOAD '" + Util.encodeEscape(file1ClusterPath) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = LOAD '" + Util.encodeEscape(file2ClusterPath) + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = cogroup A by a0, B by b0 parallel 2;");
        pigServer
                .registerQuery("D = foreach C { E = limit A 10; F = E.a1; G = DISTINCT F; generate group, COUNT(G);};");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        assertTrue(iter.hasNext());
        assertEquals("(2,1)", iter.next().toString());
        assertTrue(iter.hasNext());
        assertEquals("(1,2)", iter.next().toString());
        assertFalse(iter.hasNext());
        Util.deleteFile(cluster, file1ClusterPath);
        Util.deleteFile(cluster, file2ClusterPath);
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

        String clusterPath = Util.removeColon(tmpFile1.getCanonicalPath());

        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), clusterPath);
        pigServer.registerQuery("A = LOAD '" + Util.encodeEscape(clusterPath) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by $0 parallel 2;");
        pigServer.registerQuery("C = foreach B { D = distinct A; generate group, D;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        assertTrue(iter.hasNext());
        assertEquals("(2,{(2,3,4)})", iter.next().toString());
        assertTrue(iter.hasNext());
        assertEquals("(1,{(1,2,3),(1,2,4),(1,3,4)})", iter.next().toString());
        assertFalse(iter.hasNext());
        Util.deleteFile(cluster, clusterPath);
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

        String clusterPath = Util.removeColon(tmpFile1.getCanonicalPath());

        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), clusterPath);
        pigServer.registerQuery("A = LOAD '" + Util.encodeEscape(clusterPath) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by $0 parallel 2;");
        pigServer.registerQuery("C = foreach B { D = limit A 10; E = order D by $1; generate group, E;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Schema s = pigServer.dumpSchema("C");

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(s));
        Util.deleteFile(cluster, clusterPath);
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

        String clusterPath = Util.removeColon(tmpFile1.getCanonicalPath());

        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), clusterPath);
        pigServer.registerQuery("A = LOAD '" + Util.encodeEscape(clusterPath) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by $0 parallel 2;");
        pigServer.registerQuery("C = foreach B { D = order A by a1 desc; generate group, D;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Schema s = pigServer.dumpSchema("C");

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(s));
        Util.deleteFile(cluster, clusterPath);
    }

    @Test
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
        String clusterPath1 = tmpFile1.getName();
        String clusterPath2 = tmpFile2.getName();
        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), clusterPath1);
        Util.copyFromLocalToCluster(cluster, tmpFile2.getCanonicalPath(), clusterPath2);
        pigServer.registerQuery("A = LOAD '" + clusterPath1 + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = LOAD '" + clusterPath2 + "' AS (b0, b1, b2);");
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
        Util.deleteFile(cluster, clusterPath1);
        Util.deleteFile(cluster, clusterPath2);
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
            assertEquals(ExecJob.JOB_STATUS.COMPLETED, job.getStatus());
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
        String clusterFilePath = Util.removeColon(tmpFile1.getCanonicalPath());

        Util.copyFromLocalToCluster(cluster, tmpFile1.getCanonicalPath(), clusterFilePath);
        pigServer.registerQuery("A = LOAD '" + Util.encodeEscape(clusterFilePath) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by (a0, a1);");
        pigServer.registerQuery("C = foreach B { C1 = A.(a1,a2); generate group, C1;};");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Schema s = pigServer.dumpSchema("C");

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(s));

        Util.deleteFile(cluster, clusterFilePath);
    }
}

