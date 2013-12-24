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
package org.apache.pig.tez;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.Properties;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLauncher;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLocalExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.TezPlanContainer;
import org.apache.pig.backend.hadoop.executionengine.tez.TezPlanContainerPrinter;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.test.Util;
import org.apache.pig.test.utils.TestHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test cases to test the TezCompiler. VERY IMPORTANT NOTE: The tests here
 * compare results with a "golden" set of outputs. In each test case here, the
 * operators generated have a random operator key which uses Java's Random
 * class. So if there is a code change which changes the number of operators
 * created in a plan, then  the "golden" file for that test case
 * need to be changed.
 */

public class TestTezCompiler {
    private static PigContext pc;
    private static PigServer pigServer;
    private static final int MAX_SIZE = 100000;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        pc = new PigContext(new TezLocalExecType(), new Properties());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws ExecException {
        NodeIdGenerator.reset();
        PigServer.resetScope();
        pigServer = new PigServer(pc);
    }

    @Test
    public void testFilter() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = filter a by x > 0;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC1.gld");
    }

    @Test
    public void testGroupBy() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = group a by x;" +
                "c = foreach b generate group, COUNT(a.x);" +
                "store c into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC2.gld");
    }

    @Test
    public void testJoin() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = join a by x, b by x;" +
                "d = foreach c generate a::x as x, y, z;" +
                "store d into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC3.gld");
    }

    @Test
    public void testLimit() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = limit a 10;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC4.gld");
    }

    @Test
    public void testDistinct() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = distinct a;" +
                "c = foreach b generate y;" +
                "store c into 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC5.gld");
    }

    @Test
    public void testSplitSingleVertex() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "split a into b if x <= 5, c if x <= 10, d if x >10;" +
                "store b into 'file:///tmp/output/b';" +
                "store c into 'file:///tmp/output/c';" +
                "store d into 'file:///tmp/output/d';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC6.gld");
    }

    @Test
    public void testSplitMultiVertex() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "split a into b if x <= 5, c if x <= 10, d if x >10;" +
                "split b into e if x < 3, f if x >= 3;" +
                // No Combiner on this split groupby when both b1 and b2 are stored
                "b1 = group b by x;" +
                "b2 = foreach b1 generate group, SUM(b.x);" +
                // Case of two outputs within a split going to same edge as input
                "c1 = join c by x, b by x;" +
                "c2 = group c by x;" +
                // TODO: Combiner as only c3 is stored.
                "c3 = foreach c2 generate group, SUM(c.x);" +
                "d1 = filter d by x == 5;" +
                //"e1 = order e by x;" + //TODO
                // TODO: Physical plan has extra split for f1 - 1-2: Split - scope-80
                // POSplit has only 1 sub plan. Optimized and removed in MR plan.
                // Needs to be removed in Tez plan as well.
                "f1 = limit f 1;" +
                //"f2 = union d1, f1;" + //TODO
                "store b1 into 'file:///tmp/output/b1';" +
                "store b2 into 'file:///tmp/output/b2';" +
                "store c1 into 'file:///tmp/output/c1';" +
                "store c3 into 'file:///tmp/output/c1';" +
                "store d1 into 'file:///tmp/output/d1';" +
                //"store e1 into 'file:///tmp/output/e1';" +
                "store f1 into 'file:///tmp/output/f1';";
                //"store f2 into 'file:///tmp/output/f2';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC7.gld");
    }

    @Test
    public void testMultipleGroupBySplit() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "b = group a by x;" +
                "b1 = foreach b generate group, COUNT(a.y);" +
                "c = group a by (x,y);" +
                "store b1 into 'file:///tmp/output/b';" +
                "store c into 'file:///tmp/output/c';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC8.gld");
    }

    @Test
    public void testJoinWithSplit() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = join a by x, b by x;" +
                "d = foreach c generate $0, $1, $3;" +
                "e = foreach c generate $0, $1, $2, $3;" +
                "store c into 'file:///tmp/output/c';" +
                "store d into 'file:///tmp/output/d';" +
                "store e into 'file:///tmp/output/e';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC9.gld");
    }

    @Test
    public void testReplicatedJoinInMapper() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = load 'file:///tmp/input2' as (x:int, z:int);" +
                "c = load 'file:///tmp/input3' as (x:int, z:int);" +
                "d = join a by x, b by x, c by x using 'replicated';" +
                "store d into 'file:///tmp/output/d';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC10.gld");
    }

    @Test
    public void testReplicatedJoinInReducer() throws Exception {
        String query =
                "a = load 'file:///tmp/input1' as (x:int, y:int);" +
                "b = group a by x;" +
                "b1 = foreach b generate group, COUNT(a.y);" +
                "c = load 'file:///tmp/input2' as (x:int, z:int);" +
                "d = join b1 by group, c by x using 'replicated';" +
                "store d into 'file:///tmp/output/e';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC11.gld");
    }

    public void testStream() throws Exception {
        String query =
                "a = load 'file:///tmp/input' using PigStorage(',') as (x:int, y:int);" +
                "b = stream a through `stream.pl -n 5`;" +
                "STORE b INTO 'file:///tmp/output';";

        PhysicalPlan pp = Util.buildPp(pigServer, query);
        run(pp, "test/org/apache/pig/test/data/GoldenFiles/TEZC12.gld");
    }

    private void run(PhysicalPlan pp, String expectedFile) throws Exception {
        TezLauncher launcher = new TezLauncher();
        TezPlanContainer tezPlanContainer = launcher.compile(pp, pc);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        TezPlanContainerPrinter printer = new TezPlanContainerPrinter(ps, tezPlanContainer);
        printer.visit();
        String compiledPlan = baos.toString();
        System.out.println();
        System.out.println("<<<" + compiledPlan + ">>>");

        FileInputStream fis = new FileInputStream(expectedFile);
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        fis.close();
        String goldenPlan = new String(b, 0, len);
        if (goldenPlan.charAt(len-1) == '\n') {
            goldenPlan = goldenPlan.substring(0, len-1);
        }

        System.out.println("-------------");
        System.out.println("Golden");
        System.out.println("<<<" + goldenPlan + ">>>");
        System.out.println("-------------");

        String goldenPlanClean = Util.standardizeNewline(goldenPlan).trim();
        String compiledPlanClean = Util.standardizeNewline(compiledPlan).trim();
        assertEquals(TestHelper.sortUDFs(Util.removeSignature(goldenPlanClean)),
                TestHelper.sortUDFs(Util.removeSignature(compiledPlanClean)));
    }
}

