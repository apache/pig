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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.InputSizeReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.PigGraceShuffleVertexManager;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.test.Util;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTezGraceParallelism {
    private static PigServer pigServer;
    private static final String INPUT_FILE1 = TestTezGraceParallelism.class.getName() + "_1";
    private static final String INPUT_FILE2 = TestTezGraceParallelism.class.getName() + "_2";
    private static final String INPUT_DIR = Util.getTestDirectory(TestTezGraceParallelism.class);

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(Util.getLocalTestMode());
    }

    private static void createFiles() throws IOException {
        new File(INPUT_DIR).mkdirs();

        PrintWriter w = new PrintWriter(new FileWriter(INPUT_DIR + "/" + INPUT_FILE1));

        String boyNames[] = {"Noah", "Liam", "Jacob", "Mason", "William",
                "Ethan", "Michael", "Alexander", "Jayden", "Daniel"};
        String girlNames[] = {"Sophia", "Emma", "Olivia", "Isabella", "Ava",
                "Mia", "Emily", "Abigail", "Madison", "Elizabeth"};

        String names[] = new String[boyNames.length + girlNames.length];
        for (int i=0;i<boyNames.length;i++) {
            names[i] = boyNames[i];
        }
        for (int i=0;i<girlNames.length;i++) {
            names[boyNames.length+i] = girlNames[i];
        }

        Random rand = new Random(1);
        for (int i=0;i<1000;i++) {
            w.println(names[rand.nextInt(names.length)] + "\t" + rand.nextInt(18));
        }
        w.close();

        w = new PrintWriter(new FileWriter(INPUT_DIR + "/" + INPUT_FILE2));
        for (String name : boyNames) {
            w.println(name + "\t" + "M");
        }
        for (String name : girlNames) {
            w.println(name + "\t" + "F");
        }
        w.close();
    }

    private static void deleteFiles() {
        Util.deleteDirectory(new File(INPUT_DIR));
    }

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        createFiles();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        deleteFiles();
    }

    @Test
    public void testDecreaseParallelism() throws IOException{
        NodeIdGenerator.reset();
        PigServer.resetScope();
        StringWriter writer = new StringWriter();
        Util.createLogAppender("testDecreaseParallelism", writer, new Class[]{PigGraceShuffleVertexManager.class, ShuffleVertexManager.class});
        try {
            // DAG: 47 \
            //           -> 49(join) -> 52(distinct) -> 61(group)
            //      48 /
            // Parallelism at compile time:
            // DAG: 47(1) \
            //              -> 49(2) -> 52(20) -> 61(200)
            //      48(1) /
            // However, when 49 finishes, the actual output of 49 only justify parallelism 1.
            // We adjust the parallelism for 61 to 100 based on this.
            // At runtime, ShuffleVertexManager still kick in and further reduce parallelism from 100 to 1. 
            // 
            pigServer.registerQuery("A = load '" + INPUT_DIR + "/" + INPUT_FILE1 + "' as (name:chararray, age:int);");
            pigServer.registerQuery("B = load '" + INPUT_DIR + "/" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
            pigServer.registerQuery("C = join A by name, B by name;");
            pigServer.registerQuery("D = foreach C generate A::name as name, A::age as age, gender;");
            pigServer.registerQuery("E = distinct D;");
            pigServer.registerQuery("F = group E by gender;");
            pigServer.registerQuery("G = foreach F generate group as gender, SUM(E.age);");
            Iterator<Tuple> iter = pigServer.openIterator("G");
            List<Tuple> expectedResults = Util
                    .getTuplesFromConstantTupleStrings(new String[] {
                            "('F',1349L)", "('M',1373L)"});
            Util.checkQueryOutputsAfterSort(iter, expectedResults);
            assertTrue(writer.toString().contains("Initialize parallelism for scope-52 to 20"));
            assertTrue(writer.toString().contains("Initialize parallelism for scope-61 to 100"));
            assertTrue(writer.toString().contains("Reduce auto parallelism for vertex: scope-49 to 1 from 2"));
            assertTrue(writer.toString().contains("Reduce auto parallelism for vertex: scope-52 to 1 from 20"));
            assertTrue(writer.toString().contains("Reduce auto parallelism for vertex: scope-61 to 1 from 100"));
        } finally {
            Util.removeLogAppender(PigGraceShuffleVertexManager.class, "testDecreaseParallelism");
            Util.removeLogAppender(ShuffleVertexManager.class, "testDecreaseParallelism");
        }
    }

    @Test
    public void testIncreaseParallelism() throws IOException{
        NodeIdGenerator.reset();
        PigServer.resetScope();
        StringWriter writer = new StringWriter();
        Util.createLogAppender("testIncreaseParallelism", writer, new Class[]{PigGraceShuffleVertexManager.class, ShuffleVertexManager.class});
        try {
            // DAG: 35 \             /  46(sample)   \
            //           -> 37(order)        ->    56(order) -> 58(order) -> 64(distinct)
            //      36 /
            // Parallelism at compile time:
            // DAG: 35(1) \          /  46(1)   \
            //              -> 37(2)     ->    56(-1) -> 58(-1) -> 64(20)
            //      36(1) /
            // However, when 56 finishes, the actual output of 56 need parallelism 5.
            // We adjust the parallelism for 64 to 50 based on this.
            // At runtime, ShuffleVertexManager will play and reduce parallelism from 50 
            pigServer.getPigContext().getProperties().setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "80000");
            pigServer.registerQuery("A = load '" + INPUT_DIR + "/" + INPUT_FILE1 + "' as (name:chararray, age:int);");
            pigServer.registerQuery("B = load '" + INPUT_DIR + "/" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
            pigServer.registerQuery("C = join A by 1, B by 1;");
            pigServer.registerQuery("D = foreach C generate A::name as name, A::age as age, gender;");
            pigServer.registerQuery("E = order D by name;");
            pigServer.registerQuery("F = distinct E;");
            Iterator<Tuple> iter = pigServer.openIterator("F");
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            assertEquals(count, 644);
            System.out.println(writer.toString());
            assertTrue(writer.toString().contains("Initialize parallelism for scope-64 to 50"));
            // There are randomness in which task finishes first, so the auto parallelism could result different result
            assertTrue(Pattern.compile("Reduce auto parallelism for vertex: scope-64 to (\\d+)* from 50").matcher(writer.toString()).find());
        } finally {
            Util.removeLogAppender(PigGraceShuffleVertexManager.class, "testIncreaseParallelism");
            Util.removeLogAppender(ShuffleVertexManager.class, "testIncreaseParallelism");
        }
    }

    @Test
    public void testJoinWithDifferentDepth() throws IOException{
        NodeIdGenerator.reset();
        PigServer.resetScope();
        StringWriter writer = new StringWriter();
        Util.createLogAppender("testJoinWithDifferentDepth", writer, PigGraceShuffleVertexManager.class);
        try {
            // DAG:     /  51(sample) \
            //      42        ->        61(order) -> 63(order) -> 69(distinct) \
            //                                                                   -> 80(join)
            //                                             78  -> 79(group)    /
            // The join(80) has two inputs: 69 with deeper pipeline, 79 with narrower.
            // This test is to make sure 79 can start (by invoking 80.setParallelism) early,
            // don't need to wait for 63 complete
            pigServer.registerQuery("A = load '" + INPUT_DIR + "/" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
            pigServer.registerQuery("B = order A by name;");
            pigServer.registerQuery("C = distinct B;");
            pigServer.registerQuery("D = load '" + INPUT_DIR + "/" + INPUT_FILE1 + "' as (name:chararray, age:int);");
            pigServer.registerQuery("E = group D by name;");
            pigServer.registerQuery("F = foreach E generate group as name, AVG(D.age) as avg_age;");
            pigServer.registerQuery("G = join C by name, F by name;");
            Iterator<Tuple> iter = pigServer.openIterator("G");
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            assertEquals(count, 20);
            assertTrue(writer.toString().contains("All predecessors for scope-79 are finished, time to set parallelism for scope-80"));
            assertTrue(writer.toString().contains("Initialize parallelism for scope-80 to 101"));
        } finally {
            Util.removeLogAppender(PigGraceShuffleVertexManager.class, "testJoinWithDifferentDepth");
        }
    }

    @Test
    public void testJoinWithDifferentDepth2() throws IOException{
        NodeIdGenerator.reset();
        PigServer.resetScope();
        StringWriter writer = new StringWriter();
        Util.createLogAppender("testJoinWithDifferentDepth2", writer, PigGraceShuffleVertexManager.class);
        try {
            // DAG:     /  40(sample) \
            //      31        ->        50(order) -> 52(order) -> 58(distinct) \
            //                                                                   -> 68(join)
            //                                                           67    /
            // The join(68) should start immediately. We will not use PigGraceShuffleVertexManager in this case
            pigServer.registerQuery("A = load '" + INPUT_DIR + "/" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
            pigServer.registerQuery("B = order A by name;");
            pigServer.registerQuery("C = distinct B;");
            pigServer.registerQuery("D = load '" + INPUT_DIR + "/" + INPUT_FILE1 + "' as (name:chararray, age:int);");
            pigServer.registerQuery("E = join C by name, D by name;");
            Iterator<Tuple> iter = pigServer.openIterator("E");
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            assertEquals(count, 1000);
            assertFalse(writer.toString().contains("scope-68"));
        } finally {
            Util.removeLogAppender(PigGraceShuffleVertexManager.class, "testJoinWithDifferentDepth2");
        }
    }

    @Test
    // See PIG-4635 for a NPE in TezOperDependencyParallelismEstimator
    public void testJoinWithUnion() throws IOException{
        NodeIdGenerator.reset();
        PigServer.resetScope();
        StringWriter writer = new StringWriter();
        Util.createLogAppender("testJoinWithUnion", writer, PigGraceShuffleVertexManager.class);
        try {
            // DAG: 29 -> 32 -> 41 \
            //                       -> 70 (vertex group) -> 61
            //      42 -> 45 -> 54 /
            pigServer.registerQuery("A = load '" + INPUT_DIR + "/" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
            pigServer.registerQuery("B = distinct A;");
            pigServer.registerQuery("C = group B by name;");
            pigServer.registerQuery("D = load '" + INPUT_DIR + "/" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
            pigServer.registerQuery("E = distinct D;");
            pigServer.registerQuery("F = group E by name;");
            pigServer.registerQuery("G = union C, F;");
            pigServer.registerQuery("H = distinct G;");
            Iterator<Tuple> iter = pigServer.openIterator("H");
            int count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            assertEquals(count, 20);
            assertTrue(writer.toString().contains("time to set parallelism for scope-41"));
            assertTrue(writer.toString().contains("time to set parallelism for scope-54"));
        } finally {
            Util.removeLogAppender(PigGraceShuffleVertexManager.class, "testJoinWithUnion");
        }
    }
}
