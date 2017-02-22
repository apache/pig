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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.InputSizeReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.tez.TezDagBuilder;
import org.apache.pig.backend.hadoop.executionengine.tez.TezJobCompiler;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.ParallelismSetter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.test.MiniGenericCluster;
import org.apache.pig.test.Util;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTezAutoParallelism {
    private static final String INPUT_FILE1 = TestTezAutoParallelism.class.getName() + "_1";
    private static final String INPUT_FILE2 = TestTezAutoParallelism.class.getName() + "_2";
    private static final String INPUT_DIR = Util.getTestDirectory(TestTezAutoParallelism.class);

    private static PigServer pigServer;
    private static Properties properties;
    private static MiniGenericCluster cluster;

    private static final PathFilter PART_FILE_FILTER = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            if (path.getName().startsWith("part")) {
                return true;
            }
            return false;
        }
    };

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        cluster = MiniGenericCluster.buildCluster(MiniGenericCluster.EXECTYPE_TEZ);
        properties = cluster.getProperties();
        //Test spilling to disk as tests here have multiple splits
        properties.setProperty(PigConfiguration.PIG_TEZ_INPUT_SPLITS_MEM_THRESHOLD, "10");
        properties.setProperty(PigConfiguration.PIG_TEZ_GRACE_PARALLELISM, "false");
        createFiles();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        deleteFiles();
        cluster.shutDown();
    }

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(MiniGenericCluster.EXECTYPE_TEZ, properties);
    }

    @After
    public void tearDown() throws Exception {
        removeProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION);
        removeProperty(MRConfiguration.MAX_SPLIT_SIZE);
        removeProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM);
        removeProperty(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART);
        removeProperty(TezConfiguration.TEZ_AM_LOG_LEVEL);
        pigServer.shutdown();
        pigServer = null;
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
        Util.copyFromLocalToCluster(cluster, INPUT_DIR + "/" + INPUT_FILE1, INPUT_FILE1);

        w = new PrintWriter(new FileWriter(INPUT_DIR + "/" + INPUT_FILE2));
        for (String name : boyNames) {
            w.println(name + "\t" + "M");
        }
        for (String name : girlNames) {
            w.println(name + "\t" + "F");
        }
        w.close();
        Util.copyFromLocalToCluster(cluster, INPUT_DIR + "/" + INPUT_FILE2, INPUT_FILE2);
    }

    private static void deleteFiles() {
        Util.deleteDirectory(new File(INPUT_DIR));
    }

    @Test
    public void testGroupBy() throws IOException{
        // parallelism is 3 originally, reduce to 1
        setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        setProperty(MRConfiguration.MAX_SPLIT_SIZE, "3000");
        setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                Long.toString(InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER));
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = group A by name;");
        pigServer.store("B", "output1");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output1"), PART_FILE_FILTER);
        assertEquals(files.length, 1);
        fs.delete(new Path("output1"), true);
    }

    @Test
    public void testBytesPerReducer() throws IOException{

        NodeIdGenerator.reset();
        PigServer.resetScope();

        setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        setProperty(MRConfiguration.MAX_SPLIT_SIZE, "3000");
        setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "1000");

        StringWriter writer = new StringWriter();
        Util.createLogAppender("testAutoParallelism", writer, TezDagBuilder.class);
        try {
            pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
            pigServer.registerQuery("B = group A by name;");
            pigServer.store("B", "output1");
            FileSystem fs = cluster.getFileSystem();
            FileStatus[] files = fs.listStatus(new Path("output1"), PART_FILE_FILTER);
            assertEquals(files.length, 10);
            String log = writer.toString();
            assertTrue(log.contains("For vertex - scope-13: parallelism=3"));
            assertTrue(log.contains("For vertex - scope-14: parallelism=10"));
        } finally {
            Util.removeLogAppender("testAutoParallelism", TezDagBuilder.class);
            Util.deleteFile(cluster, "output1");
        }
    }

    @Test
    public void testOrderbyDecreaseParallelism() throws IOException{
        // order by parallelism is 3 originally, reduce to 1
        setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        setProperty(MRConfiguration.MAX_SPLIT_SIZE, "3000");
        setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                Long.toString(InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER));
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = group A by name parallel 3;");
        pigServer.registerQuery("C = foreach B generate group as name, AVG(A.age) as age;");
        pigServer.registerQuery("D = order C by age;");
        pigServer.store("D", "output2");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output2"), PART_FILE_FILTER);
        assertEquals(files.length, 1);
    }

    @Test
    public void testOrderbyIncreaseParallelism() throws IOException{
        // order by parallelism is 3 originally, increase to 4
        setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        setProperty(MRConfiguration.MAX_SPLIT_SIZE, "3000");
        setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "1000");
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = group A by name parallel 3;");
        pigServer.registerQuery("C = foreach B generate group as name, AVG(A.age) as age;");
        pigServer.registerQuery("D = order C by age;");
        pigServer.store("D", "output3");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output3"), PART_FILE_FILTER);
        assertEquals(files.length, 4);
    }

    @Test
    public void testSkewedJoinDecreaseParallelism() throws IOException{
        // skewed join parallelism is 4 originally, reduce to 1
        setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        setProperty(MRConfiguration.MAX_SPLIT_SIZE, "3000");
        setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                Long.toString(InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER));
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
        pigServer.registerQuery("C = join A by name, B by name using 'skewed';");
        pigServer.store("C", "output4");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output4"), PART_FILE_FILTER);
        assertEquals(files.length, 1);
    }

    @Test
    public void testSkewedJoinIncreaseParallelism() throws IOException{
        // skewed join parallelism is 3 originally, increase to 5
        setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        setProperty(MRConfiguration.MAX_SPLIT_SIZE, "3000");
        setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "40000");
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
        pigServer.registerQuery("C = join A by name, B by name using 'skewed';");
        pigServer.store("C", "output5");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output5"), PART_FILE_FILTER);
        assertEquals(files.length, 5);
    }

    @Test
    public void testSkewedFullJoinIncreaseParallelism() throws IOException{
        // skewed full join parallelism take the initial setting, since the join vertex has a broadcast(sample) dependency,
        // which prevent it changing parallelism
        setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        setProperty(MRConfiguration.MAX_SPLIT_SIZE, "3000");
        setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "40000");
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
        pigServer.registerQuery("C = join A by name full, B by name using 'skewed';");
        pigServer.store("C", "output6");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output5"), PART_FILE_FILTER);
        assertEquals(files.length, 5);
    }

    @Test
    public void testSkewedJoinIncreaseParallelismWithScalar() throws IOException{
        // skewed join parallelism take the initial setting, since the join vertex has a broadcast(scalar) dependency,
        // which prevent it changing parallelism
        setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        setProperty(MRConfiguration.MAX_SPLIT_SIZE, "3000");
        setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "40000");
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
        pigServer.registerQuery("C = join A by name, B by name using 'skewed';");
        pigServer.registerQuery("D = load 'org.apache.pig.tez.TestTezAutoParallelism_1' as (name:chararray, age:int);");
        pigServer.registerQuery("E = group D all;");
        pigServer.registerQuery("F = foreach E generate COUNT(D) as count;");
        pigServer.registerQuery("G = foreach C generate age/F.count, gender;");
        pigServer.store("G", "output7");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output7"), PART_FILE_FILTER);
        assertEquals(files.length, 4);
    }

    @Test
    public void testSkewedJoinRightInputAutoParallelism() throws IOException{
        setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        setProperty(MRConfiguration.MAX_SPLIT_SIZE, "3000");
        setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "40000");
        setProperty(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, "1.0");
        setProperty(TezConfiguration.TEZ_AM_LOG_LEVEL, "DEBUG");
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = FILTER B by name == 'Noah';");
        pigServer.registerQuery("B1 = group B by name;");
        pigServer.registerQuery("C = join A by name, B1 by group using 'skewed';");
        pigServer.store("C", "output8");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output8"), PART_FILE_FILTER);
        assertEquals(5, files.length);
    }

    @Test
    public void testFlattenParallelism() throws IOException{
        String outputDir = "/tmp/testFlattenParallelism";
        String script = "A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);"
                + "B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);"
                + "C = join A by name, B by name using 'skewed' parallel 1;"
                + "C1 = group C by A::name;"
                + "C2 = FOREACH C1 generate group, FLATTEN(C);"
                + "D = group C2 by group;"
                + "E = foreach D generate group, COUNT(C2.A::name);"
                + "STORE E into '" + outputDir + "/finalout';";
        String log = testAutoParallelism(script, outputDir, true, TezJobCompiler.class, TezDagBuilder.class);
        assertTrue(log.contains("For vertex - scope-74: parallelism=10"));
        assertTrue(log.contains("For vertex - scope-75: parallelism=70"));
        assertTrue(log.contains("Total estimated parallelism is 89"));
    }

    @Test
    public void testIncreaseIntermediateParallelism1() throws IOException{
        // User specified parallelism is overriden for intermediate step
        String outputDir = "/tmp/testIncreaseIntermediateParallelism";
        String script = "A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);"
                + "B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);"
                + "C = join A by name, B by name using 'skewed' parallel 1;"
                + "D = group C by A::name;"
                + "E = foreach D generate group, COUNT(C.A::name);"
                + "STORE E into '" + outputDir + "/finalout';";
        String log = testIncreaseIntermediateParallelism(script, outputDir, true);
        // Parallelism of C should be increased
        assertTrue(log.contains("Increased requested parallelism of scope-59 to 4"));
        assertEquals(1, StringUtils.countMatches(log, "Increased requested parallelism"));
        assertTrue(log.contains("Total estimated parallelism is 40"));
    }

    @Test
    public void testIncreaseIntermediateParallelism2() throws IOException{
        // User specified parallelism should not be overriden for intermediate step if there is a STORE
        String outputDir = "/tmp/testIncreaseIntermediateParallelism";
        String script = "A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);"
                + "B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);"
                + "C = join A by name, B by name using 'skewed' parallel 2;"
                + "STORE C into '/tmp/testIncreaseIntermediateParallelism';"
                + "D = group C by A::name parallel 2;"
                + "E = foreach D generate group, COUNT(C.A::name);"
                + "STORE E into '" + outputDir + "/finalout';";
        String log = testIncreaseIntermediateParallelism(script, outputDir, true);
        // Parallelism of C will not be increased as the Split has a STORE
        assertEquals(0, StringUtils.countMatches(log, "Increased requested parallelism"));
    }

    @Test
    public void testIncreaseIntermediateParallelism3() throws IOException{
        // Multiple levels with default parallelism. Group by followed by Group by
        try {
            String outputDir = "/tmp/testIncreaseIntermediateParallelism";
            String script = "set default_parallel 1\n"
                    + "A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);"
                    + "B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);"
                    + "C = join A by name, B by name;"
                    + "STORE C into '/tmp/testIncreaseIntermediateParallelism';"
                    + "C1 = group C by A::name;"
                    + "C2 = FOREACH C1 generate group, FLATTEN(C);"
                    + "D = group C2 by group;"
                    + "E = foreach D generate group, COUNT(C2.A::name);"
                    + "F = order E by $0;"
                    + "STORE F into '" + outputDir + "/finalout';";
            String log = testIncreaseIntermediateParallelism(script, outputDir, false);
            // Parallelism of C1 should be increased. C2 will not be increased due to order by
            assertEquals(1, StringUtils.countMatches(log, "Increased requested parallelism"));
            assertTrue(log.contains("Increased requested parallelism of scope-65 to 10"));
            assertTrue(log.contains("Total estimated parallelism is 19"));
        } finally {
            pigServer.setDefaultParallel(-1);
        }
    }

    private String testIncreaseIntermediateParallelism(String script, String outputDir, boolean sortAndCheck) throws IOException {
        return testAutoParallelism(script, outputDir, sortAndCheck, ParallelismSetter.class, TezJobCompiler.class);
    }

    private String testAutoParallelism(String script, String outputDir, boolean sortAndCheck, Class... classesToLog) throws IOException {
        NodeIdGenerator.reset();
        PigServer.resetScope();
        StringWriter writer = new StringWriter();
        // When there is a combiner operation involved user specified parallelism is overriden
        Util.createLogAppender("testAutoParallelism", writer, classesToLog);
        try {
            setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
            setProperty(MRConfiguration.MAX_SPLIT_SIZE, "4000");
            setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "80000");
            pigServer.setBatchOn();
            pigServer.registerScript(new ByteArrayInputStream(script.getBytes()));
            pigServer.executeBatch();

            pigServer.registerQuery("A = load '" + outputDir + "/finalout' as (name:chararray, count:long);");
            Iterator<Tuple> iter = pigServer.openIterator("A");

            List<Tuple> expectedResults = Util
                    .getTuplesFromConstantTupleStrings(new String[] {
                            "('Abigail',56L)", "('Alexander',45L)", "('Ava',60L)",
                            "('Daniel',68L)", "('Elizabeth',42L)",
                            "('Emily',57L)", "('Emma',50L)", "('Ethan',50L)",
                            "('Isabella',43L)", "('Jacob',43L)", "('Jayden',59L)",
                            "('Liam',46L)", "('Madison',46L)", "('Mason',54L)",
                            "('Mia',51L)", "('Michael',47L)", "('Noah',38L)",
                            "('Olivia',50L)", "('Sophia',52L)", "('William',43L)" });
            if (sortAndCheck) {
                Util.checkQueryOutputsAfterSort(iter, expectedResults);
            } else {
                Util.checkQueryOutputs(iter, expectedResults);
            }
            return writer.toString();
        } finally {
            Util.removeLogAppender("testAutoParallelism", classesToLog);
            Util.deleteFile(cluster, outputDir);
        }
    }

    private void setProperty(String property, String value) {
        pigServer.getPigContext().getProperties().setProperty(property, value);
    }

    private void removeProperty(String property) {
        pigServer.getPigContext().getProperties().remove(property);
    }
}
