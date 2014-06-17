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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.InputSizeReducerEstimator;
import org.apache.pig.test.MiniGenericCluster;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTezAutoParallelism {
    private static final String INPUT_FILE1 = TestTezAutoParallelism.class.getName() + "_1";
    private static final String INPUT_FILE2 = TestTezAutoParallelism.class.getName() + "_2";
    private static final String INPUT_DIR = "build/test/data";

    private static PigServer pigServer;
    private static Properties properties;
    private static MiniGenericCluster cluster;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        cluster = MiniGenericCluster.buildCluster(MiniGenericCluster.EXECTYPE_TEZ);
        properties = cluster.getProperties();
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
        pigServer.shutdown();
        pigServer = null;
    }

    private static void createFiles() throws IOException {
        new File(INPUT_DIR).mkdir();

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
        pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        pigServer.getPigContext().getProperties().setProperty("mapred.max.split.size", "3000");
        pigServer.getPigContext().getProperties().setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, 
                Long.toString(InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER));
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = group A by name;");
        pigServer.store("B", "output1");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output1"), new PathFilter(){
            public boolean accept(Path path) {
                if (path.getName().startsWith("part")) {
                    return true;
                }
                return false;
            }
        });
        assertEquals(files.length, 1);
    }

    @Test
    public void testOrderbyDecreaseParallelism() throws IOException{
        // order by parallelism is 3 originally, reduce to 1
        pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        pigServer.getPigContext().getProperties().setProperty("mapred.max.split.size", "3000");
        pigServer.getPigContext().getProperties().setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, 
                Long.toString(InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER));
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = group A by name parallel 3;");
        pigServer.registerQuery("C = foreach B generate group as name, AVG(A.age) as age;");
        pigServer.registerQuery("D = order C by age;");
        pigServer.store("D", "output2");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output2"), new PathFilter(){
            public boolean accept(Path path) {
                if (path.getName().startsWith("part")) {
                    return true;
                }
                return false;
            }
        });
        assertEquals(files.length, 1);
    }

    @Test
    public void testOrderbyIncreaseParallelism() throws IOException{
        // order by parallelism is 3 originally, increase to 4
        pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        pigServer.getPigContext().getProperties().setProperty("mapred.max.split.size", "3000");
        pigServer.getPigContext().getProperties().setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "1000");
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = group A by name parallel 3;");
        pigServer.registerQuery("C = foreach B generate group as name, AVG(A.age) as age;");
        pigServer.registerQuery("D = order C by age;");
        pigServer.store("D", "output3");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output3"), new PathFilter(){
            public boolean accept(Path path) {
                if (path.getName().startsWith("part")) {
                    return true;
                }
                return false;
            }
        });
        assertEquals(files.length, 4);
    }

    @Test
    public void testSkewedJoinDecreaseParallelism() throws IOException{
        // skewed join parallelism is 4 originally, reduce to 1
        pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        pigServer.getPigContext().getProperties().setProperty("mapred.max.split.size", "3000");
        pigServer.getPigContext().getProperties().setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, 
                Long.toString(InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER));
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
        pigServer.registerQuery("C = join A by name, B by name using 'skewed';");
        pigServer.store("C", "output4");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output4"), new PathFilter(){
            public boolean accept(Path path) {
                if (path.getName().startsWith("part")) {
                    return true;
                }
                return false;
            }
        });
        assertEquals(files.length, 1);
    }

    @Test
    public void testSkewedJoinIncreaseParallelism() throws IOException{
        // skewed join parallelism is 3 originally, increase to 5
        pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_NO_SPLIT_COMBINATION, "true");
        pigServer.getPigContext().getProperties().setProperty("mapred.max.split.size", "3000");
        pigServer.getPigContext().getProperties().setProperty(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM, "80000");
        pigServer.registerQuery("A = load '" + INPUT_FILE1 + "' as (name:chararray, age:int);");
        pigServer.registerQuery("B = load '" + INPUT_FILE2 + "' as (name:chararray, gender:chararray);");
        pigServer.registerQuery("C = join A by name, B by name using 'skewed';");
        pigServer.store("C", "output5");
        FileSystem fs = cluster.getFileSystem();
        FileStatus[] files = fs.listStatus(new Path("output5"), new PathFilter(){
            public boolean accept(Path path) {
                if (path.getName().startsWith("part")) {
                    return true;
                }
                return false;
            }
        });
        assertEquals(files.length, 5);
    }
}
