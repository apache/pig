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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestCustomPartitioner {
    private static MiniGenericCluster cluster;
    private static Properties properties;
    private static PigServer pigServer;
    private static FileSystem fs;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();

    @Before
    public void setUp() throws Exception {
        FileLocalizer.setR(new Random());
        pigServer = new PigServer(cluster.getExecType(), properties);
    }

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        cluster = MiniGenericCluster.buildCluster();
        properties = cluster.getProperties();
        fs = cluster.getFileSystem();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    // See PIG-282
    @Test
    @Ignore
    // Fails with Tez - DefaultSorter.java Illegal partition for Null: false index: 0 1 (-1), TotalPartitions: 0
    public void testCustomPartitionerParseJoins() throws Exception{
        String[] input = {
                "1\t3",
                "1\t2"
        };
        Util.createInputFile(cluster, "table_testCustomPartitionerParseJoins", input);

        // Custom Partitioner is not allowed for skewed joins, will throw a ExecException
        try {
            pigServer.registerQuery("A = LOAD 'table_testCustomPartitionerParseJoins' as (a0:int, a1:int);");
            pigServer.registerQuery("B = ORDER A by $0;");
            pigServer.registerQuery("skewed = JOIN A by $0, B by $0 USING 'skewed' PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner;");
            //control should not reach here
            Assert.fail("Skewed join cannot accept a custom partitioner");
        } catch(FrontendException e) {
            Assert.assertTrue( e.getMessage().contains( "Custom Partitioner is not supported for skewed join" ) );
        }

        pigServer.registerQuery("hash = JOIN A by $0, B by $0 USING 'hash' PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner;");
        Iterator<Tuple> iter = pigServer.openIterator("hash");

        List<String> expected = new ArrayList<String>();
        expected.add("(1,3,1,2)");
        expected.add("(1,3,1,3)");
        expected.add("(1,2,1,2)");
        expected.add("(1,2,1,3)");
        Collections.sort(expected);

        List<String> actual = new ArrayList<String>();
        while (iter.hasNext()) {
            actual.add(iter.next().toString());
        }
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);

        // No checks are made for merged and replicated joins as they are compiled to a map only job
        // No frontend error checking has been added for these jobs, hence not adding any test cases
        // Manually tested the sanity once. Above test should cover the basic sanity of the scenario

        Util.deleteFile(cluster, "table_testCustomPartitionerParseJoins");
    }

    // See PIG-282
    @Test
    public void testCustomPartitionerGroups() throws Exception{
        String[] input = {
                "1\t1",
                "2\t1",
                "3\t1",
                "4\t1"
        };
        Util.createInputFile(cluster, "table_testCustomPartitionerGroups", input);

        String outputDir = "tmp_testCustomPartitionerGroup";
        pigServer.registerQuery("A = LOAD 'table_testCustomPartitionerGroups' as (a0:int, a1:int);");
        // It should be noted that for a map reduce job, the total number of partitions
        // is the same as the number of reduce tasks for the job. Hence we need to find a case wherein
        // we will get more than one reduce job so that we can use the partitioner.
        // The following logic assumes that we get 2 reduce jobs, so that we can hard-code the logic.
        // SimpleCustomPartitioner3 simply returns '1' (second reducer) for all inputs when
        // partition number is bigger than 1.
        //
        pigServer.registerQuery("B = group A by $0 PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner3 parallel 2;");
        pigServer.store("B", outputDir);

        new File(outputDir).mkdir();
        FileStatus[] outputFiles = fs.listStatus(new Path(outputDir), Util.getSuccessMarkerPathFilter());

        Util.copyFromClusterToLocal(cluster, outputFiles[0].getPath().toString(), outputDir + "/" + 0);
        BufferedReader reader = new BufferedReader(new FileReader(outputDir + "/" + 0));
        while(reader.readLine() != null) {
            Assert.fail("Partition 0 should be empty.  Most likely Custom Partitioner was not used.");
        }
        reader.close();

        Util.copyFromClusterToLocal(cluster, outputFiles[1].getPath().toString(), outputDir + "/" + 1);
        reader = new BufferedReader(new FileReader(outputDir + "/" + 1));
        int count=0;
        while(reader.readLine() != null) {
            //all outputs should come to partion 1 (with SimpleCustomPartitioner3)
            count++;
        }
        reader.close();
        Assert.assertEquals(4, count);

        Util.deleteDirectory(new File(outputDir));
        Util.deleteFile(cluster, outputDir);
        Util.deleteFile(cluster, "table_testCustomPartitionerGroups");
    }

    // See PIG-3385
    @Test
    public void testCustomPartitionerDistinct() throws Exception{
        String[] input = {
                "1\t1",
                "2\t1",
                "1\t1",
                "3\t1",
                "4\t1",
        };
        Util.createInputFile(cluster, "table_testCustomPartitionerDistinct", input);

        String outputDir = "tmp_testCustomPartitionerDistinct";
        pigServer.registerQuery("A = LOAD 'table_testCustomPartitionerDistinct' as (a0:int, a1:int);");
        pigServer.registerQuery("B = distinct A PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner3 parallel 2;");
        pigServer.store("B", outputDir);

        new File(outputDir).mkdir();
        FileStatus[] outputFiles = fs.listStatus(new Path(outputDir), Util.getSuccessMarkerPathFilter());

        // SimpleCustomPartitioner3 simply partition all inputs to *second* reducer
        Util.copyFromClusterToLocal(cluster, outputFiles[0].getPath().toString(), outputDir + "/" + 0);
        BufferedReader reader = new BufferedReader(new FileReader(outputDir + "/" + 0));
        while (reader.readLine() != null) {
            Assert.fail("Partition 0 should be empty.  Most likely Custom Partitioner was not used.");
        }
        reader.close();

        Util.copyFromClusterToLocal(cluster, outputFiles[1].getPath().toString(), outputDir + "/" + 1);
        reader = new BufferedReader(new FileReader(outputDir + "/" + 1));
        int count=0;
        while (reader.readLine() != null) {
            //all outputs should come to partion 1 (with SimpleCustomPartitioner3)
            count++;
        }
        reader.close();
        Assert.assertEquals(4, count);

        Util.deleteDirectory(new File(outputDir));
        Util.deleteFile(cluster, outputDir);
        Util.deleteFile(cluster, "table_testCustomPartitionerDistinct");
    }

    // See PIG-282
    @Test
    @Ignore
    // TODO: CROSS not implemented in TEZ yet
    public void testCustomPartitionerCross() throws Exception{
    	String[] input = {
                "1\t3",
                "1\t2",
        };

        Util.createInputFile(cluster, "table_testCustomPartitionerCross", input);
        pigServer.registerQuery("A = LOAD 'table_testCustomPartitionerCross' as (a0:int, a1:int);");
        pigServer.registerQuery("B = ORDER A by $0;");
        pigServer.registerQuery("C = cross A , B PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t;

        Collection<String> results = new HashSet<String>();
        results.add("(1,3,1,2)");
        results.add("(1,3,1,3)");
        results.add("(1,2,1,2)");
        results.add("(1,2,1,3)");

        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));

        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));

        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));

        Assert.assertTrue(iter.hasNext());
        t = iter.next();
        Assert.assertTrue(t.size()==4);
        Assert.assertTrue(results.contains(t.toString()));

        Util.deleteFile(cluster, "table_testCustomPartitionerCross");
    }
}
