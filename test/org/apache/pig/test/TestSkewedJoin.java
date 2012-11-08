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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestSkewedJoin {
    private static final String INPUT_FILE1 = "SkewedJoinInput1.txt";
    private static final String INPUT_FILE2 = "SkewedJoinInput2.txt";
    private static final String INPUT_FILE3 = "SkewedJoinInput3.txt";
    private static final String INPUT_FILE4 = "SkewedJoinInput4.txt";
    private static final String INPUT_FILE5 = "SkewedJoinInput5.txt";
    private static final String INPUT_FILE6 = "SkewedJoinInput6.txt";
    private static final String INPUT_FILE7 = "SkewedJoinInput7.txt";

    private PigServer pigServer;
    private static MiniCluster cluster = MiniCluster.buildCluster();

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.getPigContext().getProperties().setProperty("pig.skewedjoin.reduce.maxtuple", "5");
        pigServer.getPigContext().getProperties().setProperty("pig.skewedjoin.reduce.memusage", "0.01");
        createFiles();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    private void createFiles() throws IOException {
        PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE1));

        int k = 0;
        for(int j=0; j<120; j++) {
               w.println("100\tapple1\taaa" + k);
            k++;
            w.println("200\torange1\tbbb" + k);
            k++;
            w.println("300\tstrawberry\tccc" + k);
            k++;
        }

        w.close();

        PrintWriter w2 = new PrintWriter(new FileWriter(INPUT_FILE2));
        w2.println("100\tapple1");
        w2.println("100\tapple2");
        w2.println("100\tapple2");
        w2.println("200\torange1");
        w2.println("200\torange2");
        w2.println("300\tstrawberry");
        w2.println("400\tpear");

        w2.close();

        PrintWriter w3 = new PrintWriter(new FileWriter(INPUT_FILE3));
        w3.println("100\tapple1");
        w3.println("100\tapple2");
        w3.println("200\torange1");
        w3.println("200\torange2");
        w3.println("300\tstrawberry");
        w3.println("300\tstrawberry2");
        w3.println("400\tpear");

        w3.close();

        PrintWriter w4 = new PrintWriter(new FileWriter(INPUT_FILE4));
        for(int i=0; i < 100; i++) {
            w4.println("[a100#apple1,a100#apple2,a200#orange1,a200#orange2,a300#strawberry,a300#strawberry2,a400#pear]");
        }
        w4.close();

        // Create a file with null keys
        PrintWriter w5 = new PrintWriter(new FileWriter(INPUT_FILE5));
        for(int i=0; i < 10; i++) {
            w5.println("\tapple1");
        }
        w5.println("100\tapple2");
        for(int i=0; i < 10; i++) {
            w5.println("\torange1");
        }
        w5.println("\t");
        w5.println("100\t");
        w5.close();

        PrintWriter w6 = new PrintWriter(new FileWriter(INPUT_FILE6));

        for(int i=0; i<300; i++) {
            for(int j=0; j<5; j++) {
                w6.println(""+i+"\t"+j);
            }
        }
        w6.close();

        PrintWriter w7 = new PrintWriter(new FileWriter(INPUT_FILE7));

        for(int i=0; i<300; i = i+3) {
            for(int j=0; j<2; j++) {
                w7.println(""+i+"\t"+j);
            }
        }
        w7.close();

        Util.copyFromLocalToCluster(cluster, INPUT_FILE1, INPUT_FILE1);
        Util.copyFromLocalToCluster(cluster, INPUT_FILE2, INPUT_FILE2);
        Util.copyFromLocalToCluster(cluster, INPUT_FILE3, INPUT_FILE3);
        Util.copyFromLocalToCluster(cluster, INPUT_FILE4, INPUT_FILE4);
        Util.copyFromLocalToCluster(cluster, INPUT_FILE5, INPUT_FILE5);
        Util.copyFromLocalToCluster(cluster, INPUT_FILE6, INPUT_FILE6);
        Util.copyFromLocalToCluster(cluster, INPUT_FILE7, INPUT_FILE7);
    }

    @After
    public void tearDown() throws Exception {
        new File(INPUT_FILE1).delete();
        new File(INPUT_FILE2).delete();
        new File(INPUT_FILE3).delete();
        new File(INPUT_FILE4).delete();
        new File(INPUT_FILE5).delete();
        new File(INPUT_FILE6).delete();
        new File(INPUT_FILE7).delete();
        Util.deleteDirectory(new File("skewedjoin"));

        Util.deleteFile(cluster, INPUT_FILE1);
        Util.deleteFile(cluster, INPUT_FILE2);
        Util.deleteFile(cluster, INPUT_FILE3);
        Util.deleteFile(cluster, INPUT_FILE4);
        Util.deleteFile(cluster, INPUT_FILE5);
        Util.deleteFile(cluster, INPUT_FILE6);
        Util.deleteFile(cluster, INPUT_FILE7);
    }

    @Test
    public void testSkewedJoinWithGroup() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");
        pigServer.registerQuery("C = GROUP A by id;");
        pigServer.registerQuery("D = GROUP B by id;");

        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("E = join C by group, D by group using 'skewed' parallel 5;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("E = join C by group, D by group;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size()>0);
        assertTrue(dbshj.size()>0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testSkewedJoinWithNoProperties() throws IOException{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
        DataBag dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by (id, name), B by (id, name) using 'skewed' parallel 5;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("E = join A by(id, name), B by (id, name);");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size()>0);
        assertTrue(dbshj.size()>0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testSkewedJoinReducers() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
        pigServer.registerQuery("C = join A by id, B by id using 'skewed' parallel 1;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        while(iter.hasNext()) {
            dbfrj.add(iter.next());
        }
    }

    @Test(expected = FrontendException.class)
    public void testSkewedJoin3Way() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");
        pigServer.registerQuery("C = LOAD '" + INPUT_FILE3 + "' as (id, name);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
        pigServer.registerQuery("D = join A by id, B by id, C by id using 'skewed' parallel 5;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        while(iter.hasNext())
            dbfrj.add(iter.next());
    }

    @Test
    public void testSkewedJoinMapKey() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE4 + "' as (m:[]);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE4 + "' as (n:[]);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
        pigServer.registerQuery("C = join A by (chararray)m#'a100', B by (chararray)n#'a100' using 'skewed' parallel 20;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        while(iter.hasNext()) {
            dbfrj.add(iter.next());
        }
    }


    @Test
    public void testSkewedJoinKeyPartition() throws IOException {
        try{
             Util.deleteFile(cluster, "skewedjoin");
        }catch(Exception e){
            // it is ok if directory not exist
        }

         pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
         pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");


         pigServer.registerQuery("E = join A by id, B by id using 'skewed' parallel 7;");
         pigServer.store("E", "skewedjoin");

         int[][] lineCount = new int[3][7];

         new File("skewedjoin").mkdir();
         // check how many times a key appear in each part- file
         for(int i=0; i<7; i++) {
             Util.copyFromClusterToLocal(cluster, "skewedjoin/part-r-0000"+i, "skewedjoin/part-r-0000"+i);

             BufferedReader reader = new BufferedReader(new FileReader("skewedjoin/part-r-0000"+i));
               String line = null;
               while((line = reader.readLine()) != null) {
                  String[] cols = line.split("\t");
                  int key = Integer.parseInt(cols[0])/100 -1;
                  lineCount[key][i] ++;
              }
         }

         int fc = 0;
         for(int i=0; i<3; i++) {
             for(int j=0; j<7; j++) {
                 if (lineCount[i][j] > 0) {
                     fc ++;
                 }
             }
         }
         // atleast one key should be a skewed key
         // check atleast one key should appear in more than 1 part- file
         assertTrue(fc > 3);
    }

    @Test
    public void testSkewedJoinNullKeys() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE5 + "' as (id,name);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE5 + "' as (id,name);");
        try {
            DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
            {
                pigServer.registerQuery("C = join A by id, B by id using 'skewed';");
                Iterator<Tuple> iter = pigServer.openIterator("C");

                while(iter.hasNext()) {
                    dbfrj.add(iter.next());
                }
            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            fail("Should support null keys in skewed join");
        }
        return;
    }

    @Test
    public void testSkewedJoinOuter() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE5 + "' as (id,name);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE5 + "' as (id,name);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by id left, B by id using 'skewed';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by id right, B by id using 'skewed';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by id full, B by id using 'skewed';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
    }

    // pig 1048
    @Test
    public void testSkewedJoinOneValue() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE3 + "' as (id,name);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE3 + "' as (id,name);");
        // Filter key with a single value

        pigServer.registerQuery("C = FILTER A by id == 400;");
        pigServer.registerQuery("D = FILTER B by id == 400;");


        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbrj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("E = join C by id, D by id using 'skewed';");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("E = join C by id, D by id;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbrj.add(iter.next());
            }
        }
        assertEquals(dbfrj.size(), dbrj.size());
        assertTrue(TestHelper.compareBags(dbfrj, dbrj));

    }

    @Test
    public void testSkewedJoinManyReducers() throws IOException {
        pigServer.getPigContext().getProperties().setProperty("pig.skewedjoin.reduce.maxtuple", "2");
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE6 + "' as (id,name);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE7 + "' as (id,name);");

        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbrj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("E = join A by id, B by id using 'skewed' parallel 300;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("E = join A by id, B by id;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while(iter.hasNext()) {
                dbrj.add(iter.next());
            }
        }
        assertEquals(dbfrj.size(), dbrj.size());
        assertTrue(TestHelper.compareBags(dbfrj, dbrj));

    }

    @Test
    public void testSkewedJoinEmptyInput() throws IOException {
        String LEFT_INPUT_FILE = "left.dat";
        String RIGHT_INPUT_FILE = "right.dat";

        PrintWriter w = new PrintWriter(new FileWriter(LEFT_INPUT_FILE));
        w.println("1");
        w.println("2");
        w.println("3");
        w.println("5");
        w.close();

        Util.copyFromLocalToCluster(cluster, LEFT_INPUT_FILE, LEFT_INPUT_FILE);

        PrintWriter w2 = new PrintWriter(new FileWriter(RIGHT_INPUT_FILE));
        w2.println("1\tone");
        w2.println("2\ttwo");
        w2.println("3\tthree");

        w2.close();

        Util.copyFromLocalToCluster(cluster, RIGHT_INPUT_FILE, RIGHT_INPUT_FILE);

        pigServer.registerQuery("a = load 'left.dat' as (nums:chararray);");
        pigServer.registerQuery("b = load 'right.dat' as (number:chararray,text:chararray);");
        pigServer.registerQuery("c = filter a by nums == '7';");
        pigServer.registerQuery("d = join c by nums LEFT OUTER, b by number USING 'skewed';");

        Iterator<Tuple> iter = pigServer.openIterator("d");

        assertFalse(iter.hasNext());

        new File(LEFT_INPUT_FILE).delete();
        Util.deleteFile(cluster, LEFT_INPUT_FILE);
        new File(RIGHT_INPUT_FILE).delete();
        Util.deleteFile(cluster, RIGHT_INPUT_FILE);
    }

    @Test
    public void testRecursiveFileListing() throws IOException {
        String LOCAL_INPUT_FILE = "test.dat";
        String INPUT_FILE = "foo/bar/test.dat";

        PrintWriter w = new PrintWriter(new FileWriter(LOCAL_INPUT_FILE));
        w.println("1");
        w.println("2");
        w.println("3");
        w.println("5");
        w.close();

        Util.copyFromLocalToCluster(cluster, LOCAL_INPUT_FILE, INPUT_FILE);

        pigServer.registerQuery("a = load 'foo' as (nums:chararray);");
        pigServer.registerQuery("b = load 'foo' as (nums:chararray);");
        pigServer.registerQuery("d = join a by nums, b by nums USING 'skewed';");

        Iterator<Tuple> iter = pigServer.openIterator("d");
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        assertEquals(4, count);

        new File(LOCAL_INPUT_FILE).delete();
        Util.deleteFile(cluster, INPUT_FILE);

    }

    @Test
    public void testSkewedJoinUDF() throws IOException {
        PartitionSkewedKeys udf = new PartitionSkewedKeys(new String[]{"0.1", "2", "1.txt"});
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(3);    // use 3 reducers
        DataBag db = new DefaultDataBag();
        Tuple sample;
        for (int i=0;i<=3;i++) {
            sample = TupleFactory.getInstance().newTuple();
            if (i!=3)
                sample.append("1");
            else
                sample.append("2");
            sample.append((long)200);
            if (i!=3)
                sample.append((long)0);
            else
                sample.append((long)30);
            db.add(sample);
        }
        t.append(db);
        Map<String, Object> output = udf.exec(t);
        DataBag parList = (DataBag)output.get(PartitionSkewedKeys.PARTITION_LIST);
        for (Tuple par : parList) {
            if (par.get(0).equals("1")) {
                par.get(1).equals(0);
                par.get(2).equals(2);
            }
        }
    }
}
