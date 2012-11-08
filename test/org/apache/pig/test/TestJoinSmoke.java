/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestJoinSmoke {
    private static final String FR_INPUT_FILE = "testFrJoinInput.txt";

    private static final String SKEW_INPUT_FILE1 = "SkewedJoinInput1.txt";
    private static final String SKEW_INPUT_FILE2 = "SkewedJoinInput2.txt";
    private static final String SKEW_INPUT_FILE5 = "SkewedJoinInput5.txt";

    private PigServer pigServer;
    private static MiniCluster cluster = MiniCluster.buildCluster();
    private File tmpFile;

    public TestJoinSmoke() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    @Before
    public void setUp() throws Exception {
        setupFRJoin();
        setupSkewJoin();
    }

    private void setupFRJoin() throws Exception {
        int LOOP_SIZE = 2;
        String[] input = new String[2 * LOOP_SIZE];
        int k = 0;
        for (int i = 1; i <= LOOP_SIZE; i++) {
            String si = i + "";
            for (int j = 1; j <= LOOP_SIZE; j++)
                input[k++] = si + "\t" + j;
        }
        Util.createInputFile(cluster, FR_INPUT_FILE, input);
    }

    private void setupSkewJoin() throws IOException {
        PrintWriter w = new PrintWriter(new FileWriter(SKEW_INPUT_FILE1));

        int k = 0;
        for (int j = 0; j < 120; j++) {
            w.println("100\tapple1\taaa" + k);
            k++;
            w.println("200\torange1\tbbb" + k);
            k++;
            w.println("300\tstrawberry\tccc" + k);
            k++;
        }

        w.close();

        PrintWriter w2 = new PrintWriter(new FileWriter(SKEW_INPUT_FILE2));
        w2.println("100\tapple1");
        w2.println("100\tapple2");
        w2.println("100\tapple2");
        w2.println("200\torange1");
        w2.println("200\torange2");
        w2.println("300\tstrawberry");
        w2.println("400\tpear");

        w2.close();

        // Create a file with null keys
        PrintWriter w5 = new PrintWriter(new FileWriter(SKEW_INPUT_FILE5));
        for (int i = 0; i < 10; i++) {
            w5.println("\tapple1");
        }
        w5.println("100\tapple2");
        for (int i = 0; i < 10; i++) {
            w5.println("\torange1");
        }
        w5.println("\t");
        w5.println("100\t");
        w5.close();

        Util.copyFromLocalToCluster(cluster, SKEW_INPUT_FILE1, SKEW_INPUT_FILE1);
        Util.copyFromLocalToCluster(cluster, SKEW_INPUT_FILE2, SKEW_INPUT_FILE2);
        Util.copyFromLocalToCluster(cluster, SKEW_INPUT_FILE5, SKEW_INPUT_FILE5);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @After
    public void tearDown() throws Exception {
        Util.deleteFile(cluster, FR_INPUT_FILE);
        tearDownSkewJoin();
    }

    private void tearDownSkewJoin() throws Exception {
        new File(SKEW_INPUT_FILE1).delete();
        new File(SKEW_INPUT_FILE2).delete();
        new File(SKEW_INPUT_FILE5).delete();
        Util.deleteDirectory(new File("skewedjoin"));

        Util.deleteFile(cluster, SKEW_INPUT_FILE1);
        Util.deleteFile(cluster, SKEW_INPUT_FILE2);
        Util.deleteFile(cluster, SKEW_INPUT_FILE5);
    }

    @Test
    public void testFRJoin() throws IOException {
        pigServer.registerQuery("A = LOAD '" + FR_INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + FR_INPUT_FILE + "' as (x:int,y:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testSkewedJoinWithGroup() throws IOException {
        pigServer.registerQuery("A = LOAD '" + SKEW_INPUT_FILE1 + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + SKEW_INPUT_FILE2 + "' as (id, name);");
        pigServer.registerQuery("C = GROUP A by id;");
        pigServer.registerQuery("D = GROUP B by id;");

        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("E = join C by group, D by group using 'skewed' parallel 5;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("E = join C by group, D by group;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testSkewedJoinOuter() throws IOException {
        pigServer.registerQuery("A = LOAD '" + SKEW_INPUT_FILE5 + "' as (id,name);");
        pigServer.registerQuery("B = LOAD '" + SKEW_INPUT_FILE5 + "' as (id,name);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("C = join A by id left, B by id using 'skewed';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by id right, B by id using 'skewed';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by id full, B by id using 'skewed';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
    }
}
