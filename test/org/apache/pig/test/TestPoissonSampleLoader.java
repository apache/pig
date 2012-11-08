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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestPoissonSampleLoader {
    private static final String INPUT_FILE1 = "SkewedJoinInput1.txt";

    private PigServer pigServer;
    private static MiniCluster cluster = MiniCluster.buildCluster();

    public TestPoissonSampleLoader() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.LOCAL);
        pigServer.getPigContext().getProperties()
                .setProperty("pig.skewedjoin.reduce.maxtuple", "5");
        pigServer.getPigContext().getProperties()
                .setProperty("pig.skewedjoin.reduce.memusage", "0.0001");
        pigServer.getPigContext().getProperties().setProperty("mapred.child.java.opts", "-Xmx512m");

        pigServer.getPigContext().getProperties().setProperty("pig.mapsplits.count", "5");
    }

    @Before
    public void setUp() throws Exception {
        createFiles();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    private void createFiles() throws IOException {
        PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE1));

        int k = 0;
        for (int j = 0; j < 100; j++) {
            w.println("100:apple1:aaa" + k);
            k++;
            w.println("200:orange1:bbb" + k);
            k++;
            w.println("300:strawberry:ccc" + k);
            k++;
        }

        w.close();

        Util.copyFromLocalToCluster(cluster, INPUT_FILE1, INPUT_FILE1);
    }

    @After
    public void tearDown() throws Exception {
        new File(INPUT_FILE1).delete();

        Util.deleteFile(cluster, INPUT_FILE1);
    }

    private int testNumSamples(String memUsage, String sampleRate) throws IOException {
        pigServer.getPigContext().getProperties()
                .setProperty("pig.skewedjoin.reduce.memusage", memUsage);
        pigServer.getPigContext().getProperties()
                .setProperty("pig.sksampler.samplerate", sampleRate);
        pigServer.registerQuery("A = Load '" + INPUT_FILE1
                + "' Using PoissonSampleLoader('PigStorage()', '100');");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        return count;
    }

    /*
     * Test use of LoadFunc with parameters as argument to PoissonSampleLoader
     */
    @Test
    public void testInstantiation() throws IOException {
        pigServer.registerQuery("A = Load '" + INPUT_FILE1
                + "' Using PoissonSampleLoader('PigStorage(\\\\\\':\\\\\\')', '100');");
        Iterator<Tuple> iter = pigServer.openIterator("A");
        assertTrue(iter.hasNext());

        Tuple t = iter.next();
        // Check the tuple size. It has to be 3.
        assertEquals(3, t.size());

        while (iter.hasNext()) {
            t = iter.next();
        }
        // Last tuple's size has to be 5
        // 3 datum (ex: 100:apple1:aaa)
        // + PoissonSampleLoader.NUMROWS_TUPLE_MARKER
        // ??_pig_inTeRnal-spEcial_roW_num_tuple3kt579CFLehkblah
        // + numRow 300
        assertEquals(5, t.size());
    }
}