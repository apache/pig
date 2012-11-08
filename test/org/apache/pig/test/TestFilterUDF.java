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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.test.utils.FILTERFROMFILE;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestFilterUDF {
    private PigServer pigServer;
    private static MiniCluster cluster = MiniCluster.buildCluster();
    private File tmpFile;

    TupleFactory tf = TupleFactory.getInstance();

    public TestFilterUDF() throws ExecException, IOException {
        int LOOP_SIZE = 20;
        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for (int i = 1; i <= LOOP_SIZE; i++) {
            ps.println(i);
        }
        ps.close();
    }

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        int LOOP_SIZE = 20;
        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for (int i = 1; i <= LOOP_SIZE; i++) {
            ps.println(i);
        }
        ps.close();
    }

    @After
    public void tearDown() throws Exception {
        tmpFile.delete();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    private File createFile(String[] data) throws Exception {
        File f = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(f);
        for (int i = 0; i < data.length; i++) {
            pw.println(data[i]);
        }
        pw.close();
        return f;
    }

    static public class MyFilterFunction extends EvalFunc<Boolean> {

        @Override
        public Boolean exec(Tuple input) throws IOException {
            try {
                int col = (Integer)input.get(0);
                if (col > 10)
                    return true;
            } catch (ExecException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return false;
        }

    }

    @Test
    public void testFilterUDF() throws Exception {

        pigServer.registerQuery("A = LOAD '"
                + Util.generateURI(tmpFile.toString(), pigServer.getPigContext())
                + "' as (x:int);");
        pigServer.registerQuery("B = filter A by " + MyFilterFunction.class.getName() + "($0);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        assertTrue("No Output received", iter.hasNext());
        int cnt = 0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(true, (Integer)t.get(0) > 10);
            ++cnt;
        }
        assertEquals(10, cnt);
    }

    @Test
    public void testFilterUDFusingDefine() throws Exception {
        File inputFile = createFile(
                new String[] {
                                "www.paulisageek.com\t4",
                                "www.yahoo.com\t12344",
                                "google.com\t1",
                                "us2.amazon.com\t4141"
                }
                );

        File filterFile = createFile(
                new String[] {
                                "12344"
                }
                );

        pigServer.registerQuery("define FILTER_CRITERION "
                + FILTERFROMFILE.class.getName()
                + "('"
                + Util.generateURI(filterFile.toString(), pigServer
                        .getPigContext()) + "');");
        pigServer.registerQuery("a = LOAD '"
                + Util.generateURI(inputFile.toString(), pigServer
                        .getPigContext())
                + "' as (url:chararray, numvisits:int);");
        pigServer.registerQuery("b = filter a by FILTER_CRITERION(numvisits);");

        Tuple expectedTuple = tf.newTuple();
        expectedTuple.append(new String("www.yahoo.com"));
        expectedTuple.append(new Integer("12344"));

        Iterator<Tuple> iter = pigServer.openIterator("b");
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(t.equals(expectedTuple));
        }
    }

    @Test
    public void testFilterUDFusingDefine2() throws Exception {
        File inputFile = createFile(
                new String[] {
                                "www.paulisageek.com\t4",
                                "www.yahoo.com\t12344",
                                "google.com\t1",
                                "us2.amazon.com\t4141"
                }
                );

        File filterFile = createFile(
                new String[] {
                                "12344"
                }
                );

        pigServer.registerQuery("define FILTER_CRITERION "
                + FILTERFROMFILE.class.getName()
                + "('"
                + Util.generateURI(filterFile.toString(), pigServer
                        .getPigContext()) + "');");
        pigServer.registerQuery("a = LOAD '"
                + Util.generateURI(inputFile.toString(), pigServer
                        .getPigContext())
                + "' as (url:chararray, numvisits:int);");
        pigServer.registerQuery("b = filter a by FILTER_CRITERION(numvisits) AND FILTER_CRITERION(numvisits);");

        Tuple expectedTuple = tf.newTuple();
        expectedTuple.append(new String("www.yahoo.com"));
        expectedTuple.append(new Integer("12344"));

        Iterator<Tuple> iter = pigServer.openIterator("b");
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertTrue(t.equals(expectedTuple));
        }
    }
}
