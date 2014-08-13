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

import static org.apache.pig.ExecType.LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.Test;

public class TestPigSplit {

    protected final Log log = LogFactory.getLog(getClass());

    private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();
    private static ExecType[] execTypes = new ExecType[] { ExecType.LOCAL, cluster.getExecType() };
    protected PigServer pigServer;

    /**
     * filename of input in each of the tests
     */
    String inputFileName;

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        if (cluster != null)
            cluster.shutDown();
    }

    public void setUp(ExecType execType) throws Exception {
        Util.resetStateForExecModeSwitch();
        if (execType == cluster.getExecType()) {
            pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
        } else {
            pigServer = new PigServer(LOCAL);
        }
    }

    private void createInput(String[] data, ExecType execType) throws IOException {
        if (execType == cluster.getExecType()) {
            Util.createInputFile(cluster, inputFileName, data);
        } else if (execType == ExecType.LOCAL) {
            Util.createLocalInputFile(inputFileName, data);
        } else {
            throw new IOException("unknown exectype:" + execType.toString());
        }
    }

    public void tearDown(ExecType execType) throws Exception {
        if (execType == cluster.getExecType()) {
            Util.deleteFile(cluster, inputFileName);
        } else if (execType == ExecType.LOCAL) {
            new File(inputFileName).delete();
        } else {
            throw new IOException("unknown exectype:" + execType.toString());
        }
        pigServer.shutdown();
    }

    @Test
    public void testLongEvalSpec2() throws Exception {
        inputFileName = "notestLongEvalSpec-input.txt";
        for (ExecType execType : execTypes) {
            try {
                setUp(execType);
                createInput(new String[] { "0\ta" }, execType);
                pigServer.registerQuery("a = load '" + inputFileName + "';");
                for (int i = 0; i < 200; i++) {
                    pigServer.registerQuery("a = filter a by $0 == '1';");
                }
                Iterator<Tuple> iter = pigServer.openIterator("a");
                assertFalse(iter.hasNext());
            } finally {
                tearDown(execType);
            }
        }
    }

    @Test
    public void testSchemaWithSplit() throws Exception {
        inputFileName = "testSchemaWithSplit-input.txt";
        String[] input = { "2", "12", "42" };
        for (ExecType execType : execTypes) {
            try {
                setUp(execType);
                createInput(input, execType);
                pigServer.registerQuery("a = load '" + inputFileName
                        + "' as (value:chararray);");
                pigServer
                        .registerQuery("split a into b if value < '20', c if value > '10';");
                pigServer.registerQuery("b1 = order b by value;");
                pigServer.registerQuery("c1 = order c by value;");

                // order in lexicographic, so 12 comes before 2
                Iterator<Tuple> iter = pigServer.openIterator("b1");
                assertTrue("b1 has an element", iter.hasNext());
                assertEquals("first item in b1", "12", iter.next().get(0));
                assertTrue("b1 has an element", iter.hasNext());
                assertEquals("second item in b1", "2", iter.next().get(0));
                assertFalse("b1 is over", iter.hasNext());

                iter = pigServer.openIterator("c1");
                assertTrue("c1 has an element", iter.hasNext());
                assertEquals("first item in c1", "12", iter.next().get(0));
                assertTrue("c1 has an element", iter.hasNext());
                assertEquals("second item in c1", "2", iter.next().get(0));
                assertTrue("c1 has an element", iter.hasNext());
                assertEquals("third item in c1", "42", iter.next().get(0));
                assertFalse("c1 is over", iter.hasNext());
            } finally {
                tearDown(execType);
            }
        }
    }

    @Test
    public void testLongEvalSpec() throws Exception {
        inputFileName = "testLongEvalSpec-input.txt";
        String[] input = new String[500];
        for (int i = 0; i < 500; i++) {
            input[i] = ("0\ta");
        }
        for (ExecType execType : execTypes) {
            try {
                setUp(execType);
                createInput(input, execType);
                pigServer.registerQuery("a = load '" + inputFileName + "';");
                pigServer.registerQuery("a = filter a by $0 == '1';");

                Iterator<Tuple> iter = pigServer.openIterator("a");
                assertFalse(iter.hasNext());
            } finally {
                tearDown(execType);
            }
        }
    }
}
