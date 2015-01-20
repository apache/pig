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

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases to test join statement
 */

public class TestJoin extends TestJoinBase {
    private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        if (cluster != null) cluster.shutDown();
    }

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
    }

    protected String createInputFile(String fileNameHint, String[] data) throws IOException {
        String fileName = "";
        Util.createInputFile(cluster, fileNameHint, data);
        fileName = fileNameHint;
        return fileName;
    }

    protected void deleteInputFile(String fileName) throws Exception {
        Util.deleteFile(cluster, fileName);
    }

    @Test
    public void testJoinWithMissingFieldsInTuples() throws Exception{
        String[] input1 = {
                "ff ff ff",
                "",
                "",
                "",
                "",
                "ff ff ff",
                "",
                ""
                };
        String[] input2 = {
                "",
                "",
                "",
                "",
                ""
                };

        String firstInput = createInputFile("a.txt", input1);
        String secondInput = createInputFile("b.txt", input2);
        String script = "a = load 'a.txt'  using PigStorage(' ');" +
        "b = load 'b.txt'  using PigStorage('\u0001');" +
        "c = join a by $0, b by $0;";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("c");
        assertFalse(it.hasNext());
        deleteInputFile(firstInput);
        deleteInputFile(secondInput);
    }
}
