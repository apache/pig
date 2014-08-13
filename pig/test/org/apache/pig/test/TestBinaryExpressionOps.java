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


import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBinaryExpressionOps {
    private static Properties properties;
    private static MiniGenericCluster cluster;
    private static final String INPUT_1 = "input1";
    private static final String INPUT_2 = "input2";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String[] inputData1 = new String[] {
                "id2\t10"
        };
        String[] inputData2 = new String[] {
                "id1\t2", "id2\t2"
        };
        Util.createInputFile(cluster, INPUT_1, inputData1);
        Util.createInputFile(cluster, INPUT_2, inputData2);
    }

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        cluster = MiniGenericCluster.buildCluster();
        properties = cluster.getProperties();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testArithmeticOperators() throws Exception {
        PigServer pig = new PigServer(cluster.getExecType(), properties);

        pig.registerQuery("A = LOAD '" + INPUT_1 + "' AS (id:chararray, val:long);");
        pig.registerQuery("B = LOAD '" + INPUT_2 + "' AS (id:chararray, val:long);");
        pig.registerQuery("C = COGROUP A BY id, B BY id;");
        pig.registerQuery("D = FOREACH C GENERATE group, SUM(B.val), SUM(A.val), "
                + "(SUM(A.val) - SUM(B.val)), (SUM(A.val) + SUM(B.val)), "
                + "(SUM(A.val) * SUM(B.val)), (SUM(A.val) / SUM(B.val)), "
                + "(SUM(A.val) % SUM(B.val)), (SUM(A.val) < 0 ? SUM(A.val) : SUM(B.val));");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                        "('id1',2L,null,null,null,null,null,null,null)",
                        "('id2',2L,10L,8L,12L,20L,5L,0L,2L)" });
        Iterator<Tuple> iter = pig.openIterator("D");
        Util.checkQueryOutputsAfterSort(iter, expectedResults);
    }

}
