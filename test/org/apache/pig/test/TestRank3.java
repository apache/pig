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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestRank3 extends TestCase {

    private final Log log = LogFactory.getLog(getClass());
    private static PigServer pigServer;
    private static TupleFactory tf = TupleFactory.getInstance();
    private Data data;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
    }

    @Override
    @Before
    public void setUp() throws Exception {

        try {
            pigServer = new PigServer("local");

            data = resetData(pigServer);
            data.set(
                    "testcascade",
                    tuple(3,2,3),
                    tuple(2,7,10),
                    tuple(1,0,2),
                    tuple(5,6,0),
                    tuple(7,4,8),
                    tuple(9,8,4),
                    tuple(1,9,10),
                    tuple(7,4,4),
                    tuple(5,7,6),
                    tuple(4,6,10),
                    tuple(5,7,2),
                    tuple(6,4,5),
                    tuple(6,0,0),
                    tuple(1,7,2),
                    tuple(7,5,6),
                    tuple(9,1,9),
                    tuple(9,8,8),
                    tuple(9,9,6),
                    tuple(5,6,5),
                    tuple(3,8,1),
                    tuple(7,0,6),
                    tuple(0,8,8),
                    tuple(6,9,10),
                    tuple(7,10,1),
                    tuple(7,8,0),
                    tuple(8,7,9),
                    tuple(8,3,5),
                    tuple(1,3,10),
                    tuple(9,7,4),
                    tuple(9,4,4));

        } catch (ExecException e) {
            IOException ioe = new IOException("Failed to create Pig Server");
            ioe.initCause(e);
            throw ioe;
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
    }

    @Test
    public void testRankCascade() throws IOException {
        String query = "R1 = LOAD 'testcascade' USING mock.Storage() AS (a:long,b:long,c:long);"
            + "R2 = rank R1 by a ASC,b ASC DENSE;"
            + "R3 = rank R2 by a ASC,c DESC DENSE;"
            + "R4 = rank R3 by b DESC,c ASC DENSE;"
            + "R5 = rank R4 by b DESC,a ASC;"
            + "R6 = rank R5 by c ASC,b DESC;"
            + "R7 = order R6 by a ASC,c DESC,b DESC;"
            + "R8 = rank R7;"
            + "store R8 into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long)1,(long)21,(long)5,(long)7,(long)1,(long)1,(long)0,(long)8,(long)8)),
                tf.newTuple(ImmutableList.of((long)2,(long)26,(long)2,(long)3,(long)2,(long)5,(long)1,(long)9,(long)10)),
                tf.newTuple(ImmutableList.of((long)3,(long)30,(long)24,(long)21,(long)2,(long)3,(long)1,(long)3,(long)10)),
                tf.newTuple(ImmutableList.of((long)4,(long)6,(long)10,(long)8,(long)3,(long)4,(long)1,(long)7,(long)2)),
                tf.newTuple(ImmutableList.of((long)5,(long)8,(long)28,(long)25,(long)3,(long)2,(long)1,(long)0,(long)2)),
                tf.newTuple(ImmutableList.of((long)6,(long)28,(long)11,(long)12,(long)4,(long)6,(long)2,(long)7,(long)10)),
                tf.newTuple(ImmutableList.of((long)7,(long)9,(long)26,(long)22,(long)5,(long)7,(long)3,(long)2,(long)3)),
                tf.newTuple(ImmutableList.of((long)8,(long)5,(long)6,(long)5,(long)6,(long)8,(long)3,(long)8,(long)1)),
                tf.newTuple(ImmutableList.of((long)9,(long)29,(long)16,(long)15,(long)7,(long)9,(long)4,(long)6,(long)10)),
                tf.newTuple(ImmutableList.of((long)10,(long)18,(long)12,(long)10,(long)8,(long)11,(long)5,(long)7,(long)6)),
                tf.newTuple(ImmutableList.of((long)11,(long)14,(long)17,(long)14,(long)9,(long)10,(long)5,(long)6,(long)5)),
                tf.newTuple(ImmutableList.of((long)12,(long)6,(long)12,(long)8,(long)10,(long)11,(long)5,(long)7,(long)2)),
                tf.newTuple(ImmutableList.of((long)13,(long)2,(long)17,(long)13,(long)11,(long)10,(long)5,(long)6,(long)0)),
                tf.newTuple(ImmutableList.of((long)14,(long)26,(long)3,(long)3,(long)12,(long)14,(long)6,(long)9,(long)10)),
                tf.newTuple(ImmutableList.of((long)15,(long)15,(long)20,(long)18,(long)13,(long)13,(long)6,(long)4,(long)5)),
                tf.newTuple(ImmutableList.of((long)16,(long)3,(long)29,(long)24,(long)14,(long)12,(long)6,(long)0,(long)0)),
                tf.newTuple(ImmutableList.of((long)17,(long)23,(long)21,(long)19,(long)15,(long)16,(long)7,(long)4,(long)8)),
                tf.newTuple(ImmutableList.of((long)18,(long)19,(long)19,(long)16,(long)16,(long)17,(long)7,(long)5,(long)6)),
                tf.newTuple(ImmutableList.of((long)19,(long)20,(long)30,(long)26,(long)16,(long)15,(long)7,(long)0,(long)6)),
                tf.newTuple(ImmutableList.of((long)20,(long)12,(long)21,(long)17,(long)17,(long)16,(long)7,(long)4,(long)4)),
                tf.newTuple(ImmutableList.of((long)21,(long)4,(long)1,(long)1,(long)18,(long)19,(long)7,(long)10,(long)1)),
                tf.newTuple(ImmutableList.of((long)22,(long)1,(long)7,(long)4,(long)19,(long)18,(long)7,(long)8,(long)0)),
                tf.newTuple(ImmutableList.of((long)23,(long)24,(long)14,(long)11,(long)20,(long)21,(long)8,(long)7,(long)9)),
                tf.newTuple(ImmutableList.of((long)24,(long)16,(long)25,(long)20,(long)21,(long)20,(long)8,(long)3,(long)5)),
                tf.newTuple(ImmutableList.of((long)25,(long)25,(long)27,(long)23,(long)22,(long)22,(long)9,(long)1,(long)9)),
                tf.newTuple(ImmutableList.of((long)26,(long)21,(long)8,(long)7,(long)23,(long)25,(long)9,(long)8,(long)8)),
                tf.newTuple(ImmutableList.of((long)27,(long)17,(long)4,(long)2,(long)24,(long)26,(long)9,(long)9,(long)6)),
                tf.newTuple(ImmutableList.of((long)28,(long)10,(long)8,(long)6,(long)25,(long)25,(long)9,(long)8,(long)4)),
                tf.newTuple(ImmutableList.of((long)29,(long)11,(long)15,(long)9,(long)25,(long)24,(long)9,(long)7,(long)4)),
                tf.newTuple(ImmutableList.of((long)30,(long)12,(long)23,(long)17,(long)25,(long)23,(long)9,(long)4,(long)4))
        );

        verifyExpected(data.get("result"), expected);
    }

    public void verifyExpected(List<Tuple> out, Set<Tuple> expected) {

        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }
    }

}
