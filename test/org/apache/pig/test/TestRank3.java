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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestRank3 {
    private static PigServer pigServer;
    private static TupleFactory tf = TupleFactory.getInstance();
    private Data data;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        try {
            pigServer = new PigServer("local");

            data = resetData(pigServer);
            data.set("empty");
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
                tf.newTuple(ImmutableList.of(1L,21L,5L,7L,1L,1L,0L,8L,8L)),
                tf.newTuple(ImmutableList.of(2L,26L,2L,3L,2L,5L,1L,9L,10L)),
                tf.newTuple(ImmutableList.of(3L,30L,24L,21L,2L,3L,1L,3L,10L)),
                tf.newTuple(ImmutableList.of(4L,6L,10L,8L,3L,4L,1L,7L,2L)),
                tf.newTuple(ImmutableList.of(5L,8L,28L,25L,3L,2L,1L,0L,2L)),
                tf.newTuple(ImmutableList.of(6L,28L,11L,12L,4L,6L,2L,7L,10L)),
                tf.newTuple(ImmutableList.of(7L,9L,26L,22L,5L,7L,3L,2L,3L)),
                tf.newTuple(ImmutableList.of(8L,5L,6L,5L,6L,8L,3L,8L,1L)),
                tf.newTuple(ImmutableList.of(9L,29L,16L,15L,7L,9L,4L,6L,10L)),
                tf.newTuple(ImmutableList.of(10L,18L,12L,10L,8L,11L,5L,7L,6L)),
                tf.newTuple(ImmutableList.of(11L,14L,17L,14L,9L,10L,5L,6L,5L)),
                tf.newTuple(ImmutableList.of(12L,6L,12L,8L,10L,11L,5L,7L,2L)),
                tf.newTuple(ImmutableList.of(13L,2L,17L,13L,11L,10L,5L,6L,0L)),
                tf.newTuple(ImmutableList.of(14L,26L,3L,3L,12L,14L,6L,9L,10L)),
                tf.newTuple(ImmutableList.of(15L,15L,20L,18L,13L,13L,6L,4L,5L)),
                tf.newTuple(ImmutableList.of(16L,3L,29L,24L,14L,12L,6L,0L,0L)),
                tf.newTuple(ImmutableList.of(17L,23L,21L,19L,15L,16L,7L,4L,8L)),
                tf.newTuple(ImmutableList.of(18L,19L,19L,16L,16L,17L,7L,5L,6L)),
                tf.newTuple(ImmutableList.of(19L,20L,30L,26L,16L,15L,7L,0L,6L)),
                tf.newTuple(ImmutableList.of(20L,12L,21L,17L,17L,16L,7L,4L,4L)),
                tf.newTuple(ImmutableList.of(21L,4L,1L,1L,18L,19L,7L,10L,1L)),
                tf.newTuple(ImmutableList.of(22L,1L,7L,4L,19L,18L,7L,8L,0L)),
                tf.newTuple(ImmutableList.of(23L,24L,14L,11L,20L,21L,8L,7L,9L)),
                tf.newTuple(ImmutableList.of(24L,16L,25L,20L,21L,20L,8L,3L,5L)),
                tf.newTuple(ImmutableList.of(25L,25L,27L,23L,22L,22L,9L,1L,9L)),
                tf.newTuple(ImmutableList.of(26L,21L,8L,7L,23L,25L,9L,8L,8L)),
                tf.newTuple(ImmutableList.of(27L,17L,4L,2L,24L,26L,9L,9L,6L)),
                tf.newTuple(ImmutableList.of(28L,10L,8L,6L,25L,25L,9L,8L,4L)),
                tf.newTuple(ImmutableList.of(29L,11L,15L,9L,25L,24L,9L,7L,4L)),
                tf.newTuple(ImmutableList.of(30L,12L,23L,17L,25L,23L,9L,4L,4L))
        );

        verifyExpected(data.get("result"), expected);
    }

    // See PIG-3726
    @Test
    public void testRankEmptyRelation() throws Exception {
      String query = "DATA = LOAD 'empty' USING mock.Storage();"
        + "A = rank DATA;"
        + "store A into 'empty_result' using mock.Storage();";

      Util.registerMultiLineQuery(pigServer, query);

      Set<Tuple> expected = ImmutableSet.of();
      verifyExpected(data.get("empty_result"), expected);
    }

    public void verifyExpected(List<Tuple> out, Set<Tuple> expected) {
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }
    }

}
