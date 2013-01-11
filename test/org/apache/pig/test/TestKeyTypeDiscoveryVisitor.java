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

import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 * This testcases here test that the key type of the map key
 * is correctly determines for use when the key is null. In
 * particular it tests KeyTypeDiscoveryVisitor
 */
public class TestKeyTypeDiscoveryVisitor {

    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();

    @Before
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties()); //TODO this doesn't need to be M/R mode
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testNullJoin() throws Exception {
        String[] inputData = new String[] { "\t7\t8", "\t8\t9", "1\t20\t30", "1\t20\t40" };
        Util.createInputFile(cluster, "a.txt", inputData);

        inputData = new String[] { "7\t2", "1\t5", "1\t10" };
        Util.createInputFile(cluster, "b.txt", inputData);

        String script = "a = load 'a.txt' as (x:int, y:int, z:int);" +
                "b = load 'b.txt' as (x:int, y:int);" +
                "b_group = group b by x;" +
                "b_sum = foreach b_group generate flatten(group) as x, SUM(b.y) as clicks;" +
                // b_sum will have {(1, 15L)}
                "a_group = group a by (x, y);" +
                "a_aggs = foreach a_group generate flatten(group) as (x, y), SUM(a.z) as zs;" +
                // a_aggs will have {(<null>, 7, 8L), (<null>, 8, 9L), (1, 20, 70L)
                // The join in the next statement is on "x" which is the first column
                // The nulls in the first two records of a_aggs will test whether
                // KeyTypeDiscoveryVisitor had set a valid keyType (in this case INTEGER)
                // The null records will get discarded by the join and hence the join
                // output would be {(1, 15L, 1, 20, 70L)}
                "join_a_b = join b_sum by x, a_aggs by x;";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("join_a_b");
        Object[] results = new Object[] { 1, 15L, 1, 20, 70L};
        Tuple output = it.next();
        assertFalse(it.hasNext());
        assertEquals(results.length, output.size());
        for (int i = 0; i < output.size(); i++) {
            assertEquals(results[i], output.get(i));
        }

    }
}
