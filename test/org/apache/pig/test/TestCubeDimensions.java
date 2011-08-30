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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.apache.pig.builtin.CubeDimensions;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestCubeDimensions {

    private static TupleFactory TF = TupleFactory.getInstance();

    @Test
    public void testCube() throws IOException {
        Tuple t = TF.newTuple(ImmutableList.of("a", "b", "c"));
        Set<Tuple> expected = ImmutableSet.of(
                TF.newTuple(ImmutableList.of("a", "b", "c")),
                TF.newTuple(ImmutableList.of("a", "b", "ALL")),
                TF.newTuple(ImmutableList.of("a", "ALL", "c")),
                TF.newTuple(ImmutableList.of("ALL", "b", "c")),
                TF.newTuple(ImmutableList.of("ALL", "ALL", "c")),
                TF.newTuple(ImmutableList.of("a", "ALL", "ALL")),
                TF.newTuple(ImmutableList.of("ALL", "ALL", "ALL")),
                TF.newTuple(ImmutableList.of("ALL", "b", "ALL"))
        );

        CubeDimensions cd = new CubeDimensions("ALL");
        DataBag bag = cd.exec(t);
        assertEquals(bag.size(), expected.size());

        for (Tuple tup : bag) {
            assertTrue(expected.contains(tup));
        }
    }
}
