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
package org.apache.pig.piggybank.test.evaluation.util;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.util.ToBag;
import org.apache.pig.piggybank.evaluation.util.ToTuple;
import org.junit.Test;

public class TestToBagToTuple {
    @Test
    public void toBag() throws Exception{
        ToBag tb = new ToBag();

        Tuple input = TupleFactory.getInstance().newTuple();
        for (int i = 0; i < 100; ++i) {
            input.append(i);
        }

        Set<Integer> s = new HashSet<Integer>();
        DataBag db = tb.exec(input);
        for (Tuple t : db) {
            s.add((Integer) t.get(0));
        }

        // finally check the bag had everything we put in the tuple.
        Assert.assertEquals(100, s.size());
        for (int i = 0; i < 100; ++i) {
            Assert.assertTrue(s.contains(i));
        }
    }

    @Test
    public void toTuple() throws Exception{
        ToTuple tb = new ToTuple();

        Tuple input = TupleFactory.getInstance().newTuple();
        for (int i = 0; i < 100; ++i) {
            input.append(i);
        }

        Tuple output = tb.exec(input);
        Assert.assertFalse(input == output);
        Assert.assertEquals(input, output);
    }
}
