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

import junit.framework.TestCase;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestTupleBagInterface extends TestCase {

    private static TupleFactory TUPLEFACTORY = TupleFactory.getInstance();

    private static BagFactory BAGFACTORY = BagFactory.getInstance();

    @Test
    public void testJIRA_1166() throws ExecException {

        // Tuple set method
        Tuple tuple = TUPLEFACTORY.newTuple(3).set(0, 1).set(1, 1.2).set(2,
                "hello");
        assertEquals(1, tuple.get(0));
        assertEquals(1.2, tuple.get(1));
        assertEquals("hello", tuple.get(2));

        // Tuple append method
        tuple = TUPLEFACTORY.newTuple().append(1).append(1.2).append("hello");
        assertEquals(1, tuple.get(0));
        assertEquals(1.2, tuple.get(1));
        assertEquals("hello", tuple.get(2));

        // DataBag add method
        DataBag bag = BAGFACTORY.newDefaultBag().add(
                TUPLEFACTORY.newTuple().append(1)).add(
                TUPLEFACTORY.newTuple().append(1.2)).add(
                TUPLEFACTORY.newTuple().append("hello"));
        assertEquals(3, bag.size());
        Iterator<Tuple> iter=bag.iterator();
        assertEquals(1, iter.next().get(0));
        assertEquals(1.2, iter.next().get(0));
        assertEquals("hello", iter.next().get(0));
        
        // DataBag addAll method
        DataBag bag_2 = BAGFACTORY.newDefaultBag().addAll(bag);
        assertEquals(3, bag_2.size());
        iter=bag_2.iterator();
        assertEquals(1, iter.next().get(0));
        assertEquals(1.2, iter.next().get(0));
        assertEquals("hello", iter.next().get(0));
    }
}
