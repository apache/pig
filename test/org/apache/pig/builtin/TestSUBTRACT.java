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
package org.apache.pig.builtin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.builtin.SUBTRACT;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class TestSUBTRACT { 

    private static SUBTRACT subtract = new SUBTRACT();

    @Test
    public void testSubtractBags() throws IOException {
        Tuple input = TupleFactory.getInstance().newTuple(2);
        input.set(0, bag(tuple("item1"), tuple("item2"), tuple("item3")));
        input.set(1, bag(tuple("item1"), tuple("item4"), tuple("item5")));
        assertBagContainsOnly(subtract.exec(input), tuple("item2"), tuple("item3"));

        // Note that "item2" which appears several times in first bag, is not in the resulting bag 
        input.set(0, bag(tuple("item1"), tuple("item2"), tuple("item2"), tuple("item3"), tuple("item4")));
        input.set(1, bag(tuple("item1"), tuple("item2"), tuple("item3")));
        assertBagContainsOnly(subtract.exec(input), tuple("item4"));

        input.set(0, bag(tuple("item1"), tuple("item2"), tuple("item3"), tuple("item4")));
        input.set(1, bag(tuple("item2"), tuple("item3"), tuple("item1")));
        assertBagContainsOnly(subtract.exec(input), tuple("item4"));

        input.set(0, bag(tuple("item1"), tuple("item2"), tuple("item3")));
        input.set(1, bag(tuple("item1"), tuple("item2"), tuple("item3"), tuple("item4")));
        assertEquals(0, subtract.exec(input).size());

        input.set(0, bag(tuple("item1"), tuple("item2"), tuple("item3")));
        input.set(1, bag(tuple("item1"), tuple("item2"), tuple("item3")));
        assertEquals(0, subtract.exec(input).size());

        input.set(0, bag());
        input.set(1, bag(tuple("item1"), tuple("item2"), tuple("item3")));
        assertEquals(0, subtract.exec(input).size());

        // null treated like empty bag
        input.set(0, null);
        input.set(1, bag(tuple("item1"), tuple("item2"), tuple("item3")));
        assertEquals(0, subtract.exec(input).size());

        input.set(0, bag(tuple("item1"), tuple("item2"), tuple("item3")));
        input.set(1, bag());
        assertBagContainsOnly(subtract.exec(input), tuple("item1"), tuple("item2"), tuple("item3"));

        // null treated like empty bag
        input.set(0, bag(tuple("item1"), tuple("item2"), tuple("item3")));
        input.set(1, null);
        assertBagContainsOnly(subtract.exec(input), tuple("item1"), tuple("item2"), tuple("item3"));
    }

    @Test
    public void testShouldThrowAnIOExceptionAsInputDoesNotContainDataBagOnly() {
        Tuple input = TupleFactory.getInstance().newTuple(2);
        try {
            input.set(0, "2012/05/05");
            input.set(1, bag());
            subtract.exec(input);
            fail("IOException Expected");
        } catch (IOException e) {
            assertEquals("Expecting input to be DataBag only but was 'String'", e.getMessage());
        }
    }

    @Test
    public void testShouldThrowAnIOExceptionAsInputDoesNotHaveTwoElements() {
        Tuple input = TupleFactory.getInstance().newTuple(1);
        try {
            input.set(0, bag());
            subtract.exec(input);
            fail("IOException Expected");
        } catch (IOException e) {
            assertEquals("SUBTRACT expected two inputs but received 1 inputs.", e.getMessage());
        }

        input = TupleFactory.getInstance().newTuple(3);
        try {
            input.set(0, bag());
            input.set(1, bag());
            input.set(2, bag());
            subtract.exec(input);
            fail("IOException Expected");
        } catch (IOException e) {
            assertEquals("SUBTRACT expected two inputs but received 3 inputs.", e.getMessage());
        }
    }

    // utility assertion method

    /**
     * Checks that given {@link DataBag} contains only the given {@link Tuple}s.
     * @param bag the {@link DataBag} to check
     * @param tuples the {@link Tuple}s expected in given {@link DataBag}
     */
    private static void assertBagContainsOnly(DataBag bag, Tuple... tuples) {
        Iterator<Tuple> bagTuplesIterator = bag.iterator();
        List<Tuple> bagTuplesList = new ArrayList<Tuple>();
        while (bagTuplesIterator.hasNext()) {
            bagTuplesList.add(bagTuplesIterator.next());
        }
        assertEquals("Wrong bag size", tuples.length, bagTuplesList.size());
        // bag has same size as given tuples, let's see if each tuple is in bag.
        for (Tuple tuple : tuples) {
            assertTrue("Tuple not found in bag " + tuple, bagTuplesList.contains(tuple));
        }
    }

    // utility methods to build bags, tuples and list

    private static <E> List<E> list(E... elements) {
        if (elements == null) {
            return new ArrayList<E>();
        }
        List<E> list = new ArrayList<E>();
        for (E element : elements) {
            list.add(element);
        }
        return list;
    }

    private static Tuple tuple(Object... elements) {
        if (elements == null) {
            return TupleFactory.getInstance().newTuple();
        }
        return TupleFactory.getInstance().newTuple(list(elements));
    }

    private static DataBag bag(Tuple... tuples) {
        if (tuples == null) {
            return BagFactory.getInstance().newDefaultBag();
        }
        return BagFactory.getInstance().newDefaultBag(list(tuples));
    }

}
