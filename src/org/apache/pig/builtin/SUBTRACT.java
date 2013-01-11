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

import static java.lang.String.format;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * SUBTRACT takes two bags as arguments and returns a new bag composed of tuples of first bag not in the second bag.<br>
 * If null, bag arguments are replaced by empty bags.
 * <p>
 * The implementation assumes that both bags being passed to this function will fit entirely into memory simultaneously.
 * </br>
 * If that is not the case the UDF will still function, but it will be <strong>very</strong> slow.
 */
public class SUBTRACT extends EvalFunc<DataBag> {

    /**
     * Compares the two bag fields from input Tuple and returns a new bag composed of elements of first bag not in the second bag.
     * @param input a tuple with exactly two bag fields.
     * @throws IOException if there are not exactly two fields in a tuple or if they are not {@link DataBag}.
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input.size() != 2) {
            throw new ExecException("SUBTRACT expected two inputs but received " + input.size() + " inputs.");
        }
        DataBag bag1 = toDataBag(input.get(0));
        DataBag bag2 = toDataBag(input.get(1));
        return subtract(bag1, bag2);
    }

    private static String classNameOf(Object o) {
        return o == null ? "null" : o.getClass().getSimpleName();
    }

    private static DataBag toDataBag(Object o) throws ExecException {
        if (o == null) {
            return BagFactory.getInstance().newDefaultBag();
        }
        if (o instanceof DataBag) {
            return (DataBag) o;
        }
        throw new ExecException(format("Expecting input to be DataBag only but was '%s'", classNameOf(o)));
    }

    private static DataBag subtract(DataBag bag1, DataBag bag2) {
        DataBag subtractBag2FromBag1 = BagFactory.getInstance().newDefaultBag();
        // convert each bag to Set, this does make the assumption that the sets will fit in memory.
        Set<Tuple> set1 = toSet(bag1);
        // remove elements of bag2 from set1
        Iterator<Tuple> bag2Iterator = bag2.iterator();
        while (bag2Iterator.hasNext()) {
            set1.remove(bag2Iterator.next());
        }
        // set1 now contains all elements of bag1 not in bag2 => we can build the resulting DataBag.
        for (Tuple tuple : set1) {
            subtractBag2FromBag1.add(tuple);
        }
        return subtractBag2FromBag1;
    }

    private static Set<Tuple> toSet(DataBag bag) {
        Set<Tuple> set = new HashSet<Tuple>();
        Iterator<Tuple> iterator = bag.iterator();
        while (iterator.hasNext()) {
            set.add(iterator.next());
        }
        return set;
    }

}
