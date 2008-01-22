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

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


/**
 * DIFF compares the fields of a tuple with arity 2. If the fields are DataBags, it
 * will emit any Tuples that are in on of the DataBags but not the other. If the
 * fields are values, it will emit tuples with values that do not match.
 * 
 * @author breed
 *
 */
public class DIFF extends EvalFunc<DataBag> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    /**
     * Compares a tuple with two fields. Emits any differences.
     * @param input a tuple with exactly two fields.
     * @throws IOException if there are not exactly two fields in a tuple
     */
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input.size() != 2) {
            throw new IOException("DIFF must compare two fields not " +
                input.size());
        }
        DataBag output = mBagFactory.newDefaultBag();
        Object o1 = input.get(0);
        if (o1 instanceof DataBag) {
            DataBag bag1 = (DataBag)o1;
            DataBag bag2 = (DataBag)input.get(1);
            computeDiff(bag1, bag2, output);
        } else {
            Object d1 = input.get(0);
            Object d2 = input.get(1);
            if (!d1.equals(d2)) {
                output.add(mTupleFactory.newTuple(d1));
                output.add(mTupleFactory.newTuple(d2));
            }
        }
        return output;
    }

    private void computeDiff(
            DataBag bag1,
            DataBag bag2,
            DataBag emitTo) throws IOException {
        // Create two distinct versions of the bag.  This will speed up
        // comparison, and provide us a sorted order so we don't have to do
        // an n^2 lookup.
        DataBag d1 = mBagFactory.newDistinctBag();
        DataBag d2 = mBagFactory.newDistinctBag();
        Iterator<Tuple> i1 = d1.iterator();
        Iterator<Tuple> i2 = d2.iterator();
        while (i1.hasNext()) d1.add(i1.next());
        while (i2.hasNext()) d2.add(i2.next());

        i1 = d1.iterator();
        i2 = d2.iterator();

        Tuple t1 = i1.next();
        Tuple t2 = i2.next();

        while (i1.hasNext() && i2.hasNext()) {
            int c = t1.compareTo(t2);

            if (c < 0) {
                // place t1 in the result bag and advance i1
                emitTo.add(t1);
                t1 = i1.next();
            } else if (c > 0) {
                // place t2 in the result bag and advance i2
                emitTo.add(t2);
                t2 = i2.next();
            } else if (c == 0) {
                // put neither in the result bag, advance both iterators
                t1 = i1.next();
                t2 = i2.next();
            }
        }

        // One ran out, put all the rest of the other (if there are any) in
        // the result bag.
        while (i1.hasNext()) {
            emitTo.add(i1.next());
        }
        while (i2.hasNext()) {
            emitTo.add(i2.next());
        }
    }
    
    
}
