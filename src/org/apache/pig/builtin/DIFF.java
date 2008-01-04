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
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;


/**
 * DIFF compares the fields of a tuple with arity 2. If the fields are DataBags, it
 * will emit any Tuples that are in on of the DataBags but not the other. If the
 * fields are values, it will emit tuples with values that do not match.
 * 
 * @author breed
 *
 */
public class DIFF extends EvalFunc<DataBag> {
    /**
     * Compares a tuple with two fields. Emits any differences.
     * @param input a tuple with exactly two fields.
     * @throws IOException if there are not exactly two fields in a tuple
     */
    @Override
    public void exec(Tuple input, DataBag output) throws IOException {
        if (input.arity() != 2) {
            throw new IOException("DIFF must compare two fields not " + input.arity());
        }
        if (input.getField(0) instanceof DataBag) {
            DataBag field1 = input.getBagField(0);
            DataBag field2 = input.getBagField(1);
            Iterator<Tuple> it1 = field1.iterator();
            checkInBag(field2, it1, output);
            Iterator<Tuple> it2 = field2.iterator();
            checkInBag(field1, it2, output);
        } else {
            DataAtom d1 = input.getAtomField(0);
            DataAtom d2 = input.getAtomField(1);
            if (!d1.equals(d2)) {
                output.add(new Tuple(d1));
                output.add(new Tuple(d2));
            }
        }
    }

    private void checkInBag(DataBag bag, Iterator<Tuple> iterator, DataBag emitTo) throws IOException {
        while(iterator.hasNext()) {
            Tuple t = iterator.next();
            Iterator<Tuple> it2 = bag.iterator();
            boolean found = false;
            while(it2.hasNext()) {
                if (t.equals(it2.next())) {
                    found = true;
                }
            }
            if (!found) {
                emitTo.add(t);
            }
        }
    }
    
    
}
