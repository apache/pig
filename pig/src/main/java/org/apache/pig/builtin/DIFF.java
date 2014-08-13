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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


/**
 * DIFF takes two bags as arguments and compares them.   Any tuples
 * that are in one bag but not the other are returned.  If the
 * fields are not bags then they will be returned if they do not match, or 
 * an empty bag will be returned if the two records match.
 * <p>
 * The implementation assumes that both bags being passed to this function will
 * fit entirely into memory simultaneously.  If that is not the case the UDF
 * will still function, but it will be <strong>very</strong> slow.
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
            int errCode = 2107;
            String msg = "DIFF expected two inputs but received " + input.size() + " inputs.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        try {
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
        } catch (ExecException ee) {
            throw ee;
        }
    }

    private void computeDiff(
            DataBag bag1,
            DataBag bag2,
            DataBag emitTo) {
        // Build two hash tables and probe with first one, then the other.
        // This does make the assumption that the distinct set of keys from
        // each bag will fit in memory.
        Set<Tuple> s1 = new HashSet<Tuple>();
        Iterator<Tuple> i1 = bag1.iterator();
        while (i1.hasNext()) s1.add(i1.next());

        Set<Tuple> s2 = new HashSet<Tuple>();
        Iterator<Tuple> i2 = bag2.iterator();
        while (i2.hasNext()) s2.add(i2.next());

        for (Tuple t : s1) if (!s2.contains(t)) emitTo.add(t);
        for (Tuple t : s2) if (!s1.contains(t)) emitTo.add(t);

    }
    
    
}
