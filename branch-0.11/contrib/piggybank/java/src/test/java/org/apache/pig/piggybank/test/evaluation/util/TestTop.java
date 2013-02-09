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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.util.Top;
import org.junit.Test;

public class TestTop extends TestCase {

    Top top_ = new Top();
    TupleFactory tupleFactory_ = DefaultTupleFactory.getInstance();
    BagFactory bagFactory_ = DefaultBagFactory.getInstance();
    Tuple inputTuple_ = tupleFactory_.newTuple(3);
    DataBag dBag_ = bagFactory_.newDefaultBag();

    public void setup() throws ExecException {
        // set N = 10 i.e retain top 10 tuples
        inputTuple_.set(0, 10);
        // compare tuples by field number 1
        inputTuple_.set(1, 1);
        // set the data bag containing the tuples
        inputTuple_.set(2, dBag_);

        // generate tuples of the form (group-1, 1), (group-2, 2) ...
        for (long i = 0; i < 100; i++) {
            Tuple nestedTuple = tupleFactory_.newTuple(2);
            nestedTuple.set(0, "group-" + i);
            nestedTuple.set(1, i);
            dBag_.add(nestedTuple);
        }     
    }
    @Test
    public void testTopExec() throws Exception {
        setup();
        DataBag outBag = top_.exec(inputTuple_);
        assertEquals(outBag.size(), 10L);
        checkItemsGT(outBag, 1, 89);
    }

    @Test
    public void testTopAlgebraic() throws IOException {
        setup();
        // two initial results
        Tuple init1 = (new Top.Initial()).exec(inputTuple_);
        Tuple init2 = (new Top.Initial()).exec(inputTuple_);
        // two intermediate results

        DataBag intermedBag = bagFactory_.newDefaultBag();
        intermedBag.add(init1);
        intermedBag.add(init2);
        Tuple intermedInput = tupleFactory_.newTuple(intermedBag);
        Tuple intermedOutput1 = (new Top.Intermed()).exec(intermedInput);
        Tuple intermedOutput2 = (new Top.Intermed()).exec(intermedInput);
        checkItemsGT((DataBag)intermedOutput1.get(2), 1, 94);

        // final result
        DataBag finalInputBag = bagFactory_.newDefaultBag();
        finalInputBag.add(intermedOutput1);
        finalInputBag.add(intermedOutput2);
        Tuple finalInput = tupleFactory_.newTuple(finalInputBag);
        DataBag outBag = (new Top.Final()).exec(finalInput);
        assertEquals(outBag.size(), 10L);
        checkItemsGT(outBag, 1, 96);
    }

    private void checkItemsGT(Iterable<Tuple> tuples, int field, int limit) throws ExecException {
        for (Tuple t : tuples) {
            Long val = (Long) t.get(field);
            assertTrue("Value "+ val + " exceeded the expected limit", val > limit);
        }
    }
}
