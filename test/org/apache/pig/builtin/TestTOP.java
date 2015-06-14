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

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.Tuple;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTOP {
    private static TupleFactory tupleFactory_ = TupleFactory.getInstance();
    private static BagFactory bagFactory_ = BagFactory.getInstance();
    private static Tuple inputTuple_ = null;

    @BeforeClass
    public static void setup() throws ExecException {
        inputTuple_ = fillTuple(0, 100);
    }
    
    public static Tuple fillTuple(int start, int stop) throws ExecException {
        Tuple tuple = tupleFactory_.newTuple(3);
        
        DataBag dBag = bagFactory_.newDefaultBag();

        // set N = 10 i.e retain top 10 tuples
        tuple.set(0, 10);
        // compare tuples by field number 1
        tuple.set(1, 1);
        // set the data bag containing the tuples
        tuple.set(2, dBag);

        // generate tuples of the form (group-1, 1), (group-2, 2) ...
        for (long i = start; i < stop; i++) {
            Tuple nestedTuple = tupleFactory_.newTuple(2);
            nestedTuple.set(0, "group-" + i);
            nestedTuple.set(1, i);
            dBag.add(nestedTuple);
        }
        
        return tuple;
    }

    @Test
    public void testTOPExec() throws Exception {
        TOP top = new TOP("DESC");
        DataBag outBag = top.exec(inputTuple_);
        assertEquals(outBag.size(), 10L);
        checkItemsGT(outBag, 1, 89);

        top = new TOP("ASC");
        outBag = top.exec(inputTuple_);
        assertEquals(outBag.size(), 10L);
        checkItemsLT(outBag, 1, 10);
    }

    @Test
    public void testTOPAccumulator() throws Exception {
        Tuple firstTuple = fillTuple(0, 50);
        Tuple secondTuple = fillTuple(50, 100);
        
        TOP top = new TOP("DESC");
        top.accumulate(firstTuple);
        top.accumulate(secondTuple);
        DataBag outBag = top.getValue();
        assertEquals(outBag.size(), 10L);
        checkItemsGT(outBag, 1, 89);

        top = new TOP("ASC");
        top.accumulate(firstTuple);
        top.accumulate(secondTuple);
        outBag = top.getValue();
        assertEquals(outBag.size(), 10L);
        checkItemsLT(outBag, 1, 10);
    }

    @Test
    public void testTopAlgebraic() throws IOException {
        // initial results
        Tuple init = (new TOP.Initial("DESC")).exec(inputTuple_);

        // intermediate results
        DataBag intermedBag = bagFactory_.newDefaultBag();
        Tuple intermedInput = tupleFactory_.newTuple(intermedBag);
        intermedBag.add(init);
        Tuple intermedOutput = (new TOP.Intermed("DESC")).exec(intermedInput);
        checkItemsGT((DataBag)intermedOutput.get(2), 1, 89);

        // final result
        DataBag finalInputBag = bagFactory_.newDefaultBag();
        Tuple finalInput = tupleFactory_.newTuple(finalInputBag);
        finalInputBag.add(intermedOutput);
        DataBag outBag = (new TOP.Final("DESC")).exec(finalInput);
        assertEquals(outBag.size(), 10L);
        checkItemsGT(outBag, 1, 89);

        // initial results
        init = (new TOP.Initial("ASC")).exec(inputTuple_);

        // intermediate results
        intermedBag = bagFactory_.newDefaultBag();
        intermedInput = tupleFactory_.newTuple(intermedBag);
        intermedBag.add(init);
        intermedOutput = (new TOP.Intermed("ASC")).exec(intermedInput);
        checkItemsLT((DataBag)intermedOutput.get(2), 1, 10);

        // final result
        finalInputBag = bagFactory_.newDefaultBag();
        finalInput = tupleFactory_.newTuple(finalInputBag);
        finalInputBag.add(intermedOutput);
        outBag = (new TOP.Final("ASC")).exec(finalInput);
        assertEquals(outBag.size(), 10L);
        checkItemsLT(outBag, 1, 10);
    }

    private void checkItemsGT(Iterable<Tuple> tuples, int field, int minLimit) throws ExecException {
        for (Tuple t : tuples) {
            Long val = (Long) t.get(field);
            assertTrue("Value "+ val + " should be greater than the expected limit " + minLimit, val > minLimit);
        }
    }

    private void checkItemsLT(Iterable<Tuple> tuples, int field, int maxLimit) throws ExecException {
        for (Tuple t : tuples) {
            Long val = (Long) t.get(field);
            assertTrue("Value "+ val + " should be smaller than the expected limit " + maxLimit, val < maxLimit);
        }
    }
}
