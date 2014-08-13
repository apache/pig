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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Test;

public class TestPODistinct {
    DataBag input = BagFactory.getInstance().newDefaultBag();
    Random r = new Random();
    final int MAX_VALUE = 10;
    final int MAX_SAMPLES = 100;

    @Test
    public void testPODistictWithInt() throws ExecException {

        input = BagFactory.getInstance().newDefaultBag();
        TupleFactory tf = TupleFactory.getInstance();
        for (int i = 0; i < MAX_SAMPLES; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextInt(MAX_VALUE));
            input.add(t);
            // System.out.println(t);
        }

        confirmDistinct();
     }

    @Test
    public void testPODistictWithNullValues() throws ExecException {

        input = BagFactory.getInstance().newDefaultBag();
        TupleFactory tf = TupleFactory.getInstance();
        for (int i = 0; i < MAX_SAMPLES; i++) {
            Tuple t = tf.newTuple();
            t.append(null);
            input.add(t);
            // System.out.println(t);
        }

        confirmDistinct();
     }

    @Test
    public void testPODistictWithIntAndNullValues() throws ExecException {

        input = BagFactory.getInstance().newDefaultBag();
        TupleFactory tf = TupleFactory.getInstance();
        for (int i = 0; i < MAX_SAMPLES; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextInt(MAX_VALUE));
            input.add(t);
            t = tf.newTuple();
            t.append(null);
            input.add(t);
            // System.out.println(t);
        }

        confirmDistinct();
     }

    @Test
    public void testPODistictWithIntNullValues() throws ExecException {

        input = BagFactory.getInstance().newDefaultBag();
        TupleFactory tf = TupleFactory.getInstance();
        for (int i = 0; i < MAX_SAMPLES; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextInt(MAX_VALUE));
            t.append(null);
            input.add(t);
            // System.out.println(t);
        }

        confirmDistinct();
     }

    @Test
    public void testPODistictWithNullIntValues() throws ExecException {

        input = BagFactory.getInstance().newDefaultBag();
        TupleFactory tf = TupleFactory.getInstance();
        for (int i = 0; i < MAX_SAMPLES; i++) {
            Tuple t = tf.newTuple();
            t.append(null);
            t.append(r.nextInt(MAX_VALUE));
            input.add(t);
            // System.out.println(t);
        }

        confirmDistinct();
     }

    @Test
    public void testPODistictArityWithNullValues() throws ExecException {

        input = BagFactory.getInstance().newDefaultBag();
        TupleFactory tf = TupleFactory.getInstance();
        for (int i = 0; i < MAX_SAMPLES; i++) {
            Tuple t = tf.newTuple();
            if ( r.nextInt(MAX_VALUE) % 3 == 0 ){
                t.append(null);
            }
            t.append(r.nextInt(MAX_VALUE));
            t.append(r.nextInt(MAX_VALUE));
            input.add(t);
            // System.out.println(t);
        }

        confirmDistinct();
     }

    public void confirmDistinct() throws ExecException {

        PORead read = new PORead(new OperatorKey("", r.nextLong()), input);
        List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
        inputs.add(read);
        PODistinct distinct = new PODistinct(new OperatorKey("", r.nextLong()),
                -1, inputs);
        Map<Tuple, Integer> output = new HashMap<Tuple, Integer>();
        Tuple t = null;
        Result res = distinct.getNextTuple();
        t = (Tuple) res.result;
        while (res.returnStatus != POStatus.STATUS_EOP) {
            if (output.containsKey(t)) {
                int i = output.get(t);
                output.put(t, ++i);
            } else {
                output.put(t, 1);
            }
            res = distinct.getNextTuple();
            t = (Tuple) res.result;
        }
        for (Map.Entry<Tuple, Integer> e : output.entrySet()) {
            int i = e.getValue();
            // System.out.println(e.getKey());
            assertEquals(1, i);
        }
    }

}
