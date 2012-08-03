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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.InvokeForDouble;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

/** 
 * Tests {@link Invoker} speed comparing to raw calculation.
 */
public class TestInvokerSpeed {

    private final TupleFactory tf_ = TupleFactory.getInstance();

    @Test
    public void testSpeed() throws IOException, SecurityException, ClassNotFoundException, NoSuchMethodException {
        EvalFunc<Double> log = new Log();
        Tuple tup = tf_.newTuple(1);
        long start = System.currentTimeMillis();
        for (int i=0; i < 1000000; i++) {
            tup.set(0, (double) i);
            log.exec(tup);
        }
        long staticSpeed = (System.currentTimeMillis()-start);
        start = System.currentTimeMillis();
        log = new InvokeForDouble("java.lang.Math.log", "Double", "static");
        for (int i=0; i < 1000000; i++) {
            tup.set(0, (double) i);
            log.exec(tup);
        }
        long dynamicSpeed = System.currentTimeMillis()-start;
        System.err.println("Dynamic to static ratio: "+((float) dynamicSpeed)/staticSpeed);
        assertTrue( ((float) dynamicSpeed)/staticSpeed < 5);
    }
    
    private class Log extends EvalFunc<Double> {

        @Override
        public Double exec(Tuple input) throws IOException {
            Double d = (Double) input.get(0);
            return Math.log(d);
        }
        
    }
}
