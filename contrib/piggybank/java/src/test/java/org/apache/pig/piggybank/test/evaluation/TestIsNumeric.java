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

package org.apache.pig.piggybank.test.evaluation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.IsNumeric;
import org.junit.Test;

public class TestIsNumeric {
    private static final EvalFunc<Boolean> isNumeric = new IsNumeric();
    private static Tuple testTuple = TupleFactory.getInstance().newTuple(1);

    @Test
    public void testNumeric() throws IOException {
        testTuple.set(0, "1234");
        assertTrue(isNumeric.exec(testTuple));

        testTuple.set(0, "1aaannn234");
        assertFalse(isNumeric.exec(testTuple));

        testTuple.set(0, "-1234");
        assertTrue(isNumeric.exec(testTuple));

        testTuple.set(0, "-");
        assertFalse(isNumeric.exec(testTuple));

        testTuple.set(0, "");
        assertFalse(isNumeric.exec(testTuple));

        testTuple.set(0, "1");
        assertTrue(isNumeric.exec(testTuple));

        testTuple.set(0, null);
        assertFalse(isNumeric.exec(testTuple));

    }

    @Test
    // Test for floating points
    public void testNumericDecimal() throws IOException {
        testTuple.set(0, "12.8493");
        assertTrue(isNumeric.exec(testTuple));
        
        testTuple.set(0, "12.");
        assertFalse(isNumeric.exec(testTuple));
        
        testTuple.set(0, "0.0");
        assertTrue(isNumeric.exec(testTuple));
        
        //Not a valid floating point number
        testTuple.set(0, "12.84.93");
        assertFalse(isNumeric.exec(testTuple));
        
        //Can not have more than 1 "."
        testTuple.set(0, "0..12");
        assertFalse(isNumeric.exec(testTuple));
        
        testTuple.set(0, "-1.1213213123");
        assertTrue(isNumeric.exec(testTuple));
    }
}
