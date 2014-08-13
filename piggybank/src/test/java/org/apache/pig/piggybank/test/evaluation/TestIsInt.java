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
import org.apache.pig.piggybank.evaluation.IsInt;
import org.junit.Test;

public class TestIsInt {
    private static final EvalFunc<Boolean> isInt = new IsInt();
    private static Tuple testTuple = TupleFactory.getInstance().newTuple(1);

    @Test
    public void testIsInt() throws IOException {
        testTuple.set(0, "1234");
        assertTrue(isInt.exec(testTuple));

        testTuple.set(0, "1aaannn234");
        assertFalse(isInt.exec(testTuple));

        // This should be true as its within Integer range
        testTuple.set(0, "2147483647");
        assertTrue(isInt.exec(testTuple));

        // This should be false as its not within Integer range
        testTuple.set(0, "2147483648");
        assertFalse(isInt.exec(testTuple));
        
        testTuple.set(0, "-1234");
        assertTrue(isInt.exec(testTuple));
    }
}

