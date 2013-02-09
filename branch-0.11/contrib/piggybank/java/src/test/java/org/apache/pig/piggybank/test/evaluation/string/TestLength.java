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
package org.apache.pig.piggybank.test.evaluation.string;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.string.LENGTH;
import org.junit.Test;

public class TestLength {

    private static final EvalFunc<Integer> len_ = new LENGTH();
    private static Tuple testTuple_ = TupleFactory.getInstance().newTuple(1);
    
    @Test
    public void testLength() throws IOException {
        testTuple_.set(0,"foo");
        assertEquals("regular length should match", (Integer) 3, len_.exec(testTuple_));
        
        testTuple_.set(0, null);
        assertNull("length of null is null", len_.exec(testTuple_));
        
        testTuple_.set(0, "");
        assertEquals("empty string has 0 length", (Integer) 0, len_.exec(testTuple_));
    }
}
