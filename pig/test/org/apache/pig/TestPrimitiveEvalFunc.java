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
package org.apache.pig;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestPrimitiveEvalFunc {

    private static class StringStringFunc extends PrimitiveEvalFunc<String, String> {
        @Override
        public String exec(String input) throws IOException {
            return "foo";
        }
    };

    private static class StringIntegerFunc extends PrimitiveEvalFunc<String, Integer> {
        @Override
        public Integer exec(String input) throws IOException {
            return 1;
        }
    };

    private static class IntegerFloatFunc extends PrimitiveEvalFunc<Integer, Float> {
        @Override
        public Float exec(Integer i) throws IOException {
            return 1.0f;
        }
    };

    @Test
    public final void testGetOutputTypes() throws IOException {
        StringStringFunc udf1 = new StringStringFunc();
        StringIntegerFunc udf2 = new StringIntegerFunc();
        IntegerFloatFunc udf3 = new IntegerFloatFunc();

        assertEquals(String.class, udf1.getOutputTypeClass());
        assertEquals(String.class, udf1.getInputTypeClass());

        assertEquals(Integer.class, udf2.getOutputTypeClass());
        assertEquals(String.class, udf2.getInputTypeClass());

        assertEquals(Float.class, udf3.getOutputTypeClass());
        assertEquals(Integer.class, udf3.getInputTypeClass());
    }
}
