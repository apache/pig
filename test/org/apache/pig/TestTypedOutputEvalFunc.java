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

import org.apache.pig.data.Tuple;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestTypedOutputEvalFunc {

    private static class StringFunc extends TypedOutputEvalFunc<String> {
        @Override
        public String exec(Tuple input) throws IOException {
            return "foo";
        }
    };

    private static class IntegerFunc extends TypedOutputEvalFunc<Integer> {
        @Override
        public Integer exec(Tuple input) throws IOException {
            return 1;
        }
    };

    private static class FloatFunc extends TypedOutputEvalFunc<Float> {
        @Override
        public Float exec(Tuple input) throws IOException {
            return 1.0f;
        }
    };

    private static final TypedOutputEvalFunc<String> stringFunc = new TypedOutputEvalFunc<String>() {
        @Override
        public String exec(Tuple input) throws IOException {
            return "foo";
        }
    };

    private static final TypedOutputEvalFunc<Integer> intFunc = new TypedOutputEvalFunc<Integer>() {
        @Override
        public Integer exec(Tuple input) throws IOException {
            return 1;
        }
    };

    private static final TypedOutputEvalFunc<Float> floatFunc = new TypedOutputEvalFunc<Float>() {
        @Override
        public Float exec(Tuple input) throws IOException {
            return 1.0f;
        }
    };

    @Test
    public final void testGetOutputTypes() throws IOException {
        // Verify that type inference works for declared as well as anonymous classes.
        assertEquals(String.class, stringFunc.getOutputTypeClass());
        assertEquals(Integer.class, intFunc.getOutputTypeClass());
        assertEquals(Float.class, floatFunc.getOutputTypeClass());

        assertEquals(String.class, new StringFunc().getOutputTypeClass());
        assertEquals(Integer.class, new IntegerFunc().getOutputTypeClass());
        assertEquals(Float.class, new FloatFunc().getOutputTypeClass());
    }
}
