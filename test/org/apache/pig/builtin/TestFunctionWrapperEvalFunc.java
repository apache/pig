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

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestFunctionWrapperEvalFunc {

    public static class StringStringFunction implements org.apache.pig.Function<String, String> {
        @Override
        public String apply(String s) {
            return "foo";
        }
    };

    public static class StringIntegerFunction implements org.apache.pig.Function<String, Integer> {
        @Override
        public Integer apply(String s) {
            return new Integer(s);
        }
    };

    public static class IntegerFloatFunction implements com.google.common.base.Function<Integer, Float> {
        @Override
        public Float apply(Integer integer) {
            return new Float(integer);
        }
    };

    public static class ExceptionalIntegerFloatFunction
        implements org.apache.pig.ExceptionalFunction<Integer, Float, MockException> {
        @Override
        public Float apply(Integer integer) throws MockException {
            if (integer > 10) {
                throw new MockException();
            }
            return new Float(integer);
        }
    };

    public static class MockException extends Exception {}

    @Test
    public final void testStringToString() throws IOException, ClassNotFoundException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        FunctionWrapperEvalFunc udf = new FunctionWrapperEvalFunc(StringStringFunction.class.getName());

        assertEquals(String.class, udf.getInputTypeClass());
        assertEquals(String.class, udf.getOutputTypeClass());
        assertEquals("foo", udf.exec("xyz"));
    }

    @Test
    public final void testStringToInteger() throws IOException, ClassNotFoundException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        FunctionWrapperEvalFunc udf = new FunctionWrapperEvalFunc(StringIntegerFunction.class.getName());

        assertEquals(String.class, udf.getInputTypeClass());
        assertEquals(Integer.class, udf.getOutputTypeClass());
        assertEquals(4, udf.exec("4"));
    }

    @Test
    public final void testStringToIntegerPreinitialized() throws IOException, ClassNotFoundException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        org.apache.pig.ExceptionalFunction function = new StringIntegerFunction();
        FunctionWrapperEvalFunc udf = new FunctionWrapperEvalFunc(function);

        assertEquals(String.class, udf.getInputTypeClass());
        assertEquals(Integer.class, udf.getOutputTypeClass());
        assertEquals(4, udf.exec("4"));
    }

    @Test
    public final void testIntegerToFloat() throws IOException, ClassNotFoundException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        FunctionWrapperEvalFunc udf = new FunctionWrapperEvalFunc(IntegerFloatFunction.class.getName());

        assertEquals(Integer.class, udf.getInputTypeClass());
        assertEquals(Float.class, udf.getOutputTypeClass());
        assertEquals(4.0f, udf.exec(new Integer(4)));
    }

    @Test
    public final void testIntegerToFloatPreinitialized() throws IOException, ClassNotFoundException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        com.google.common.base.Function function = new IntegerFloatFunction();
        FunctionWrapperEvalFunc udf = new FunctionWrapperEvalFunc(function);

        assertEquals(Integer.class, udf.getInputTypeClass());
        assertEquals(Float.class, udf.getOutputTypeClass());
        assertEquals(4.0f, udf.exec(new Integer(4)));
    }

    @Test
    public final void testExceptionalIntegerToFloat() throws IOException, ClassNotFoundException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        FunctionWrapperEvalFunc udf =
            new FunctionWrapperEvalFunc(ExceptionalIntegerFloatFunction.class.getName());

        assertEquals(Integer.class, udf.getInputTypeClass());
        assertEquals(Float.class, udf.getOutputTypeClass());
        assertEquals(4.0f, udf.exec(new Integer(4)));
    }

    @Test
    public final void testExceptionalIntegerToFloatWithException() throws IOException, ClassNotFoundException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {

        FunctionWrapperEvalFunc udf =
            new FunctionWrapperEvalFunc(ExceptionalIntegerFloatFunction.class.getName());

        assertEquals(Float.class, udf.getOutputTypeClass());
        Exception exception = null;
        try {
            udf.exec(new Integer(11));
        } catch (IOException e) {
            exception = e;
        }

        assertNotNull(exception);
        assertEquals(IOException.class, exception.getClass());
        assertEquals(MockException.class, exception.getCause().getClass());
    }
}
