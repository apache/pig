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

import org.apache.pig.ExceptionalFunction;
import org.apache.pig.PrimitiveEvalFunc;
import org.apache.pig.impl.PigContext;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;

/**
 * EvalFunc that wraps an implementation of the Function interface, which is passed as a String
 * in the constructor. When resolving the Function class, the Pig UDF package import list is used.
 * <P>
 * The Function must have a default no-arg constructor, which will be used. For Functions that
 * take args in the constructor, initialize the function in a subclass of this one and call
 * <code>super(function)</code>.
 * <P>
 * Example: <code>DEFINE myUdf FunctionWrapperEvalFunc('MyFunction')</code>
 *
 */
public class FunctionWrapperEvalFunc extends PrimitiveEvalFunc<Object, Object> {

    // cache the types we resolve to limit reflection
    private static HashMap<Class, Type[]> resolvedTypes = new HashMap<Class, Type[]>();

    private ExceptionalFunction function;
    private String counterGroup;

    /**
     * Takes the class name of a Function, initializes it using the default constructor and passes
     * it to FunctionWrapperEvalFunc(ExceptionalFunction function). Functions must implement either
     * com.google.common.base.Function or ExceptionalFunction.
     * @param functionClassName function class to initialize
     */
    public FunctionWrapperEvalFunc(String functionClassName)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
        InstantiationException, IOException {

        this(initializeFunction(functionClassName));
    }

    /**
     * Determines the input and output types of the Function and initializes the superclass.
     * Subclass and call this constructor if a Function with a non-default constructor is required.
     * @param function Function to be used by the UDF.
     */
    protected FunctionWrapperEvalFunc(com.google.common.base.Function function)
        throws IOException, ClassNotFoundException, NoSuchMethodException {

        this((ExceptionalFunction) new GoogleFunctionBridge(function));
    }

    /**
     * Determines the input and output types of the Function and initializes the superclass.
     * Subclass and call this constructor if a Function with a non-default constructor is required.
     * @param function Function to be used by the UDF.
     */
    protected FunctionWrapperEvalFunc(ExceptionalFunction function)
        throws IOException, ClassNotFoundException, NoSuchMethodException {

        super(getFunctionInClass(function), getFunctionOutClass(function));
        this.function = function;

        String functionName = (function instanceof GoogleFunctionBridge) ?
            ((GoogleFunctionBridge)function).getWrappedFunction().getClass().getSimpleName() :
            function.getClass().getSimpleName();
        this.counterGroup = getClass().getName() + ":" + functionName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object exec(Object input) throws IOException {
        try {
            return function.apply(input);
        } catch (Exception e) {
            safeIncrCounter(getCounterGroup(), e.getClass().getCanonicalName(), 1L);
            throw new IOException(e);
        }
    }

    @Override
    protected String getCounterGroup() {
        return this.counterGroup;
    }

    private static Class getFunctionInClass(ExceptionalFunction functionClassName)
        throws ClassNotFoundException, NoSuchMethodException, IOException {
        return getFunctionTypeClass(functionClassName, 0);
    }

    private static Class getFunctionOutClass(ExceptionalFunction functionClassName)
        throws ClassNotFoundException, NoSuchMethodException, IOException {
        return getFunctionTypeClass(functionClassName, 1);
    }

    /**
     * For a given class that implements the parameterized interface <code>ExceptionalFunction</code>,
     * return the type class at the <code>index</code> position. If the Function class, is
     * <code>GoogleFunctionBridge</code>, return the type class for the wrapped function.
     */
    private static Class getFunctionTypeClass(ExceptionalFunction function, int index)
        throws ClassNotFoundException, NoSuchMethodException, IOException {

        Class clazz;
        Class expectedInterface;
        if (function instanceof GoogleFunctionBridge) {
            clazz = ((GoogleFunctionBridge) function).getWrappedFunction().getClass();
            expectedInterface = com.google.common.base.Function.class;
        }
        else {
            clazz = function.getClass();
            expectedInterface = ExceptionalFunction.class;
        }

        // check the cache
        if (resolvedTypes.containsKey(clazz)) {
            return (Class)resolvedTypes.get(clazz)[index];
        }

        Type[] interfaceTypes = clazz.getGenericInterfaces();
        for (Type interfaceType : interfaceTypes) {
            ParameterizedType parameterizedType = (ParameterizedType)interfaceType;
            if (expectedInterface.isAssignableFrom((Class) parameterizedType.getRawType())) {
                Type[] types = parameterizedType.getActualTypeArguments();
                resolvedTypes.put(clazz, types);
                return (Class)types[index];
            }
        }

        throw new NoSuchMethodException("Unrecognized function class passed: "
                + clazz.getClass() + ". Function must implement either "
                + com.google.common.base.Function.class.getName() + " or " +
                ExceptionalFunction.class.getName());
    }

    @SuppressWarnings("unchecked")
    private static ExceptionalFunction initializeFunction(String functionClassName)
            throws IOException, IllegalAccessException, InstantiationException {

        Object functionObject = PigContext.resolveClassName(functionClassName).newInstance();

        if (functionObject instanceof ExceptionalFunction) {
            return (ExceptionalFunction) functionObject;
        }
        else if (functionObject instanceof com.google.common.base.Function) {
            return new GoogleFunctionBridge((com.google.common.base.Function)functionObject);
        }

        throw new InstantiationException("Unrecognized function class passed: "
                + functionObject.getClass() + ". Function must implement either "
                + com.google.common.base.Function.class.getName() + " or " +
                ExceptionalFunction.class.getName());
    }

    /**
     * Used so we can handle both Google's Function as well as an Pig's ExceptionalFunction.
     */
    private static class GoogleFunctionBridge<S, T> implements org.apache.pig.Function {
        private com.google.common.base.Function<S, T> function;

        private GoogleFunctionBridge(com.google.common.base.Function<S, T> function) {
            this.function = function;
        }

        public com.google.common.base.Function getWrappedFunction() { return function; }

        @Override
        @SuppressWarnings("unchecked")
        public T apply(Object item) {
            return function.apply((S)item);
        }
    }
}