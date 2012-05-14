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

import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import java.io.IOException;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class for Pig UDFs that are functions from Tuples to generic type OUT. Handles marshalling
 * objects, basic error checking, etc. Also infers outputSchema and provides a function to verify
 * the input Tuple.
 * <P></P>
 * Extend this class and implement the <pre>OUT exec(Tuple input)</pre> method when writting a UDF
 * that operates on multiple inputs from the Tuple.
 */
public abstract class TypedOutputEvalFunc<OUT> extends EvalFunc<OUT> {

    // Used to implement outputSchema below.
    protected Class<OUT> outTypeClass = null;

    public Class<OUT> getOutputTypeClass() { return outTypeClass; }

    @SuppressWarnings("unchecked")
    public TypedOutputEvalFunc() {
        outTypeClass = (Class<OUT>) getTypeArguments(TypedOutputEvalFunc.class, getClass()).get(0);
    }

    // Increment Hadoop counters for bad inputs which are either null or too small.
    protected void verifyInput(Tuple input, int minimumSize) throws IOException {
        verifyUdfInput(getCounterGroup(), input, minimumSize);
    }

    /**
     * Incremented counters will use this as the counter group. Typically this works fine, since
     * the subclass name is enough to identify the UDF. In some cases though (i.e. a UDF wrapper that
     * is a facade to a number of different transformation functions), a more specific group name is
     * needed.
     */
    protected String getCounterGroup() {
        return getClass().getName();
    }

    /**
     * Get the actual type arguments a child class has used to extend a generic base class.
     *
     * @param baseClass the base class
     * @param childClass the child class
     * @return a list of the raw classes for the actual type arguments.
     */
    @SuppressWarnings("unchecked")
    protected static <T> List<Class<?>> getTypeArguments(Class<T> baseClass,
        Class<? extends T> childClass) {
        Map<Type, Type> resolvedTypes = Maps.newHashMap();
        Type type = childClass;
        // start walking up the inheritance hierarchy until we hit baseClass
        while (!getClass(type).equals(baseClass)) {
            if (type instanceof Class) {
                // there is no useful information for us in raw types, so just keep going.
                type = ((Class) type).getGenericSuperclass();
            } else {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                Class<?> rawType = (Class) parameterizedType.getRawType();

                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                TypeVariable<?>[] typeParameters = rawType.getTypeParameters();
                for (int i = 0; i < actualTypeArguments.length; i++) {
                    resolvedTypes.put(typeParameters[i], actualTypeArguments[i]);
                }

                if (!rawType.equals(baseClass)) {
                    type = rawType.getGenericSuperclass();
                }
            }
        }

        // finally, for each actual type argument provided to baseClass, determine (if possible)
        // the raw class for that type argument.
        Type[] actualTypeArguments;
        if (type instanceof Class) {
            actualTypeArguments = ((Class) type).getTypeParameters();
        } else {
            actualTypeArguments = ((ParameterizedType) type).getActualTypeArguments();
        }
        List<Class<?>> typeArgumentsAsClasses = new ArrayList<Class<?>>();
        // resolve types by chasing down type variables.
        for (Type baseType : actualTypeArguments) {
            while (resolvedTypes.containsKey(baseType)) {
                baseType = resolvedTypes.get(baseType);
            }
            typeArgumentsAsClasses.add(getClass(baseType));
        }
        return typeArgumentsAsClasses;
    }

    /**
     * Get the underlying class for a type, or null if the type is a variable type.
     *
     * @param type the type
     * @return the underlying class
     */
    @SuppressWarnings("unchecked")
    private static Class<?> getClass(Type type) {
        if (type instanceof Class) {
            return (Class) type;
        } else if (type instanceof ParameterizedType) {
            return getClass(((ParameterizedType) type).getRawType());
        } else if (type instanceof GenericArrayType) {
            Type componentType = ((GenericArrayType) type).getGenericComponentType();
            Class<?> componentClass = getClass(componentType);
            if (componentClass != null) {
                return Array.newInstance(componentClass, 0).getClass();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    /** Increment Hadoop counters for bad inputs which are either null or too small.
     *
     * @param klass the name of the calling class, for recording purposes
     * @param input the tuple passed to the UDF.
     * @param minimumSize the minimum size required of the tuple.
     */
    protected static void verifyUdfInput(String klass, Tuple input, int minimumSize) throws IOException {
        if (input == null) {
            safeIncrCounter(klass, "NullInput", 1L);
            throw new IOException("Null input to UDF " + klass);
        } else if (input.size() < minimumSize) {
             String reason = "TooFewArguments_Got_" + input.size() + "_NeededAtLeast_" + minimumSize;
              safeIncrCounter(klass, reason, 1L);
            throw new IOException("Not enough arguments to " + klass + ": got " + input.size() +
                    ", expected at least " + minimumSize);
        } else {
            safeIncrCounter(klass, "ValidInput", 1L);
        }
    }

    protected static void safeIncrCounter(String group, String name, Long increment) {
        Counter counter = PigStatusReporter.getInstance().getCounter(group, name);
        if (counter != null) {
            counter.increment(increment);
        }
    }
}
