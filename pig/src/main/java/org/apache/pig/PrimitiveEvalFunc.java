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

import java.io.IOException;
import java.util.List;

/**
 * Base class for simple Pig UDFs that are functions of primitive types IN to OUT. Handles
 * marshalling objects, basic error checking, etc. Extend this class and implement the
 * <pre>OUT exec(IN input)</pre> method when writting a UDF that operates on only the first input
 * (of expected type IN) from the Tuple.
 */
public abstract class PrimitiveEvalFunc<IN, OUT> extends TypedOutputEvalFunc<OUT> {

    protected Class<IN> inTypeClass = null;

    public Class<IN> getInputTypeClass() { return inTypeClass; }

    @SuppressWarnings("unchecked")
    public PrimitiveEvalFunc() {
        List<?> typeArgs = getTypeArguments(PrimitiveEvalFunc.class, getClass());
        inTypeClass = (Class<IN>) typeArgs.get(0);
        outTypeClass = (Class<OUT>) typeArgs.get(1);
    }

    @SuppressWarnings("unchecked")
    public PrimitiveEvalFunc(Class inTypeClass, Class outTypeClass) {
        this.inTypeClass = inTypeClass;
        this.outTypeClass = outTypeClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    public OUT exec(Tuple tuple) throws IOException {
        verifyUdfInput(getCounterGroup(), tuple, 1);

        IN input = (IN) tuple.get(0);
        if (input == null) {
            // Default behavior of null input should be null output.
            return null;
        }

        return exec(input);
    }

    public abstract OUT exec(IN input) throws IOException;
}
