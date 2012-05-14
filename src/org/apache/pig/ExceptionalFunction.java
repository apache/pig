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

/**
 * An interface that captures a unit of work against an item where an exception might be thrown.
 *
 * @param <S> The argument type for the function.
 * @param <T> The return type for the function.
 * @param <E> The exception type that the function throws.
 *
 */
public interface ExceptionalFunction<S, T, E extends Exception> {

    /**
     * Performs a unit of work on item, possibly throwing {@code E} in the process.
     *
     * @param item The item to perform work against.
     * @return The result of the computation.
     * @throws E if there was a problem performing the work.
     */
    public T apply(S item) throws E;
}