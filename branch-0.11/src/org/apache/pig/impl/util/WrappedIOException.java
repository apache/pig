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

package org.apache.pig.impl.util;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;

/**
 * @deprecated This class was introduced to overcome the limitation that before 
 * Java 1.6, {@link IOException} did not have a constructor which took a 
 * {@link Throwable} argument. Since Pig code is now compiled with Java 1.6 and 
 * {@link EvalFunc} and {@link LoadFunc} user implementations should also use 
 * Java 1.6, they can use {@link IOException} instead. From Java 1.6, 
 * {@link IOException} has constructors which take a {@link Throwable}
 * argument.
 */
@Deprecated 
public class WrappedIOException {

    public static IOException wrap(final Throwable e) {
        return wrap(e.getMessage(), e);
    }
    
    public static IOException wrap(final String message, final Throwable e) {
        final IOException wrappedException = new IOException(message + " [" +
            e.getMessage() + "]");
        wrappedException.setStackTrace(e.getStackTrace());
        wrappedException.initCause(e);
        return wrappedException;
    }
}
