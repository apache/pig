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

import java.io.IOException;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.Tuple;

/**
 * The interface is used for the custom mapping of a byte array, received from
 * the stdout of the streaming process, to a {@link Tuple}.
 *
 * This interface, together with {@link PigToStream}, is designed to provide
 * a common protocol for data exchange between Pig runtime and streaming
 * executables.
 *
 * The method <code>getLoadCaster</code> is called by Pig to convert the
 * fields in the byte array to typed fields of the Tuple based on a given
 * schema.
 *
 * Typically, user implements this interface for a particular type of
 * stream command and specifies the implementation class in the Pig DEFINE
 * statement.
 * @since Pig 0.7
 */

/**
 * @deprecated Use {@link org.apache.pig.PigStreamingBase}
 */

@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface StreamToPig {
    /**
     *  Given a byte array from a streaming executable, produce a tuple.
     * @param bytes to deserialize.
     * @return Data as a Pig Tuple.
     */
    public Tuple deserialize(byte[] bytes) throws IOException;

    /**
     * This will be called on the front end during planning and not on the back
     * end during execution.
     *
     * @return the {@link LoadCaster} associated with this object, or null if
     * there is no such LoadCaster.
     * @throws IOException if there is an exception during LoadCaster
     */
    public LoadCaster getLoadCaster() throws IOException;

}
