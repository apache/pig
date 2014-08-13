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
 * The interface used for the custom mapping of a {@link Tuple} to a byte
 * array. The byte array is fed to the stdin of the streaming process.
 *
 * This interface, together with {@link StreamToPig}, is designed to provide
 * a common protocol for data exchange between Pig runtime and streaming
 * executables.
 *
 * Typically, a user implements this interface for a particular type of
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
public interface PigToStream {

    /**
     * Given a tuple, produce an array of bytes to be passed to the streaming
     * executable.
     * @param t Tuple to serialize
     * @return Serialized form of the tuple
     * @throws IOException
     */
    public byte[] serialize(Tuple t) throws IOException;

}
