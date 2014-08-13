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
import org.apache.pig.data.WritableByteArray;
import org.apache.pig.data.Tuple;

/**
 * The interface is used for the custom mapping of
 *    - a {@link Tuple} to a byte array. The byte array is fed to the stdin
 *      of the streaming process.
 *    - a byte array, received from the stdout of the streaming process,
 *      to a {@link Tuple}.
 *
 * This interface is designed to provide a common protocol for data exchange
 * between Pig runtime and streaming executables.
 *
 * Typically, a user implements this interface for a particular type of stream
 * command and specifies the implementation class in the Pig DEFINE statement.
 *
 * @since Pig 0.12
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class PigStreamingBase implements PigToStream, StreamToPig {

    @Override
    @Deprecated
    public final byte[] serialize(Tuple t) throws IOException {
        WritableByteArray data = serializeToBytes(t);
        if (data.getLength() == data.getData().length) {
            return data.getData();
        } else {
            byte[] buf = new byte[data.getLength()];
            System.arraycopy(data.getData(), 0, buf, 0, data.getLength());
            return buf;
        }
    }

    /**
     * Given a tuple, produce an array of bytes to be passed to the streaming
     * executable.
     * @param t Tuple to serialize
     * @return Serialized form of the tuple
     * @throws IOException
     */
    public abstract WritableByteArray serializeToBytes(Tuple t) throws IOException;

    @Override
    @Deprecated
    public final Tuple deserialize(byte[] bytes) throws IOException {
        return deserialize(bytes, 0, bytes.length);
    }

    /**
     * Given a byte array from a streaming executable, produce a tuple.
     * @param bytes  bytes to deserialize.
     * @param offset the offset in the byte array from which to deserialize.
     * @param length the number of bytes from the offset of the byte array to deserialize.
     * @return Data as a Pig Tuple.
     * @throws IOException
     */
    public abstract Tuple deserialize(byte[] bytes, int offset, int length) throws IOException;

    /**
     * This will be called on the front end during planning and not on the back
     * end during execution.
     *
     * @return the {@link LoadCaster} associated with this object, or null if
     * there is no such LoadCaster.
     * @throws IOException if there is an exception during LoadCaster
     */
    @Override
    public abstract LoadCaster getLoadCaster() throws IOException;

}
