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
package org.apache.pig.data;

import java.io.ByteArrayOutputStream;

/** A reusable byte buffer implementation
 *
 * <p>This saves memory over creating a new byte[] and
 * ByteArrayOutputStream each time data is written.
 *
 * <p>Typical usage is something like the following:<pre>
 *
 * WritableByteArray buffer = new WritableByteArray();
 * while (... loop condition ...) {
 *   buffer.reset();
 *   ... write to buffer ...
 *   byte[] data = buffer.getData();
 *   int dataLength = buffer.getLength();
 *   ... write data to its ultimate destination ...
 * }
 * </pre>
 *
 */
public class WritableByteArray extends ByteArrayOutputStream {

    public WritableByteArray() {
        super();
    }

    public WritableByteArray(int size) {
        super(size);
    }

    public WritableByteArray(byte[] buf) {
        this.buf = buf;
        this.count = buf.length;
    }

    /**
     * Returns the current contents of the buffer.
     * Data is only valid to {@link #getLength()}.
     * @return contents of the buffer
     */
    public byte[] getData() {
        return buf;
    }

    /**
     * Returns the length of the valid data currently in the buffer
     * @return length of valid data
     */
    public int getLength() {
        return count;
    }

}
