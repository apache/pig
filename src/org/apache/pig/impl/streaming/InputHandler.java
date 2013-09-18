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
package org.apache.pig.impl.streaming;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.pig.PigStreamingBase;
import org.apache.pig.PigToStream;
import org.apache.pig.data.WritableByteArray;
import org.apache.pig.data.Tuple;

/**
 * {@link InputHandler} is responsible for handling the input to the
 * Pig-Streaming external command.
 *
 * The managed executable could be fed input in a {@link InputType#SYNCHRONOUS}
 * manner via its <code>stdin</code> or in an {@link InputType#ASYNCHRONOUS}
 * manner via an external file which is subsequently read by the executable.
 */
public abstract class InputHandler {
    /**
     *
     */
    public enum InputType {SYNCHRONOUS, ASYNCHRONOUS}

    /*
     * The serializer to be used to send data to the managed process.
     *
     * It is the responsibility of the concrete sub-classes to setup and
     * manage the serializer.
     */
    protected PigToStream serializer;

    private PigStreamingBase newSerializer;

    private OutputStream out;

    // flag to mark if close() has already been called
    protected boolean alreadyClosed = false;

    /**
     * Get the handled <code>InputType</code>
     * @return the handled <code>InputType</code>
     */
    public abstract InputType getInputType();

    /**
     * Send the given input <code>Tuple</code> to the managed executable.
     *
     * @param t input <code>Tuple</code>
     * @throws IOException
     */
    public void putNext(Tuple t) throws IOException {
        if (newSerializer != null) {
            WritableByteArray buf = newSerializer.serializeToBytes(t);
            out.write(buf.getData(), 0, buf.getLength());
        } else {
            out.write(serializer.serialize(t));
        }
    }

    /**
     * Close the <code>InputHandler</code> since there is no more input
     * to be sent to the managed process.
     * @param process the managed process - this could be null in some cases
     * like when input is through files. In that case, the process would not
     * have been exec'ed yet - if this method if overridden it is the responsibility
     * of the implementer to check that the process is usable. The managed process
     * object is supplied by the ExecutableManager to this call so that this method
     * can check if the process is alive if it needs to know.
     *
     * @throws IOException
     */
    public synchronized void close(Process process) throws IOException {
        if(!alreadyClosed) {
            alreadyClosed = true;
            out.flush();
            out.close();
            out = null;
        }
    }

    /**
     * Bind the <code>InputHandler</code> to the <code>OutputStream</code>
     * from which it reads input and sends it to the managed process.
     *
     * @param os <code>OutputStream</code> from which to read input data for the
     *           managed process
     * @throws IOException
     */
    public void bindTo(OutputStream os) throws IOException {
        out = os;
        if (this.serializer instanceof PigStreamingBase) {
            this.newSerializer = (PigStreamingBase) serializer;
        }
    }
}
