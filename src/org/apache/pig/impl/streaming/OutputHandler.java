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

import org.apache.pig.LoadFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;

/**
 * {@link OutputHandler} is responsible for handling the output of the 
 * Pig-Streaming external command.
 * 
 * The output of the managed executable could be fetched in a 
 * {@link OutputType#SYNCHRONOUS} manner via its <code>stdout</code> or in an 
 * {@link OutputType#ASYNCHRONOUS} manner via an external file to which the
 * process wrote its output.
 */
public abstract class OutputHandler {
    public enum OutputType {SYNCHRONOUS, ASYNCHRONOUS}

    /*
     * The deserializer to be used to send data to the managed process.
     * 
     * It is the responsibility of the concrete sub-classes to setup and
     * manage the deserializer. 
     */  
    protected LoadFunc deserializer;
    
    /**
     * Get the handled <code>OutputType</code>.
     * @return the handled <code>OutputType</code> 
     */
    public abstract OutputType getOutputType();
    
    /**
     * Bind the <code>OutputHandler</code> to the <code>InputStream</code>
     * from which to read the output data of the managed process.
     * 
     * @param is <code>InputStream</code> from which to read the output data 
     *           of the managed process
     * @throws IOException
     */
    public void bindTo(String fileName, BufferedPositionedInputStream is,
                       long offset, long end) throws IOException {
        deserializer.bindTo(fileName, new BufferedPositionedInputStream(is), 
                            offset, end);
    }
    
    /**
     * Get the next output <code>Tuple</code> of the managed process.
     * 
     * @return the next output <code>Tuple</code> of the managed process
     * @throws IOException
     */
    public Tuple getNext() throws IOException {
        return deserializer.getNext();
    }
    
    /**
     * Close the <code>OutputHandler</code>.
     * @throws IOException
     */
    public void close() throws IOException {}
}
