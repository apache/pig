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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.pig.PigStreamingBase;
import org.apache.pig.StreamToPig;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;

import com.google.common.base.Charsets;

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
    public static final Object END_OF_OUTPUT = new Object();
    private static final byte[] DEFAULT_RECORD_DELIM = new byte[] {'\n'};

    public enum OutputType {SYNCHRONOUS, ASYNCHRONOUS}

    /*
     * The deserializer to be used to send data to the managed process.
     *
     * It is the responsibility of the concrete sub-classes to setup and
     * manage the deserializer.
     */
    protected StreamToPig deserializer;

    private PigStreamingBase newDeserializer;

    protected LineReader in = null;

    private Text currValue = new Text();

    private BufferedPositionedInputStream istream;
    
    //Both of these ignore the trailing \n.  So if the
    //default delimiter is "\n" recordDelimStr is "".
    private String recordDelimStr = null;
    private int recordDelimLength = 0;

    /**
     * Get the handled <code>OutputType</code>.
     * @return the handled <code>OutputType</code>
     */
    public abstract OutputType getOutputType();

    // flag to mark if close() has already been called
    protected boolean alreadyClosed = false;

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
        this.istream  = is;
        this.in = new LineReader(istream);
        if (this.deserializer instanceof PigStreamingBase) {
            this.newDeserializer = (PigStreamingBase) deserializer;
        }
    }

    /**
     * Get the next output <code>Tuple</code> of the managed process.
     *
     * @return the next output <code>Tuple</code> of the managed process
     * @throws IOException
     */
    public Tuple getNext() throws IOException {
        if (in == null) {
            return null;
        }

        currValue.clear();
        if (!readValue()) {
            return null;
        }

        if (newDeserializer != null) {
            return newDeserializer.deserialize(currValue.getBytes(), 0, currValue.getLength());
        } else {
            byte[] newBytes = new byte[currValue.getLength()];
            System.arraycopy(currValue.getBytes(), 0, newBytes, 0, currValue.getLength());
            return deserializer.deserialize(newBytes);
        }
    }

    private boolean readValue() throws IOException {
        int num = in.readLine(currValue);
        if (num <= 0) {
            return false;
        }

        while(!isEndOfRow()) {
            //Need to add back the newline character we ate.
            currValue.append(new byte[] {'\n'}, 0, 1);

            byte[] lineBytes = readNextLine();
            if (lineBytes == null) {
                //We have no more input, so just break;
                break;
            }
            currValue.append(lineBytes, 0, lineBytes.length);
        }
        
        return true;
    }
    
    private byte[] readNextLine() throws IOException {
        Text line = new Text();
        int num = in.readLine(line);
        byte[] lineBytes = line.getBytes();
        if (num <= 0) {
            return null;
        }
        
        return lineBytes;
    }

    private boolean isEndOfRow() {
        if (recordDelimStr == null) {
            byte[] recordDelimBa = getRecordDelimiter();
            recordDelimLength = recordDelimBa.length - 1; //Ignore trailing \n
            recordDelimStr = new String(recordDelimBa, 0, recordDelimLength,  Charsets.UTF_8);
        }
        if (recordDelimLength == 0 || currValue.getLength() < recordDelimLength) {
            return true;
        }
        return currValue.find(recordDelimStr, currValue.getLength() - recordDelimLength) >= 0;
    }
    
    protected byte[] getRecordDelimiter() {
        return DEFAULT_RECORD_DELIM;
    }

    /**
     * Close the <code>OutputHandler</code>.
     * @throws IOException
     */
    public synchronized void close() throws IOException {
        if(!alreadyClosed) {
            istream.close();
            istream = null;
            alreadyClosed = true;
        }
    }
}
