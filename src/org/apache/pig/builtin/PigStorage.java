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
package org.apache.pig.builtin;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.TimestampedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;


/**
 * A load function that parses a line of input into fields using a delimiter to set the fields. The
 * delimiter is given as a regular expression. See String.split(delimiter) and
 * http://java.sun.com/j2se/1.5.0/docs/api/java/util/regex/Pattern.html for more information.
 */
public class PigStorage implements LoadFunc, StoreFunc {
    protected BufferedPositionedInputStream in = null;
    private DataInputStream inData = null;
        
	long                end            = Long.MAX_VALUE;
	private String recordDel = "\n";
	private String fieldDel = "\t";
    
    public PigStorage() {
    }

    /**
     * Constructs a Pig loader that uses specified regex as a field delimiter.
     * 
     * @param delimiter
     *            the regular expression that is used to separate fields. ("\t" is the default.) See
     *            http://java.sun.com/j2se/1.5.0/docs/api/java/util/regex/Pattern.html for complete
     *            explanation.
     */
    public PigStorage(String delimiter) {
        this.fieldDel = delimiter;
    }

	public Tuple getNext() throws IOException {
        if (in == null || in.getPosition() > end) {
            return null;
        }
        String line;
        if((line = inData.readLine()) != null) {            
            return new Tuple(line, fieldDel);
        }
        return null;
    }

	public void bindTo(String fileName, BufferedPositionedInputStream in, long offset, long end) throws IOException {
        this.in = in;
        inData = new DataInputStream(in);
        this.end = end;
        
        // Since we are not block aligned we throw away the first
        // record and cound on a different instance to read it
        if (offset != 0) {
            getNext();
        }
    }
    
    OutputStream os;
    public void bindTo(OutputStream os) throws IOException {
        this.os = os;
    }

    public void putNext(Tuple f) throws IOException {
        os.write((f.toDelimitedString(this.fieldDel) + this.recordDel).getBytes());
    }

    public void finish() throws IOException {
    }

}
