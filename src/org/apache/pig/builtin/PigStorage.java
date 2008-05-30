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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.pig.ReversibleLoadStoreFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;


/**
 * A load function that parses a line of input into fields using a delimiter to set the fields. The
 * delimiter is given as a regular expression. See String.split(delimiter) and
 * http://java.sun.com/j2se/1.5.0/docs/api/java/util/regex/Pattern.html for more information.
 */
public class PigStorage implements ReversibleLoadStoreFunc {
    protected BufferedPositionedInputStream in = null;
    long                end            = Long.MAX_VALUE;
    private byte recordDel = (byte)'\n';
    private String fieldDel = "\t";
    final private static Charset utf8 = Charset.forName("UTF8");
    
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
        if((line = in.readLine(utf8, recordDel)) != null) {            
            if (line.length()>0 && line.charAt(line.length()-1)=='\r')
                line = line.substring(0, line.length()-1);
            return new Tuple(line, fieldDel);
        }
        return null;
    }

    public void bindTo(String fileName, BufferedPositionedInputStream in, long offset, long end) throws IOException {
        this.in = in;
        this.end = end;
        
        // Since we are not block aligned we throw away the first
        // record and could on a different instance to read it
        if (offset != 0) {
            getNext();
        }
    }
    
    OutputStream os;
    public void bindTo(OutputStream os) throws IOException {
        this.os = os;
    }

    public void putNext(Tuple f) throws IOException {
        os.write((f.toDelimitedString(this.fieldDel) + (char)this.recordDel).getBytes("utf8"));
        //os.write((f.toDelimitedString(this.fieldDel) + (char)this.recordDel).getBytes(utf8));
    }

    public void finish() throws IOException {
    }

    public boolean equals(Object obj) {
        return equals((PigStorage)obj);
    }

    public boolean equals(PigStorage other) {
        return this.fieldDel.equals(other.fieldDel);
    }
    
}
