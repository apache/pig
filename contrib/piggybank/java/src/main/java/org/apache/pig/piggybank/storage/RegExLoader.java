/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.ReversibleLoadStoreFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;

/**
 * RegExLoader is an abstract class used to parse logs based on a regular expression.
 * 
 * There is a single abstract method, getPattern which needs to return a Pattern. Each group will be returned
 * as a different DataAtom.
 * 
 * Look to org.apache.pig.piggybank.storage.apachelog.CommonLogLoader for example usage.
 */

public abstract class RegExLoader implements ReversibleLoadStoreFunc {
    protected BufferedPositionedInputStream in = null;
    long end = Long.MAX_VALUE;
    private byte recordDel = (byte) '\n';
    private String fieldDel = "\t";
    final private static Charset utf8 = Charset.forName("UTF8");
    OutputStream os;

    abstract public Pattern getPattern();

    public RegExLoader() {
    }

    public Tuple getNext() throws IOException {
        if (in == null || in.getPosition() > end) {
            return null;
        }

        Pattern pattern = getPattern();
        Matcher matcher = pattern.matcher("");

        String line;
        if ((line = in.readLine(utf8, recordDel)) != null) {
            if (line.length() > 0 && line.charAt(line.length() - 1) == '\r')
                line = line.substring(0, line.length() - 1);

            matcher.reset(line);
            if (matcher.find()) {
                ArrayList<Datum> list = new ArrayList<Datum>();

                for (int i = 1; i <= matcher.groupCount(); i++) {
                    list.add(new DataAtom(matcher.group(i)));
                }
                return new Tuple(list);
            }
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

    public void bindTo(OutputStream os) throws IOException {
        this.os = os;
    }

    public void putNext(Tuple f) throws IOException {
        os.write((f.toDelimitedString(this.fieldDel) + (char) this.recordDel).getBytes("utf8"));
    }

    public void finish() throws IOException {
    }
}
