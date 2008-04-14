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
package org.apache.pig.backend.executionengine;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.pig.Slice;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.SeekableInputStream;
import org.apache.pig.backend.datastorage.SeekableInputStream.FLAGS;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.tools.bzip2r.CBZip2InputStream;

/**
 * Slice that loads data using a LoadFunc.
 */
public class PigSlice implements Slice {

    public PigSlice(String path, String parser, long start, long length) {
        this.file = path;
        this.start = start;
        this.length = length;
        this.parser = parser;
    }

    public String[] getLocations() {
        return new String[] { file };
    }

    public long getStart() {
        return start;
    }
    
    public long getLength() {
        return length;
    }

    public void init(DataStorage base) throws IOException {
        if (parser == null) {
            loader = new PigStorage();
        } else {
            try {
                loader = (LoadFunc) PigContext.instantiateFuncFromSpec(parser);
            } catch (Exception exp) {
                throw new RuntimeException("can't instantiate " + parser);
            }
        }
        fsis = base.asElement(base.getActiveContainer(), file).sopen();
        fsis.seek(start, FLAGS.SEEK_CUR);

        end = start + getLength();

        if (file.endsWith(".bz") || file.endsWith(".bz2")) {
            is = new CBZip2InputStream(fsis, 9);
        } else if (file.endsWith(".gz")) {
            is = new GZIPInputStream(fsis);
            // We can't tell how much of the underlying stream GZIPInputStream
            // has actually consumed
            end = Long.MAX_VALUE;
        } else {
            is = fsis;
        }
        loader.bindTo(file.toString(), new BufferedPositionedInputStream(is,
                start), start, end);
    }

    public boolean next(Tuple value) throws IOException {
        Tuple t = loader.getNext();
        if (t == null) {
            return false;
        }
        value.copyFrom(t);
        return true;
    }

    public long getPos() throws IOException {
        return fsis.tell();
    }

    public void close() throws IOException {
        is.close();
    }

    public float getProgress() throws IOException {
        float progress = getPos() - start;
        float finish = getLength();
        return progress / finish;
    }

    // assigned during construction
    String file;
    long start;
    long length;
    String parser;

    // Created as part of init
    private InputStream is;
    private SeekableInputStream fsis;
    private long end;
    private LoadFunc loader;

    private static final long serialVersionUID = 1L;
}
