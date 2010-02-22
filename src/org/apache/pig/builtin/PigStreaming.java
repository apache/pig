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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.pig.LoadCaster;
import org.apache.pig.PigToStream;
import org.apache.pig.StreamToPig;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;

/**
 * The default implementation of {@link PigToStream} and {@link StreamToPig}
 * interfaces. It converts tuples into <code>fieldDel</code> separated lines 
 * and <code>fieldDel</code> separated lines into tuples.
 * 
 */
public class PigStreaming implements PigToStream, StreamToPig {

    private byte recordDel = '\n';
    
    private byte fieldDel = '\t';
    
    private ByteArrayOutputStream out;
    
    /**
     * The constructor that uses the default field delimiter.
     */
    public PigStreaming() {
        out = new ByteArrayOutputStream();
    }
    
    /**
     * The constructor that accepts a user-specified field
     * delimiter.
     * 
     * @param delimiter a <code>String</code> specifying the field
     * delimiter.
     */
    public PigStreaming(String delimiter) {
        this();
        fieldDel = StorageUtil.parseFieldDel(delimiter);
    }
    
    @Override
    public byte[] serialize(Tuple t) throws IOException {
        out.reset();
        int sz = t.size();
        for (int i=0; i<sz; i++) {
            Object field = t.get(i);

            StorageUtil.putField(out,field);

            if (i == sz - 1) {
                // last field in tuple.
                out.write(recordDel);
            } else {
                out.write(fieldDel);
            }
        }
        return out.toByteArray();
    }

    @Override
    public Tuple deserialize(byte[] bytes) throws IOException {
        Text val = new Text(bytes);
        return StorageUtil.textToTuple(val, fieldDel);
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {        
        return new Utf8StorageConverter();
    }

}
