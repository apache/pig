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

import org.apache.pig.LoadCaster;
import org.apache.pig.PigStreamingBase;
import org.apache.pig.data.WritableByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;

/**
 * The default implementation of {@link PigStreamingBase}.
 * It converts tuples into <code>fieldDel</code> separated lines
 * and <code>fieldDel</code> separated lines into tuples.
 *
 */
public class PigStreaming extends PigStreamingBase {

    private byte recordDel = '\n';

    private byte fieldDel = '\t';

    private WritableByteArray out;

    /**
     * The constructor that uses the default field delimiter.
     */
    public PigStreaming() {
        out = new WritableByteArray();
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
    public WritableByteArray serializeToBytes(Tuple t) throws IOException {
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
        return out;
    }

    @Override
    public Tuple deserialize(byte[] bytes, int offset, int length) throws IOException {
        return StorageUtil.bytesToTuple(bytes, offset, length, fieldDel);
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return new Utf8StorageConverter();
    }

}
