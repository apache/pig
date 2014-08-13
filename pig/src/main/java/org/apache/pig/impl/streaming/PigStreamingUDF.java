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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.WritableComparator;
import org.apache.pig.LoadCaster;
import org.apache.pig.PigStreamingBase;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.ToDate;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.WritableByteArray;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.StorageUtil;

import com.google.common.base.Charsets;

public class PigStreamingUDF extends PigStreamingBase {
    private static final byte PRE_WRAP_DELIM = '|';
    private static final byte POST_WRAP_DELIM = '_';
    private static final StreamingDelimiters DELIMS = 
            new StreamingDelimiters(PRE_WRAP_DELIM, POST_WRAP_DELIM, false);
    
    private FieldSchema topLevelFs;
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();

    private WritableByteArray out;

    public PigStreamingUDF() {
        out = new WritableByteArray();
    }
    
    public PigStreamingUDF(FieldSchema topLevelFs) {
        out = new WritableByteArray();
        this.topLevelFs = topLevelFs;
    }

    @Override
    public WritableByteArray serializeToBytes(Tuple t) throws IOException {
        out.reset();
        int sz;
        Object field;
        if (t == null) {
            sz = 0;
        } else {
            sz = t.size();
        }
        for (int i=0; i < sz; i++) {
            field = t.get(i);
            StorageUtil.putField(out, field, DELIMS, true);
            if (i != sz-1) {
                out.write(DELIMS.getParamDelim());
            }
        }
        byte[] recordDel = DELIMS.getRecordEnd();
        out.write(recordDel, 0, recordDel.length);
        return out;
    }
    
    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return new Utf8StorageConverter();
    }
    
    @Override
    public Tuple deserialize(byte[] bytes, int offset, int length) throws IOException {
        Object o = deserialize(topLevelFs, bytes, 0 + offset, length - DELIMS.getRecordEnd().length); //Drop newline
        return tupleFactory.newTuple(o);
    }
    
    public byte[] getRecordDelim() {
        return DELIMS.getRecordEnd();
    }

    private Object deserialize(FieldSchema fs, byte[] bytes, int startIndex, int endIndex) throws IOException {
        //If null, return null;
        if (WritableComparator.compareBytes(
                bytes, startIndex, DELIMS.getNull().length,
                DELIMS.getNull(), 0, DELIMS.getNull().length) == 0) {
            return null;
        }

        if (fs.type == DataType.BAG) {
            return deserializeBag(fs, bytes, startIndex + 3, endIndex - 2);
        } else if (fs.type == DataType.TUPLE) {
            return deserializeTuple(fs, bytes, startIndex + 3, endIndex - 2);
        } else if (fs.type == DataType.MAP) {
            return deserializeMap(bytes, startIndex + 3, endIndex - 2);
        }

        if (fs.type == DataType.CHARARRAY) {
            return extractString(bytes, startIndex, endIndex, true);
        } else if (fs.type == DataType.BYTEARRAY) {
            return new DataByteArray(bytes, startIndex, endIndex+1);
        }

        //Can we do this faster?
        String val = extractString(bytes, startIndex, endIndex, false);

        if (fs.type == DataType.LONG) {
            return Long.valueOf(val);
        } else if (fs.type == DataType.INTEGER) {
            return Integer.valueOf(val);
        } else if (fs.type == DataType.FLOAT) {
            return Float.valueOf(val);
        } else if (fs.type == DataType.DOUBLE) {
            return Double.valueOf(val);
        } else if (fs.type == DataType.BOOLEAN) {
            return Boolean.valueOf(val);
        } else if (fs.type == DataType.DATETIME) {
           return ToDate.extractDateTime(val);
        } else if (fs.type == DataType.BIGINTEGER) {
            return new BigInteger(val);
        } else if (fs.type == DataType.BIGDECIMAL) {
            return new BigDecimal(val);
        } else {
            throw new ExecException("Can't deserialize type: " + DataType.findTypeName(fs.type));
        }
    }
    
    private DataBag deserializeBag(FieldSchema fs, byte[] buf, int startIndex, int endIndex) throws IOException {
        ArrayList<Tuple> protoBag = new ArrayList<Tuple>();
        int depth = 0;
        int fieldStart = startIndex;

        for (int index = startIndex; index <= endIndex; index++) {
            depth = DELIMS.updateDepth(buf, depth, index);
            if ( StreamingDelimiters.isDelimiter(DELIMS.getFieldDelim(), buf, index, depth, endIndex)) {
                protoBag.add((Tuple)deserialize(fs.schema.getField(0), buf, fieldStart, index - 1));
                fieldStart = index + 3;
            }
        }
        return bagFactory.newDefaultBag(protoBag);
    }
    
    private Tuple deserializeTuple(FieldSchema fs, byte[] buf, int startIndex, int endIndex) throws IOException {
        Schema tupleSchema = fs.schema;
        
        ArrayList<Object> protoTuple = new ArrayList<Object>(tupleSchema.size());
        int depth = 0;
        int fieldNum = 0;
        int fieldStart = startIndex;
        

        for (int index = startIndex; index <= endIndex; index++) {
            depth = DELIMS.updateDepth(buf, depth, index);
            if (StreamingDelimiters.isDelimiter(DELIMS.getFieldDelim(), buf, index, depth, endIndex)) {
                protoTuple.add(deserialize(tupleSchema.getField(fieldNum), buf, fieldStart, index - 1));
                fieldStart = index + 3;
                fieldNum++;
            }
        }
        return tupleFactory.newTupleNoCopy(protoTuple);
    }
    
    private Map<String, Object> deserializeMap(byte[] buf, int startIndex, int endIndex) throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        int depth = 0;
        int fieldStart = startIndex;

        String key = null;
        Object val = null;

        for (int index = startIndex; index <= endIndex; index++) {
            byte currChar = buf[index];
            depth = DELIMS.updateDepth(buf, depth, index);

            if (currChar == DELIMS.getMapKeyDelim() && depth == 0) {
                key = extractString(buf, fieldStart, index - 1, true);
                fieldStart = index + 1;
            }

            if (StreamingDelimiters.isDelimiter(DELIMS.getFieldDelim(), buf, index, depth, endIndex)) {
                val = extractString(buf, fieldStart, index - 1, true);
                map.put(key, val);
                fieldStart = index + 3;
            }
        }

        return map;
    }
    
    private String extractString(byte[] bytes, int startIndex, int endIndex, boolean useUtf8) {
        int length = endIndex - startIndex + 1;
        if (useUtf8) {
            return new String(bytes, startIndex, length, Charsets.UTF_8);
        } else {
            return new String(bytes, startIndex, length, Charset.defaultCharset());
        }
    }
    
    

}
