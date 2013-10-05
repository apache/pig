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
package org.apache.pig.piggybank.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.piggybank.storage.hiverc.HiveRCOutputFormat;

public class HiveColumnarStorage extends PigStorage {
    private static final String UTF8 = "UTF-8";

    private static final char LIST_DELIMITER = 2;
    private static final char MAP_DELIMITER = 3;

    private int numColumns = -1;
    private ByteStream.Output byteStream;
    private BytesRefArrayWritable rowWritable;
    private BytesRefWritable[] colValRefs;

    @Override
    public OutputFormat getOutputFormat() {
        return new HiveRCOutputFormat();
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        super.setStoreLocation(location, job);
        // set number of columns if this is set in context.
        Properties p = getUDFProperties();
        if (p != null) {
            numColumns = Integer.parseInt(p.getProperty("numColumns", "-1"));
        }

        if (numColumns > 0) {
            RCFileOutputFormat.setColumnNumber(job.getConfiguration(), numColumns);
        }
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        super.checkSchema(s);
        getUDFProperties().setProperty("numColumns", Integer.toString(s.getFields().length));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple t) throws IOException {

        if (rowWritable == null) { // initialize
            if (numColumns < 1) {
                throw new IOException("number of columns is not set");
            }

            byteStream = new ByteStream.Output();
            rowWritable = new BytesRefArrayWritable();
            colValRefs = new BytesRefWritable[numColumns];

            for (int i = 0; i < numColumns; i++) {
                colValRefs[i] = new BytesRefWritable();
                rowWritable.set(i, colValRefs[i]);
            }
        }

        byteStream.reset();

        int sz = t.size();
        int startPos = 0;

        for (int i = 0; i < sz && i < numColumns; i++) {

            putField(byteStream, t.get(i));
            colValRefs[i].set(byteStream.getData(), startPos, byteStream.getCount() - startPos);
            startPos = byteStream.getCount();
        }

        try {
            writer.write(null, rowWritable);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private Properties getUDFProperties() {
        return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                new String[] { signature });
    }

    public void putField(OutputStream out, Object field) throws IOException {

        switch (DataType.findType(field)) {
        case DataType.NULL:
            break; // just leave it empty

        case DataType.BOOLEAN:
            out.write(((Boolean) field).toString().getBytes());
            break;

        case DataType.INTEGER:
            out.write(((Integer) field).toString().getBytes());
            break;

        case DataType.LONG:
            out.write(((Long) field).toString().getBytes());
            break;

        case DataType.FLOAT:
            out.write(((Float) field).toString().getBytes());
            break;

        case DataType.DOUBLE:
            out.write(((Double) field).toString().getBytes());
            break;

        case DataType.BYTEARRAY:
            byte[] b = ((DataByteArray) field).get();
            out.write(b, 0, b.length);
            break;

        case DataType.CHARARRAY:
            out.write(((String) field).getBytes(UTF8));
            break;

        case DataType.MAP:
            boolean mapHasNext = false;
            Map<String, Object> m = (Map<String, Object>) field;

            for (Map.Entry<String, Object> e : m.entrySet()) {
                if (mapHasNext) {
                    out.write(LIST_DELIMITER);
                } else {
                    mapHasNext = true;
                }
                putField(out, e.getKey());
                out.write(MAP_DELIMITER);
                putField(out, e.getValue());
            }

            break;
        case DataType.INTERNALMAP:
            boolean internalMapHasNext = false;
            Map<String, Object> im = (Map<String, Object>) field;

            for (Map.Entry<String, Object> e : im.entrySet()) {
                if (internalMapHasNext) {
                    out.write(LIST_DELIMITER);
                } else {
                    internalMapHasNext = true;
                }
                putField(out, e.getKey());
                out.write(MAP_DELIMITER);
                putField(out, e.getValue());
            }

            break;

        case DataType.TUPLE:
            boolean tupleHasNext = false;
            Tuple t = (Tuple) field;

            for (int i = 0; i < t.size(); ++i) {
                if (tupleHasNext) {
                    out.write(LIST_DELIMITER);
                } else {
                    tupleHasNext = true;
                }
                try {
                    putField(out, t.get(i));
                } catch (ExecException ee) {
                    throw ee;
                }
            }

            break;

        case DataType.BAG:
            boolean bagHasNext = false;
            Iterator<Tuple> tupleIter = ((DataBag) field).iterator();
            while (tupleIter.hasNext()) {
                if (bagHasNext) {
                    out.write(LIST_DELIMITER);
                } else {
                    bagHasNext = true;
                }
                putField(out, tupleIter.next());
            }

            break;

        default: {
            int errCode = 2108;
            String msg = "Could not determine data type of field: " + field;
            throw new ExecException(msg, errCode, PigException.BUG);
        }

        }
    }

}
