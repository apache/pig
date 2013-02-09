/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.pig.piggybank.storage.avro;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * An avro GenericDatumWriter to write pig data as Avro data.
 *
 */
public class PigAvroDatumWriter extends GenericDatumWriter<Object> {

    /**
     * construct with output schema
     */
    public PigAvroDatumWriter(Schema schema) {
        setSchema(schema);
    }

    @Override
    protected void write(Schema schema, Object datum, Encoder out)
                                    throws IOException {
        try {

            /**
             * In case users want to get rid of Pig tuple wrapper
             */
            if (!schema.getType().equals(Schema.Type.RECORD)
                    && !schema.getType().equals(Schema.Type.UNION)
                    && datum instanceof Tuple
                    && unwrappedInstanceOf(schema, datum)) {
                Tuple t = (Tuple) datum;
                if (t.size() > 1)
                    throw new IOException("Incompatible schema:" + schema+ " \n for data " + datum);
                write(schema, t.get(0), out);
                return;
            }

            switch (schema.getType()) {
            case FIXED:
                writeFixed(schema, datum, out);
                break;
            case ENUM:
                writeEnum(schema, datum, out);
                break;
            case STRING:
                writeString(schema, datum, out);
                break;
            case BYTES:
                writeBytes(datum, out);
                break;
            case BOOLEAN:
                writeBoolean(datum, out);
                break;
            case UNION:
                writeUnion(schema, datum, out);
                break;
            case LONG:
                writeLong(datum, out);
                break;
            case FLOAT:
                writeFloat(datum, out);
                break;
            case DOUBLE:
                writeDouble(datum, out);
                break;
            case ARRAY: /*falls through*/
            case MAP: /*falls through*/
            case RECORD: /*falls through*/
            case INT: /*falls through*/
            case NULL:/*falls through*/
            default:
                super.write(schema, datum, out);
            }
        } catch (NullPointerException e) {
            throw npe(e, " of " + schema.getName());
        }
    }

    /**
     * Called to write union. 
     */
    protected void writeUnion(Schema schema, Object datum, Encoder out)
                                    throws IOException {
        int index = resolveUnionSchema(schema, datum);
        out.writeIndex(index);
        write(schema.getTypes().get(index), datum, out);
    }

    /**
     * Called to resolve union. 
     */
    protected int resolveUnionSchema(Schema union, Object datum) throws IOException {
        int i = 0;
        for (Schema type : union.getTypes()) {
            if (type.getType().equals(Schema.Type.UNION))
                throw new IOException("A union cannot immediately contain other unions.");
            if (instanceOf(type, datum))
                return i;
            i++;
        }
        throw new RuntimeException("Datum " + datum + " is not in union " + union);
    }

    /**
     * Recursively check whether "datum" is an instance of "schema" and called 
     * by {@link #resolveUnionSchema(Schema,Object)},
     * {@link #unwrappedInstanceOf(Schema,Object)}. 
     * 
     */
    protected boolean instanceOf(Schema schema, Object datum)
                                    throws IOException {

        try {
            switch (schema.getType()) {
            case RECORD:
                if (datum instanceof Tuple) {
                    Tuple tuple = (Tuple) datum;
                    List<Field> fields = schema.getFields();
                    if (fields.size() != tuple.size()) {
                        return false;
                    }
                    for (int i = 0; i < fields.size(); i++) {
                        if (!instanceOf(fields.get(i).schema(), tuple.get(i)))
                            return false;
                    }
                    return true;
                }
                return false;

            case UNION:
                @SuppressWarnings("unused")
                int index = resolveUnionSchema(schema, datum);
                return true;
            case ENUM:
                return datum instanceof String && schema.hasEnumSymbol(((String) datum))
                            || unwrappedInstanceOf(schema, datum);
            case ARRAY:
                return datum instanceof DataBag
                            ||  unwrappedInstanceOf(schema, datum);
            case MAP:
                return datum instanceof Map
                            || unwrappedInstanceOf(schema, datum);
            case FIXED:
                return datum instanceof DataByteArray && ((DataByteArray) datum).size() == schema.getFixedSize()
                            || unwrappedInstanceOf(schema, datum);
            case STRING:
                return datum instanceof String
                            || unwrappedInstanceOf(schema, datum);
            case BYTES:
                return datum instanceof DataByteArray
                            || unwrappedInstanceOf(schema, datum);
            case INT:
                return datum instanceof Integer
                            || unwrappedInstanceOf(schema, datum);
            case LONG:
                return datum instanceof Long
                            || datum instanceof Integer
                            || unwrappedInstanceOf(schema, datum);
            case FLOAT:
                return datum instanceof Float
                            || datum instanceof Integer
                            || datum instanceof Long
                            || unwrappedInstanceOf(schema, datum);
            case DOUBLE:
                return datum instanceof Double
                            || datum instanceof Float
                            || datum instanceof Integer
                            || datum instanceof Long
                            || unwrappedInstanceOf(schema, datum);
            case BOOLEAN:
                return datum instanceof Boolean
                            || datum instanceof Integer
                            || unwrappedInstanceOf(schema, datum);
            case NULL:
                return datum == null;
            default:
                throw new RuntimeException("Unexpected type: " + schema);
            }
        } catch (ExecException e) {
            e.printStackTrace(System.err);
            throw new RuntimeException(e);
        }
    }

    /**
     * Check whether "datum" is an instance of "schema" after stripping
     * the tuple wrapper.
     */
    private boolean unwrappedInstanceOf(Schema schema, Object datum)
                                    throws IOException {
        try {
            if (!(datum instanceof Tuple))
                return false;

            Tuple tuple = (Tuple) datum;
            if (tuple.size() != 1)
                return false;

            switch (schema.getType()) {
            case ENUM:
            case ARRAY:
            case MAP:
            case FIXED:
            case STRING:
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return instanceOf(schema, tuple.get(0));
            default:
                throw new IOException("Invalid type:" + schema.getType());
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
            throw new IOException(e);
        }
    }

    /**
     * Write double. Users can cast long, float and integer to double.
     * 
     */
    protected void writeDouble(Object datum, Encoder out) throws IOException {
        double num;
        if (datum instanceof Integer) {
            num = ((Integer) datum).doubleValue();
        } else if (datum instanceof Long) {
            num = ((Long) datum).doubleValue();
        } else if (datum instanceof Float) {
            num = ((Float) datum).doubleValue();
        } else if (datum instanceof Double) {
            num = (Double) datum;
        } else
            throw new IOException("Cannot convert to double:" + datum.getClass());

        out.writeDouble(num);
    }

    /**
     * Write float. Users can cast long and integer into float.
     * 
     */
    protected void writeFloat(Object datum, Encoder out) throws IOException {
        float num;
        if (datum instanceof Integer) {
            num = ((Integer) datum).floatValue();
        } else if (datum instanceof Long) {
            num = ((Long) datum).floatValue();
        } else if (datum instanceof Float) {
            num = (Float) datum;
        } else
            throw new IOException("Cannot convert to float:" + datum.getClass());

        out.writeFloat(num);

    }

    /**
     * Write long. Users can cast integer into long.
     * 
     */
    protected void writeLong(Object datum, Encoder out) throws IOException {
        long num;
        if (datum instanceof Integer) {
            num = ((Integer) datum).longValue();
        } else if (datum instanceof Long) {
            num = (Long) datum;
        } else
            throw new IOException("Cannot convert to long:" + datum.getClass());

        out.writeLong(num);
    }

    /**
     * Write boolean. Users can cast an integer into boolean.
     * 
     */
    protected void writeBoolean(Object datum, Encoder out) throws IOException {

        if (datum instanceof Boolean) {
            out.writeBoolean((Boolean) datum);
        } else if (datum instanceof Integer) {
            out.writeBoolean(((Integer) datum) != 0);
        } else
            throw new RuntimeException("Unsupported type boolean:" + datum.getClass());

    }

   /**
    * As of Avro 1.5.1 this method is now in the superclass so it's no longer
    * needed here, but leaving here for backward compatibility with Avro 1.4.1.
    */
    protected NullPointerException npe(NullPointerException e, String s) {
        NullPointerException result = new NullPointerException(e.getMessage()
                                        + s);
        result.initCause(e.getCause() == null ? e : e.getCause());
        return result;
    }

  /**
     * Called to write a bytes. 
     */
    @Override
    protected void writeBytes(Object datum, org.apache.avro.io.Encoder out)
                                    throws IOException {
        if (datum instanceof DataByteArray) {
            out.writeBytes(((DataByteArray) datum).get());
        } else
            throw new RuntimeException("Unsupported type bytes:" + datum.getClass());
    }

    /**
     * Called to write a fixed value. 
     */
    @Override
    protected void writeFixed(Schema schema, Object datum,
                                    org.apache.avro.io.Encoder out)
                                    throws IOException {
        if (datum instanceof DataByteArray) {
            final byte[] bytes = ((DataByteArray) datum).get();
            out.writeFixed(bytes, 0, bytes.length);
        } else
            throw new RuntimeException("Unsupported type fixed:" + datum.getClass());

    }

    /**
     * Overriding to fetch the field value from the Tuple.
     */
    @Override
    protected void writeRecord(Schema schema, Object datum, Encoder out)
      throws IOException {
      for (Field f : schema.getFields()) {
        Object value = getField(datum, f.name(), f.pos());
        try {
          write(f.schema(), value, out);
        } catch (NullPointerException e) {
          throw npe(e, " in field "+f.name());
        }
      }
    }

    /**
     * Called by the implementation of {@link #writeRecord} to retrieve
     * a record field value.
     */
    protected Object getField(Object record, String name, int pos) {
        if (record instanceof Tuple) {
            try {
                return ((Tuple) record).get(pos);
            } catch (ExecException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } else
            throw new RuntimeException("Unsupported type in record:" + record.getClass());
    }

    /**
     * Called by the implementation of {@link #writeArray} to get the
     * size of an array.
     */
    @Override
    protected long getArraySize(Object array) {
        if (array instanceof DataBag) {
            return ((DataBag) array).size();
        } else
            throw new RuntimeException("Unsupported type in array:" + array.getClass());
    }

    /**
     * Called by the implementation of {@link #writeArray} to enumerate
     * array elements.
     */
    @Override
    protected Iterator<? extends Object> getArrayElements(Object array) {
        if (array instanceof DataBag) {
            return ((DataBag) array).iterator();
        } else
            throw new RuntimeException("Unsupported type in array:" + array.getClass());
    }


}
