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
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.util.Utf8;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * An avro GenericDatumReader which reads in avro data and 
 * converts them to pig data: tuples, bags, etc.
 * 
 */
public class PigAvroDatumReader extends GenericDatumReader<Object> {

    /** 
     * Construct where the writer's and reader's schemas are the same. 
     */
    public PigAvroDatumReader(Schema schema) {
        super(schema);
    }

    /** 
     * Construct given writer's and reader's schema. 
     */
    public PigAvroDatumReader(Schema writer, Schema reader) throws IOException {
        super(writer, reader);
    }

    /**
     * Called to read a record instance. Overridden to read a pig tuple.
     */
    @Override
    protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {

        // find out the order in which we will receive fields from the ResolvingDecoder
        Field[] readOrderedFields = in.readFieldOrder();

        /* create an empty tuple */
        Tuple tuple = TupleFactory.getInstance().newTuple(readOrderedFields.length);

        /* read fields and put in output order in tuple
         * The ResolvingDecoder figures out the writer schema to reader schema mapping for us
         */
        for (Field f : readOrderedFields) {
            tuple.set(f.pos(), read(old, f.schema(), in));
        }

        return tuple;
    }

    /**
     * Called to read a map instance. Overridden to read a pig map.
     */
    protected Object readMap(Object old, Schema expected, ResolvingDecoder in) throws IOException {
        Schema eValue = expected.getValueType();
        long l = in.readMapStart();
        Object map = newMap(old, (int) l);
        if (l > 0) {
            do {
                for (int i = 0; i < l; i++) {
                    addToMap(map, readString(null, AvroStorageUtils.StringSchema, in),
                                      read(null, eValue, in));
                }
            } while ((l = in.mapNext()) > 0);
        }
        return map;
    }

    /**
     * Called to create an enum value. Overridden to create a pig string.
     */
    @Override
    protected Object createEnum(String symbol, Schema schema) {
        return symbol;
    }

    /**
     * Called by the default implementation of {@link #readArray} to retrieve a
     * value from a reused instance.
     */
    @Override
    protected Object peekArray(Object array) {
        return null;
    }

    /**
     * Called by the default implementation of {@link #readArray} to add a
     * value. Overridden to append to pig bag.
     */
    @Override
    protected void addToArray(Object array, long pos, Object e) {
        if (e instanceof Tuple) {
            ((DataBag) array).add((Tuple) e);
        } else {
            Tuple t = new DefaultTuple();
            t.append(e);
            ((DataBag) array).add(t);
        }
    }

    /**
     * Called to read a fixed value. Overridden to read a pig byte array.
     */
    @Override
    protected Object readFixed(Object old, Schema expected, Decoder in) throws IOException {
        GenericFixed fixed = (GenericFixed) super.readFixed(old, expected, in);
        DataByteArray byteArray = new DataByteArray(fixed.bytes());
        return byteArray;
     }

    /**
     * Called to create new record instances. Overridden to return a new tuple.
     */
    @Override
    protected Object newRecord(Object old, Schema schema) {
        return TupleFactory.getInstance().newTuple();
    }

    /**
     * Called to create new array instances. Overridden to return a new bag.
     */
    @Override
    protected Object newArray(Object old, int size, Schema schema) {
        return BagFactory.getInstance().newDefaultBag();
    }

    /**
     * Called to read strings. Overridden to return a pig string.
     */
    @Override
    protected Object readString(Object old, Schema expected, Decoder in) throws IOException {
        return super.readString(old, expected, in).toString();
    }

    /**
     * Called to read byte arrays. Overridden to return a pig byte array.
     */
    @Override
    protected Object readBytes(Object old, Decoder in) throws IOException {
        ByteBuffer buf = (ByteBuffer) super.readBytes(old, in);
        DataByteArray byteArray = new DataByteArray(buf.array());
        return byteArray;
    }

}
