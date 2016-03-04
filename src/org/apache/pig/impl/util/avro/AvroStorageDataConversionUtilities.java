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


package org.apache.pig.impl.util.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;

/**
 * Utility classes for AvroStorage; contains static methods
 * for converting between Avro and Pig objects.
 *
 */
public class AvroStorageDataConversionUtilities {

  /**
   * Packs a Pig Tuple into an Avro record.
   * @param t the Pig tuple to pack into the avro object
   * @param s The avro schema for which to determine the type
   * @return the avro record corresponding to the input tuple
   * @throws IOException
   */
  public static GenericData.Record packIntoAvro(final Tuple t, final Schema s)
      throws IOException {

    try {
      GenericData.Record record = new GenericData.Record(s);
      for (Field f : s.getFields()) {
        Object o = t.get(f.pos());
        Schema innerSchema = f.schema();
        record.put(f.pos(), packIntoAvro(o, innerSchema));
      }
      return record;
    } catch (Exception e) {
      throw new IOException(
          "exception in AvroStorageDataConversionUtilities.packIntoAvro", e);
    }
  }

  /**
   * Packs a Pig DataBag into an Avro array.
   * @param db the Pig databad to pack into the avro array
   * @param s The avro schema for which to determine the type
   * @return the avro array corresponding to the input bag
   * @throws IOException
   */
  public static GenericData.Array<Object> packIntoAvro(
      final DataBag db, final Schema s) throws IOException {

    try {
      GenericData.Array<Object> array
        = new GenericData.Array<Object>(new Long(db.size()).intValue(), s);
      for (Tuple t : db) {
        if (s.getElementType() != null
            && s.getElementType().getType() == Type.RECORD) {
          array.add(packIntoAvro(t, s.getElementType()));
        } else if (t.size() == 1) {
          array.add(t.get(0));
        } else {
          throw new IOException(
              "AvroStorageDataConversionUtilities.packIntoAvro: Can't pack "
                  + t + " into schema " + s);
        }
      }
      return array;
    } catch (Exception e) {
      throw new IOException(
          "exception in AvroStorageDataConversionUtilities.packIntoAvro", e);
    }
  }

  private static Object packIntoAvro(final Object o, Schema s)
      throws IOException {
    if (AvroStorageSchemaConversionUtilities.isNullableUnion(s)) {
      if (o == null) {
        return null;
      }
      s = AvroStorageSchemaConversionUtilities.removeSimpleUnion(s);
    }
    // what if o == null and schema doesn't allow it ?
    switch (s.getType()) {
      case RECORD:
        return packIntoAvro((Tuple) o, s);
      case ARRAY:
        return packIntoAvro((DataBag) o, s);
      case MAP:
        return packIntoAvro((Map<CharSequence, Object>) o, s);
      case BYTES:
        return ByteBuffer.wrap(((DataByteArray) o).get());
      case FIXED:
        return new GenericData.Fixed(s, ((DataByteArray) o).get());
      default:
        if (DataType.findType(o) == DataType.DATETIME) {
          return ((DateTime) o).getMillis();
        } else {
          return o;
        }
    }
  }

  private static Map<Utf8, Object> packIntoAvro(Map<CharSequence, Object> input, Schema schema)
      throws IOException {
    final Map<Utf8, Object> output = new HashMap<Utf8, Object>();
    for (Map.Entry<CharSequence, Object> e : input.entrySet()) {
      final Utf8 k = utf8(e.getKey());
      output.put(k, packIntoAvro(e.getValue(), schema.getValueType()));
    }
    return output;
  }

  private static Utf8 utf8(CharSequence v) {
    if (v instanceof Utf8) {
      return (Utf8) v;
    } else {
      final StringBuilder sb = new StringBuilder(v.length());
      sb.append(v);
      return new Utf8(sb.toString());
    }
  }
}
