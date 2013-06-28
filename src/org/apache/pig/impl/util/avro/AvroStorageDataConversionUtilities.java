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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
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
        if (AvroStorageSchemaConversionUtilities.isNullableUnion(innerSchema)) {
          if (o == null) {
            record.put(f.pos(), null);
            continue;
          }
          innerSchema = AvroStorageSchemaConversionUtilities
              .removeSimpleUnion(innerSchema);
        }
        switch(innerSchema.getType()) {
        case RECORD:
          record.put(f.pos(), packIntoAvro((Tuple) o, innerSchema));
          break;
        case ARRAY:
          record.put(f.pos(), packIntoAvro((DataBag) o, innerSchema));
          break;
        case BYTES:
          record.put(f.pos(), ByteBuffer.wrap(((DataByteArray) o).get()));
          break;
        case FIXED:
          record.put(f.pos(), new GenericData.Fixed(
              innerSchema, ((DataByteArray) o).get()));
          break;
        default:
          if (t.getType(f.pos()) == DataType.DATETIME) {
            record.put(f.pos(), ((DateTime) o).getMillis() );
          } else {
            record.put(f.pos(), o);
          }
        }
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


}
