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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Object that wraps an Avro object in a tuple.
 * @param <T> The type of the Avro object
 */
public final class AvroTupleWrapper <T extends IndexedRecord>
    implements Tuple {

  /**
   * The Avro object wrapped in the pig Tuple.
   */
  private T avroObject;

  /**
   * Creates a new AvroTupleWrapper object.
   * @param o The object to wrap
   */
  public AvroTupleWrapper(final T o) {
    avroObject = o;
  }

  @Override
  public void write(final DataOutput o) throws IOException {
    throw new IOException(
        this.getClass().toString() + ".write called, but not implemented yet");
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compareTo(final Object o) {
    if (o instanceof AvroTupleWrapper) {
      return GenericData.get().compare(avroObject,
          ((AvroTupleWrapper) o).avroObject,
          avroObject.getSchema());
    }
    return -1;
  }

  @Override
  public void append(final Object o) {
    List<Field> fields = avroObject.getSchema().getFields();
    avroObject.put(fields.size(), o);
    Schema fieldSchema = null;
    if (o instanceof String) {
      fieldSchema = Schema.create(Type.STRING);
    } else if (o instanceof Integer) {
      fieldSchema = Schema.create(Type.INT);
    } else if (o instanceof Long) {
      fieldSchema = Schema.create(Type.LONG);
    } else if (o instanceof Double) {
      fieldSchema = Schema.create(Type.DOUBLE);
    } else if (o instanceof Float) {
      fieldSchema = Schema.create(Type.FLOAT);
    } else if (o == null) {
      fieldSchema = Schema.create(Type.NULL);
    } else if (o instanceof Boolean) {
      fieldSchema = Schema.create(Type.BOOLEAN);
    } else if (o instanceof Map) {
      fieldSchema = Schema.create(Type.MAP);
    }
    Field newField = new Field("FIELD_" + fields.size(), fieldSchema, "", null);
    fields.add(newField);
    avroObject.getSchema().setFields(fields);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object get(final int pos) throws ExecException {

    Schema s = avroObject.getSchema().getFields().get(pos).schema();
    Object o = avroObject.get(pos);

    switch(s.getType()) {
      case STRING:
        // unwrap avro UTF8 encoding
        return o.toString();
      case MAP:
        return new AvroMapWrapper((Map<CharSequence, Object>) o);
      case RECORD:
        return new AvroTupleWrapper<T>((T) o);
      case ENUM:
        return o.toString();
      case ARRAY:
        return new AvroBagWrapper<GenericData.Record>(
            (GenericArray<GenericData.Record>) o);
      case FIXED:
        return new DataByteArray(((GenericData.Fixed) o).bytes());
      case BYTES:
        return new DataByteArray(((ByteBuffer) o).array());
      case UNION:
        return unionResolver(o);
      default:
        return o;
    }
  }

  public static Object unionResolver(Object o) {
    if (o instanceof org.apache.avro.util.Utf8) {
      return o.toString();
    } else if (o instanceof IndexedRecord) {
      return new AvroTupleWrapper<IndexedRecord>((IndexedRecord) o);
    } else if (o instanceof GenericArray) {
      return new AvroBagWrapper<GenericData.Record>(
          (GenericArray<GenericData.Record>) o);
    } else if (o instanceof Map) {
      return new AvroMapWrapper((Map<CharSequence, Object>) o);
    } else if (o instanceof GenericData.Fixed) {
      return new DataByteArray(((GenericData.Fixed) o).bytes());
    } else if (o instanceof ByteBuffer) {
      return new DataByteArray(((ByteBuffer) o).array());
    } else if (o instanceof GenericEnumSymbol) {
      return o.toString();
    } else {
      return o;
    }
  }

  @Override
  public List<Object> getAll() {

    List<Object> all = Lists.newArrayList();
    for (Schema.Field f : avroObject.getSchema().getFields()) {
      try {
        all.add(get(f.pos()));
      } catch (ExecException e) {
        LogFactory.getLog(getClass()).error(
            "could not process tuple with contents " + avroObject, e);
        return null;
      }
    }
    return all;
  }

  @Override
  public long getMemorySize() {
    return getMemorySize(avroObject);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private long getMemorySize(final IndexedRecord r) {
    int total = 0;
    final int bitsPerByte = 8;
    for (Field f : r.getSchema().getFields()) {
      switch (f.schema().getType()) {
      case BOOLEAN:
      case ENUM:
      case INT:
        total += Integer.SIZE << bitsPerByte;
        break;
      case DOUBLE:
        total += Double.SIZE << bitsPerByte;
        break;
      case FLOAT:
        total += Float.SIZE << bitsPerByte;
        break;
      case NULL:
        break;
      case STRING:
        total += ((String) r.get(f.pos())).length()
           * (Character.SIZE << bitsPerByte);
        break;
      case BYTES:
        total += ((Byte[]) r.get(f.pos())).length;
        break;
      case RECORD:
        total += new AvroTupleWrapper(
            (IndexedRecord) r.get(f.pos())).getMemorySize();
        break;
      case ARRAY:
        total += new AvroBagWrapper(
            (GenericArray) r.get(f.pos())).getMemorySize();
        break;
      }
    }
    return total;
  }



  @Override
  public byte getType(final int arg0) throws ExecException {
    Schema s = avroObject.getSchema().getFields().get(arg0).schema();
    return AvroStorageSchemaConversionUtilities.getPigType(s);
  }

  @Override
  public boolean isNull(final int arg0) throws ExecException {
    return avroObject == null || avroObject.get(arg0) == null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void reference(final Tuple arg0) {
    avroObject = (T) ((AvroTupleWrapper<T>) arg0).avroObject;
  }

  @Override
  public void set(final int arg0, final Object arg1) throws ExecException {
    avroObject.put(arg0, arg1);
  }

  @Override
  public int size() {
    return avroObject.getSchema().getFields().size();
  }

  @Override
  public String toDelimitedString(final String arg0) throws ExecException {
    StringBuffer delimitedString = new StringBuffer();
    boolean notfirst = false;
    for (Field f : avroObject.getSchema().getFields()) {
      if (notfirst) {
        delimitedString.append(arg0);
        notfirst = true;
      }
      Object val = avroObject.get(f.pos());
      if (val == null) {
          delimitedString.append("");
      } else {
          delimitedString.append(val.toString());
      }
    }
    return delimitedString.toString();
  }

  @Override
  public void readFields(final DataInput d) throws IOException {
    throw new IOException(
        this.getClass().toString()
        +  ".readFields called but not implemented yet");
  }

  @Override
  public Iterator<Object> iterator() {
    return Iterators.transform(avroObject.getSchema().getFields().iterator(),
        new Function<Schema.Field, Object>() {
            @Override
            public Object apply(final Field f) {
              return avroObject.get(f.pos());
            }
          }
        );
  }

}
