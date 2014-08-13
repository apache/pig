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
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Class that implements the Pig bag interface, wrapping an Avro array.
 * Designed to reduce data copying.
 * @param <T> Type of objects in Avro array
 */
public final class AvroBagWrapper<T> implements DataBag {

  private static final long serialVersionUID = 1L;

  /**
   * The array object wrapped in this AvroBagWrapper object.
   */
  private GenericArray<T> theArray;

  /**
   * Create new AvroBagWrapper instance.
   * @param a Avro array to wrap in bag
   */
  public AvroBagWrapper(final GenericArray<T> a) { theArray = a; }

  @Override
  public long spill() { return 0; }

  @Override
  public long getMemorySize() {
    return 0;
  }

  @Override
  public void readFields(final DataInput d) throws IOException {
    throw new IOException(
        this.getClass().toString() + ".readFields not implemented yet");
  }

  @Override
  public void write(final DataOutput d) throws IOException {
    throw new IOException(
        this.getClass().toString() + ".write not implemented yet");
  }

  @Override
  public int compareTo(final Object o) {
    if (this == o) return 0;

    if (o instanceof AvroBagWrapper) {
      @SuppressWarnings("rawtypes")
      AvroBagWrapper bOther = (AvroBagWrapper) o;
      return GenericData.get().compare(theArray, bOther.theArray, theArray.getSchema());
    } else {
      return DataType.compare(this, o);
    }
  }

  @Override public long size() { return theArray.size(); }
  @Override public boolean isSorted() { return false; }
  @Override public boolean isDistinct() { return false; }

  @Override
  public Iterator<Tuple> iterator() {
    return Iterators.transform(theArray.iterator(),
        new Function<T, Tuple>() {
            @Override
            public Tuple apply(final T arg) {
              if (arg instanceof IndexedRecord) {
                return new AvroTupleWrapper<IndexedRecord>((IndexedRecord) arg);
              } else {
                return TupleFactory.getInstance().newTuple(AvroTupleWrapper.unionResolver(arg));
              }
            }
          }
        );
  }

  @SuppressWarnings("unchecked")
  @Override public void add(final Tuple t) { theArray.add((T) t); }

  @Override
  public void addAll(final DataBag b) {
    for (Tuple t : b) {
      add(t);
    }
  }

  @Override public void clear() { theArray.clear(); }
  @Override public void markStale(final boolean stale) { }

}
