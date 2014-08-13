/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.scripting.groovy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class GroovyUtils {

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  private static final BagFactory bagFactory = BagFactory.getInstance();

  /**
   * Converts an object created on the Groovy side to its Pig counterpart.
   *
   * The conversions are as follow:
   *
   * Groovy Pig
   * Object[] Tuple
   * groovy.lang.Tuple Tuple
   * org.apache.pig.data.Tuple Tuple
   * org.apache.pig.data.DataBag DataBag
   * java.util.Map Map
   * java.util.List DataBag
   * Byte/Short/Integer int
   * Long long
   * Float float
   * Double double
   * BigInteger BigInteger
   * BigDecimal BigDecimal
   * String chararray
   * byte[] DataByteArray (copy)
   * Boolean boolean
   * org.joda.time.DateTime org.joda.time.DateTime
   * null null
   *
   * anything else raises an exception
   *
   * @param groovyObject
   *          Groovy object to convert
   * @return the Pig counterpart of groovyObject
   *
   * @throws ExecException
   */
  public static Object groovyToPig(Object groovyObject) throws ExecException {
    Object pigObject = null;

    if (groovyObject instanceof Object[] || groovyObject instanceof groovy.lang.Tuple) {
      //
      // Allocate a List<Object> that will be filled with converted
      // objects and later passed to newTuple.
      //

      List<Object> pigObjects = new ArrayList<Object>();

      //
      // Convert each member of groovyObject
      //

      if (groovyObject instanceof Object[]) {
        for (Object o : (Object[]) groovyObject) {
          pigObjects.add(groovyToPig(o));
        }
      } else {
        for (Object o : (Iterable) groovyObject) {
          pigObjects.add(groovyToPig(o));
        }
      }

      //
      // Create the result Tuple
      //

      pigObject = tupleFactory.newTuple(pigObjects);
    } else if (groovyObject instanceof Tuple || groovyObject instanceof DataBag) {
      //
      // Copy Pig Tuple/DataBag as is
      // This enables the creation of instances of DataBag which do not fit in
      // memory
      //
      // It is advised to wrap objects into a call to groovyToPig
      // prior to adding them to a Tuple or DataBag
      //

      pigObject = groovyObject;
    } else if (groovyObject instanceof Map) {
      //
      // Allocate a Map
      //

      Map<String, Object> pigMap = new HashMap<String, Object>();

      //
      // Iterate over Groovy Map, putting each entry into pigMap
      //

      for (Map.Entry<?, ?> entry : ((Map<?, ?>) groovyObject).entrySet()) {
        pigMap.put(groovyToPig(entry.getKey()).toString(), groovyToPig(entry.getValue()));
      }

      pigObject = pigMap;
    } else if (groovyObject instanceof List) {
      //
      // Allocate a DataBag
      //

      DataBag bag = bagFactory.newDefaultBag();

      //
      // Pig's bags can only contain tuples, so we cast the return value
      // of groovyToPig to a Tuple, if it's not a tuple, a ClassCastException
      // will
      // be thrown.
      //

      for (Object o : (List) groovyObject) {
        Object p = groovyToPig(o);

        if (p instanceof Tuple || null == p) {
          bag.add((Tuple) p);
        } else {
          // Wrap value in a Tuple if it's not already a tuple
          bag.add(tupleFactory.newTuple(p));
        }
      }

      pigObject = bag;
    } else if (groovyObject instanceof Integer || groovyObject instanceof Long || groovyObject instanceof Float
        || groovyObject instanceof Double) {
      //
      // Numeric types which have an equivalent in Pig are passed as is as they
      // are immutable
      //
      pigObject = groovyObject;
    } else if (groovyObject instanceof Byte || groovyObject instanceof Short) {
      pigObject = ((Number) groovyObject).intValue();
    } else if (groovyObject instanceof BigInteger) {
      pigObject = groovyObject;
    } else if (groovyObject instanceof BigDecimal) {
      pigObject = groovyObject;
    } else if (groovyObject instanceof byte[]) {
      //
      // Clone the byte array
      //

      byte[] b = new byte[((byte[]) groovyObject).length];
      System.arraycopy((byte[]) groovyObject, 0, b, 0, b.length);

      pigObject = new DataByteArray(b);
    } else if (groovyObject instanceof String) {
      //
      // String is immutable, so pass it as is
      //

      pigObject = groovyObject;
    } else if (groovyObject instanceof Boolean) {
      pigObject = groovyObject;
    } else if (groovyObject instanceof org.joda.time.DateTime) {
      //
      // jodatime's DateTime is immutable, so reuse the same instance
      //
      pigObject = groovyObject;
    } else if (null == groovyObject) {
      pigObject = null;
    } else {
      throw new ExecException("Unable to cast " + groovyObject.getClass().getName() + " to a Pig datatype.");
    }

    return pigObject;
  }

  /**
   * Converts an object created on the Pig side to its Groovy counterpart.
   *
   * The conversions are as follow:
   *
   * Pig Groovy
   * Tuple groovy.lang.tuple
   * DataBag groovy.lang.Tuple containing the bag's size and an iterator on its
   * content
   * Map java.util.Map
   * int/long/float/double as is
   * chararray String
   * bytearray byte[] (copy)
   * boolean boolean
   * BigInteger BigInteger
   * BigDecimal BigDecimal
   * org.joda.time.DateTime org.joda.time.DateTime
   * null null
   *
   * anything else raises an exception
   *
   * @param pigObject
   * @return Object
   * @throws ExecException
   */
  public static Object pigToGroovy(Object pigObject) throws ExecException {

    Object groovyObject = null;

    if (pigObject instanceof Tuple) {
      Object[] a = new Object[((Tuple) pigObject).size()];

      int i = 0;
      for (Object oo : ((Tuple) pigObject).getAll()) {
        a[i++] = pigToGroovy(oo);
      }

      groovyObject = new groovy.lang.Tuple(a);
    } else if (pigObject instanceof DataBag) {
      //
      // Return a Groovy Tuple containing the bag's size and an
      // iterator on its content (Iterator will return instances of
      // groovy.lang.Tuple)
      //

      Object[] size_iterator = new Object[2];
      size_iterator[0] = ((DataBag) pigObject).size();
      size_iterator[1] = new DataBagGroovyIterator(((DataBag) pigObject).iterator());
      groovyObject = new groovy.lang.Tuple(size_iterator);
    } else if (pigObject instanceof Map) {
      Map<String, Object> m = new HashMap<String, Object>();

      for (Map.Entry<String, ?> entry : ((Map<String, ?>) pigObject).entrySet()) {
        m.put((String) pigToGroovy(entry.getKey()), pigToGroovy(entry.getValue()));
      }

      groovyObject = m;
    } else if (pigObject instanceof Number || pigObject instanceof String || pigObject instanceof Boolean) {
      groovyObject = pigObject;
    } else if (pigObject instanceof DataByteArray) {
      //
      // Allocate a new byte array so we don't use the original array
      //
      byte[] b = new byte[((DataByteArray) pigObject).size()];

      System.arraycopy(((DataByteArray) pigObject).get(), 0, b, 0, b.length);

      groovyObject = b;
    } else if (pigObject instanceof BigInteger) {
      groovyObject = pigObject;
    } else if (pigObject instanceof BigDecimal) {
      groovyObject = pigObject;
    }else if (pigObject instanceof org.joda.time.DateTime) {
      //
      // jodatime's DateTime is immutable, so reuse the same instance
      //
      groovyObject = pigObject;
    } else if (null == pigObject) {
      groovyObject = null;
    } else {
      throw new ExecException("Unable to cast pig datatype " + pigObject.getClass().getName() + " to a suitable Groovy Object.");
    }

    return groovyObject;
  }

  public static class DataBagGroovyIterator implements Iterator<groovy.lang.Tuple> {

    private final Iterator<Tuple> iter;

    public DataBagGroovyIterator(Iterator<Tuple> iter) {
      this.iter = iter;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public groovy.lang.Tuple next() {
      try {
        return (groovy.lang.Tuple) pigToGroovy(iter.next());
      } catch (ExecException ee) {
        throw new RuntimeException(ee);
      }
    }

    @Override
    public void remove() {
    }
  }
}
