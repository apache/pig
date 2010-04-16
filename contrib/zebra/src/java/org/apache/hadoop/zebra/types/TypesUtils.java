/**
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
package org.apache.hadoop.zebra.types;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.schema.Schema.ColumnSchema;
import org.apache.hadoop.zebra.parser.ParseException;

/**
 * Utility methods manipulating Table types (specifically, Tuple objects).
 */
public class TypesUtils {
  //static TupleFactory tf = ZebraTupleFactory.getInstance();
  static TupleFactory tf = ZebraTupleFactory.getZebraTupleFactoryInstance();

  /**
   * Create a tuple based on a schema
   * 
   * @param schema
   *          The schema that the tuple will conform to.
   * @return A suitable Tuple object that can be used to read or write a Table
   *         with the same input or output schema.
   */
  public static Tuple createTuple(Schema schema) throws IOException {
    Tuple tuple = tf.newTuple(schema.getNumColumns());
    for (int i = 0; i < schema.getNumColumns(); ++i) {
      tuple.set(i, null);
    }
    return tuple;
  }
  
  /**
   * create a tuple based on number of columns
   */
  public static Tuple createTuple(int size) throws IOException {
    Tuple tuple = tf.newTuple(size);
    for (int i = 0; i < size; ++i) {
      tuple.set(i, null);
    }
    return tuple;
  }

  /**
   * Create a PIG Bag object.
   * 
   * @return A Pig DataBag object.
   */
  public static DataBag createBag() {
    return new DefaultDataBag();
  }

  public static DataBag createBag(Schema schema) {
    return new DefaultDataBag();
  }

  /**
   * Reset the Tuple so that all fields are NULL field. This is different from
   * clearing the tuple, in which case the size of the tuple will become zero.
   * 
   * @param tuple
   *          Input tuple.
   */
  public static void resetTuple(Tuple tuple) {
    try {
      int tupleSize = tuple.size();
      for (int i = 0; i < tupleSize; ++i) {
        tuple.set(i, null);
      }
    }
    catch (Exception e) {
      throw new RuntimeException("Internal error: " + e.toString());
    }
  }
  
  private static void checkTypeError(ColumnSchema cs, ColumnType type) throws IOException {
    throw new IOException("Incompatible Tuple object - datum is " + type + ", but schema says " + cs.getType());
  }
  
  private static void checkColumnType(ColumnSchema cs, ColumnType type) throws IOException {
    switch (type) {
      case BOOL:
      case DOUBLE:
      case STRING:
      case BYTES:
      case MAP:
      case COLLECTION:
      case RECORD:
        if (cs.getType() != type) {
          checkTypeError(cs, type);
        }
        break;
      case FLOAT:
        if (cs.getType() != ColumnType.FLOAT && cs.getType() != ColumnType.DOUBLE) {
          checkTypeError(cs, type);
        }
        break;
      case LONG:
        if (cs.getType() != ColumnType.LONG && cs.getType() != ColumnType.FLOAT && cs.getType() != ColumnType.DOUBLE) {
          checkTypeError(cs, type);
        }
        break;
      case INT:
        if (cs.getType() != ColumnType.INT && cs.getType() != ColumnType.LONG && cs.getType() != ColumnType.FLOAT && cs.getType() != ColumnType.DOUBLE) {
          checkTypeError(cs, type);
        }
        break;
    }
  }
  
  @SuppressWarnings("unchecked")
  private static void checkColumn(Object d, ColumnSchema cs) throws IOException {
    if (d instanceof Boolean) {
      checkColumnType(cs, ColumnType.BOOL);   
    } else if (d instanceof Integer) {
      checkColumnType(cs, ColumnType.INT);
    } else if (d instanceof Long) {
      checkColumnType(cs, ColumnType.LONG);
    } else if (d instanceof Float) {
      checkColumnType(cs, ColumnType.FLOAT); 
    } else if (d instanceof Double) {
      checkColumnType(cs, ColumnType.DOUBLE);
    } else if (d instanceof String) {
      checkColumnType(cs, ColumnType.STRING);
    } else if (d instanceof DataByteArray) {
      checkColumnType(cs, ColumnType.BYTES);
    } else if (d instanceof Map) {
      checkMapColumn((Map<String, Object>)d, cs);
    } else if (d instanceof DataBag) {
      checkCollectionColumn((DataBag)d, cs);
    } else if (d instanceof Tuple) {
      checkRecordColumn((Tuple)d, cs);
    } else {
      throw new IOException("Unknown data type");
    }
  }
    
  private static void checkMapColumn(Map<String, Object> m, ColumnSchema cs) throws IOException {
    checkColumnType(cs, ColumnType.MAP);
    Schema schema = cs.getSchema();
    Assert.assertTrue(schema.getNumColumns() == 1);
    
    ColumnSchema tempColumnSchema = schema.getColumn(0);
    if (tempColumnSchema.getType() == ColumnType.BYTES) { // We do not check inside of map if its value type is BYTES;
                                            // This is for Pig, since it only supports BYTES as map value type.
      return;
    }
        
    Map<String, Object> m1 = (Map<String, Object>)m;
    for (Map.Entry<String, Object> e : m1.entrySet()) {
      Object d = e.getValue();
      if (d != null) {
        checkColumn(d, tempColumnSchema);
        return;   // We only check the first non-null map value in the map;
      }
    }
  }
  
  private static void checkCollectionColumn(DataBag bag, ColumnSchema cs) throws IOException {
    checkColumnType(cs, ColumnType.COLLECTION);
    Schema schema = cs.getSchema();
    Assert.assertTrue(schema.getNumColumns() == 1);
    
    Iterator<Tuple> iter = bag.iterator();
    while (iter.hasNext()) {
      Tuple tempTuple = iter.next();
      // collection has to be on record;
      if (tempTuple != null) {
        checkRecordColumn(tempTuple, schema.getColumn(0));
        return;     // We only check the first non-null record in the collection;
      }
    }     
  }
  
  private static void checkRecordColumn(Tuple d, ColumnSchema cs) throws IOException {
    checkColumnType(cs, ColumnType.RECORD);
    checkNumberColumnCompatible(d, cs.getSchema());
    
    for (int i=0; i<d.size(); i++) {
      if (d.get(i) != null) { // "null" can match any type;
        checkColumn(d.get(i), cs.getSchema().getColumn(i));
      }  
    }
  }
  
  /**
   * Check whether the input row object is compatible with the expected schema
   * 
   * @param tuple
   *          Input Tuple object
   * @param schema
   *          Table schema
   * @throws IOException
   */  
  public static void checkCompatible(Tuple tuple, Schema schema) throws IOException {
    // Create a dummy record ColumnSchema since we do not have it;
    ColumnSchema dummy = new ColumnSchema("dummy", schema);

    checkRecordColumn(tuple, dummy);
  } 
  
  /**
   * Check whether the input row object is compatible with the expected schema
   * on number of Columns;
   * @param tuple
   *          Input Tuple object
   * @param schema
   *          Table schema
   * @throws IOException
   */
  public static void checkNumberColumnCompatible(Tuple tuple, Schema schema)
      throws IOException {
    if (tuple.size() != schema.getNumColumns()) {
      throw new IOException("Incompatible Tuple object - tuple has " + tuple.size() + " columns, but schema says " + schema.getNumColumns() + " columns");
    }
  }

  /**
   * Reading a tuple from disk with projection.
   */
  public static class TupleReader {
    private Tuple tuple;
    //@SuppressWarnings("unused")
    private Schema physical;
    private Projection projection;
    SubColumnExtraction.SubColumn subcolextractor = null;

    /**
     * Constructor - create a TupleReader than can parse the serialized Tuple
     * with the specified physical schema, and produce the Tuples based on the
     * projection.
     * 
     * @param physical
     *          The physical schema of on-disk data.
     * @param projection
     *          The logical schema of tuples user expect.
     */
    public TupleReader(Schema physical, Projection projection)
        throws IOException, ParseException {
      tuple = createTuple(physical);
      this.physical = physical;
      this.projection = projection;
      subcolextractor = new SubColumnExtraction.SubColumn(physical, projection);
      subcolextractor.dispatchSource(tuple);
    }

    public Schema getSchema() {
      return physical;
    }

    public Projection getprojction() {
      return projection;
    }

    /**
     * Read a tuple from the stream, and perform projection.
     * 
     * @param in
     *          The input stream
     * @param row
     *          The input tuple that should conform to the projection schema.
     * @throws IOException
     */
    public void get(DataInputStream in, Tuple row) throws IOException, ParseException {
      checkNumberColumnCompatible(row, projection.getSchema());
      tuple.readFields(in);
      TypesUtils.resetTuple(row);
      try {
        subcolextractor.splitColumns(row);
      }
      catch (ExecException e) {
        // not going to happen.
      }
    }
  }

  /**
   * Writing a tuple to disk.
   */
  public static class TupleWriter {
    private Schema physical;

    /**
     * The constructor
     * 
     * @param physical
     *          The physical schema of the tuple.
     */
    public TupleWriter(Schema physical) {
      this.physical = physical;
    }

    /**
     * Write a tuple to the output stream.
     * 
     * @param out
     *          The output stream
     * @param row
     *          The user tuple that should conform to the physical schema.
     * @throws IOException
     */
    public void put(DataOutputStream out, Tuple row) throws IOException {
      row.write(out);
    }
  }

  /**
   * Checking and formatting an input tuple to conform to the input schema.<br>
   * 
   *           The current implementation always create a new tuple because PIG
   *           expects Slice.next(tuple) always returning a brand new tuple.
   * 
   * @param tuple
   * @throws IOException
   * 
   */
  public static void formatTuple(Tuple tuple, int ncols) throws IOException {
    Tuple one = createTuple(ncols);
    tuple.reference(one);
    return;
    /*
     * Dead code below.
     */
    // int n = schema.getNumColumns();
    // if (tuple.size() == n) return;
    // if (tuple.size() == 0) {
    // for (int i = 0; i < schema.getNumColumns(); ++i) {
    // tuple.append(null);
    // }
    // return;
    // }
    // throw new IOException("Tuple already formatted with " + tuple.size()
    // + "  fields");
  }
}
