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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Utility methods manipulating Table types (speicifically, Tuple objects).
 */
public class TypesUtils {
  static TupleFactory tf = DefaultTupleFactory.getInstance();

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

  //TODO - sync up with yan about this change
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
      for (int i = 0; i < tuple.size(); ++i) {
        tuple.set(i, null);
      }
    }
    catch (Exception e) {
      throw new RuntimeException("Internal error: " + e.toString());
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
  public static void checkCompatible(Tuple tuple, Schema schema)
      throws IOException {
    // TODO: Add more rigorous checking.
    if (tuple.size() != schema.getNumColumns()) {
      throw new IOException("Incompatible Tuple object - number of fields");
    }
  }

  /**
   * Reading a tuple from disk with projection.
   */
  public static class TupleReader {
    private Tuple tuple;
    @SuppressWarnings("unused")
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
      checkCompatible(row, projection.getSchema());
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
      checkCompatible(row, physical);
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
  public static void formatTuple(Tuple tuple, String projection) throws IOException {
    Tuple one = createTuple(Projection.getNumColumns(projection));
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
