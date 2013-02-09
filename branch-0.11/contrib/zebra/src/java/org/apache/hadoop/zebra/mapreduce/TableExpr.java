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
package org.apache.hadoop.zebra.mapreduce;

import java.io.IOException;
import java.io.StringReader;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;

/**
 * Table Expression - expression to describe an input table.
 */
abstract class TableExpr {
  private boolean sorted = false;
  /**
   * Factory method to create a TableExpr from a string.
   * 
   * @param in
   *          The string stream that is pointed at the beginning of the table
   *          expression string to be parsed.
   * @return The instantiated TableExpr object. The string stream will move past
   *         the table expression string.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static TableExpr parse(StringReader in) throws IOException {
    String clsName = TableExprUtils.decodeString(in);
    try {
      Class<? extends TableExpr> tblExprCls =
          (Class<? extends TableExpr>) Class.forName(clsName);
      return tblExprCls.newInstance().decodeParam(in);
    }
    catch (Exception e) {
      throw new RuntimeException("Failed to load class: " + e.toString());
    }
  }

  /**
   * Encode the expression to a string stream.
   * 
   * @param out
   *          The string stream that we should encode the expression into.
   */
  public final void encode(StringBuilder out) {
    TableExprUtils.encodeString(out, getClass().getName());
    encodeParam(out);
  }

  /**
   * Decode the parameters of the expression from the input stream.
   * 
   * @param in
   *          The string stream that is pointed at the beginning of the encoded
   *          parameters.
   * @return Reference to itself, and the TableExpr that has its parameters set
   *         from the string stream. The string stream will move past the
   *         encoded parameters for this expression.
   */
  protected abstract TableExpr decodeParam(StringReader in) throws IOException;

  /**
   * Encode the parameters of the expression into the string stream.
   * 
   * @param out
   *          The string stream that we write encoded parameters into.
   * @return Reference to itself,
   */
  protected abstract TableExpr encodeParam(StringBuilder out);

  /**
   * Get a TableScanner from the TableExpr object. This method only needs to be
   * implemented by table expressions that support sorted split.
   * 
   * @param begin
   *          the Begin key (inclusive). Can be null, meaning starting from the
   *          first row of the table.
   * @param end
   *          the End key (exclusive). Can be null, meaning scan to the last row
   *          of the table.
   * @param projection
   *          The projection schema. It should never be null.
   * @see Schema
   * @return A TableScanner object.
   */
  public TableScanner getScanner(BytesWritable begin,
      BytesWritable end, String projection, Configuration conf)
      throws IOException {
    return null;
  }

  /**
   * Get a scanner with an row split.
   * 
   * @param split
   *          The row split.
   * @param projection
   *          The projection schema. It should never be null.
   * @param conf
   *          The configuration
   * @return A table scanner.
   * @throws IOException
   */
  public TableScanner getScanner(RowTableSplit split, String projection,
      Configuration conf) throws IOException, ParseException {
    return null;
  }
  
  /**
   * A leaf table corresponds to a materialized table. It is represented by the
   * path to the BasicTable and the projection.
   */
  public static final class LeafTableInfo {
    private final Path path;
    private final String projection;

    public LeafTableInfo(Path path, String projection) {
      this.path = path;
      this.projection = projection;
    }

    public Path getPath() {
      return path;
    }

    public String getProjection() {
      return projection;
    }
  }

  /**
   * Get the information of all leaf tables that will be accessed by this table
   * expression.
   * 
   * @param projection
   *          The projection that is applied to the table expression.
   */
  public abstract List<LeafTableInfo> getLeafTables(
      String projection);

  /**
   * Get the schema of the table.
   * 
   * @param conf
   *          The configuration object.
   */
  public abstract Schema getSchema(Configuration conf) throws IOException;

  /**
   * Does this expression requires sorted split? If yes, we require all
   * underlying BasicTables to be sorted and we split by key sampling. If this
   * method returns true, we expect sortedSplitCapable() also return true.
   * 
   * @return Whether this expression may only be split by key.
   */
  public boolean sortedSplitRequired() {
    return sorted;
  }

  /**
   * Set the requirement for sorted table
   */
  public void setSortedSplit() {
    sorted = true;
  }

  /**
   * Is this expression capable of sorted split? If false, getScanner() should
   * return null; otherwise, getScanner() should return a valid Scanner object.
   * 
   * This function should be overridden by sub classes that is capable of sorted
   * split. Note that this method should not perform any actual I/O operation,
   * such as checking whether the leaf tables (BasicTables) is in fact sorted or
   * not. When this method returns true, while at least one of the leaf tables
   * is not sorted, an {@link IOException} will be thrown in split generation
   * time.
   * 
   * @return Whether the "table view" represented by the expression is sorted
   *         and is thus splittable by key (sorted split).
   */
  public boolean sortedSplitCapable() {
    return false;
  }

  /**
   * dump table info
   */
  public final void dumpInfo(PrintStream ps, Configuration conf) throws IOException
  {
    dumpInfo(ps, conf, 0);
  }
  
  /**
   * dump table info with indent
   */
  protected abstract void dumpInfo(PrintStream ps, Configuration conf, int indent) throws IOException;

  /**
   * get the deleted cg for tables in union
   * @param conf The Configuration object
   * @return
   */
  protected final String[] getDeletedCGsPerUnion(Configuration conf) {
    return getDeletedCGs(conf, TableInputFormat.DELETED_CG_SEPARATOR_PER_UNION);
  }
  
  protected final String[] getDeletedCGs(Configuration conf) {
    return getDeletedCGs(conf, BasicTable.DELETED_CG_SEPARATOR_PER_TABLE);
  }
  
  private final String[] getDeletedCGs(Configuration conf, String separator) {
    String[] deletedCGs = null;
    String fe;
    if ((fe = conf.get(TableInputFormat.INPUT_FE)) != null && fe.equals("true"))
    {
      String original = conf.get(TableInputFormat.INPUT_DELETED_CGS, null);
      if (original == null)
        deletedCGs = new String[0]; // empty array needed to indicate it is fe checked
      else
        deletedCGs = original.split(separator, -1);
    }
    return deletedCGs;
  }
}
