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

import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.ParseException;

/**
 * Projection for Table and Column Group
 */

public class Projection {
  public static final String source_table_vcolumn_name = "source_table";
  private Schema mProjection; // schema as needed by the projection
  private int mNumColumns;
  private String mProjStr;
  //TODO: change name and comment - to yan
  private Schema mSchema; // the logical schema literally from projection string

  /* the maps from the column in projection to a set of map keys
   * the projection is interested. If null, all keys in schema are interested
   */
  private HashMap<Schema.ColumnSchema, HashSet<String>> mKeys;

  /**
   * ctor for full projection
   */
  public Projection(Schema s) {
    mProjection = s;
    if (s != null)
    {
      mNumColumns = s.getNumColumns();
      mProjStr = s.toString();
      mSchema = s;
    }
  }

  /**
   * if a column name is on a virtual column
   */
  public static boolean isVirtualColumn(String name)
  {
    if (name == null || name.isEmpty())
      return false;
    return name.trim().equalsIgnoreCase(source_table_vcolumn_name);
  }

  /**
   * Get the indices of all virtual columns
   */
  public static Integer[] getVirtualColumnIndices(String projection)
  {
    if (projection == null)
      return null;
    String[] colnames = projection.trim().split(Schema.COLUMN_DELIMITER);
    int size = colnames.length, realsize = 0;
    ArrayList<Integer> vcol = new ArrayList();
    
    for (int i = 0; i < size; i++)
    {
      if (Projection.isVirtualColumn(colnames[i]))
      {
        vcol.add(i);
      }
    }
    Integer[] result = null;
    if (!vcol.isEmpty())
    {
      result = new Integer[vcol.size()];
      vcol.toArray(result);
    }
    return result;
  }
  
  /**
   * ctor for partial projection
   */
  public Projection(Schema s, String projection) throws ParseException
  {
    String[] colnames = projection.trim().split(Schema.COLUMN_DELIMITER);
    for (int nx = 0; nx < colnames.length; nx++)
    {
      colnames[nx] = colnames[nx].trim();
    }
    mKeys = new HashMap<Schema.ColumnSchema, HashSet<String>>();
    mProjection = s.getProjectionSchema(colnames, mKeys);
    mNumColumns = colnames.length;
    mProjStr = projection;
    mSchema = toSchema(projection);
  }

  /**
   * accessor to the map keys
   */
  HashMap<Schema.ColumnSchema, HashSet<String>> getKeys() {
    return mKeys;
  }

  /**
   * accessor to the projected schema including invalid columns
   */
  public Schema getSchema() {
     return mSchema;
  }
  
  /**
   *
   */
  public Schema getProjectionSchema()
  {
    return mProjection;
  }

  /**
   * Get a particular projected column's schema
   */
  public Schema.ColumnSchema getColumnSchema(int i)
  {
    return mProjection.getColumn(i);
  }
  
  /**
   * Get the string representation
   */
  public String toString() {
    return mProjStr;
  }
  
  /**
   * Get number of columns in the projection
   */
  public int getNumColumns() {
    return mNumColumns;
  }
  
  /**
   * Get number of columns from a projection string
   */
  public static int getNumColumns(String projection) {
    return projection.trim().split(Schema.COLUMN_DELIMITER).length;
  }
  
  /**
   * Get a projection string from a series of column names
   */
  public static String getProjectionStr(String[] names) {
    if (names == null)
      return null;
    String result = new String();
    for (String name :names)
    {
      if (!result.isEmpty())
        result += ",";
      result += name;
    }
    return result;
  }
  
  /**
   * Get schema from a projection string: all map keys are lost
   */
  public static Schema toSchema(String projection) throws ParseException
  {
    String[] colnames = projection.trim().split(Schema.COLUMN_DELIMITER);
    String schemaStr = new String();
    int startidx, i = 0;
    for (String name : colnames)
    {
      startidx = name.indexOf("#{");
      if (startidx != -1)
      {
        if (!name.endsWith("}"))
          throw new ParseException("Invalid projection");
        startidx += 2;
        if (name.substring(startidx, name.length()-1).indexOf("#") != -1 ||
          name.substring(startidx, name.length()-1).indexOf("{") != -1 ||
          name.substring(startidx, name.length()-1).indexOf("}") != -1)
          throw new ParseException("Invalid projection");
        name = name.substring(0, startidx-2);
      } else {
        if (name.indexOf("#") != -1 || name.indexOf("{") != -1 ||
            name.indexOf("}") != -1)
          throw new ParseException("Invalid projection");
      }
      if (i++ > 0)
        schemaStr += ",";
      schemaStr += name;
    }
    return new Schema(schemaStr, true);
  }
  
  /**
   * Get a column's index in a projection. 
   * @param projection
   * @param colname
   * @return -1 if the column is not found in projection
   */
  public static int getColumnIndex(String projection, String colname)
  {
    if (projection == null || colname == null)
      return -1;
    String[] colnames = projection.trim().split(Schema.COLUMN_DELIMITER);;
    for (int i = 0; i < colnames.length; i++)
    {
      if (colnames[i].equals(colname))
        return i;
    }
    return -1;
  }
}
