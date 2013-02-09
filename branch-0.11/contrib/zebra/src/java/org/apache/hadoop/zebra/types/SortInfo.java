/**
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

package org.apache.hadoop.zebra.types;

import java.io.IOException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.hadoop.zebra.tfile.TFile;

/**
 * Sortness related Information
 */
public class SortInfo {
  public static final String DEFAULT_COMPARATOR =  TFile.COMPARATOR_MEMCMP;
  private boolean global = true;
  private String[] columns;
  private int[] indices = null;
  private ColumnType[] types = null;
  private String comparator = null;
  private Schema schema = null;
  public static final String SORTED_COLUMN_DELIMITER = ",";

  private SortInfo(String[] columns, int[] sortIndices, ColumnType[] sortColTypes, String comparator, Schema schema)
  {
    this.columns = columns;
    this.indices = sortIndices;
    this.comparator = comparator;
    this.schema = schema;
    this.types = sortColTypes;
  }

  /**
   * Get an array of the sorted column names with the first column
   * being the primary sort key, the second column being the
   * secondary sort key, ..., etc.
   *
   * @return an array of strings of sorted column names
   */
  public String[] getSortColumnNames() {
    return columns;
  }
  
  /**
   * Get an array of zebra types of the sorted columns with the first column
   * being the primary sort key, the second column being the
   * secondary sort key, ..., etc.
   *
   * @return an array of strings of sorted column names
   */
  public ColumnType[] getSortColumnTypes() {
	  return types;
  }

  /**
   * Get an array of column indices in schema of the sorted columns with the first column
   * being the primary sort key, the second column being the
   * secondary sort key, ..., etc.
   *
   * @return an array of strings of sorted column names
   */
  public int[] getSortIndices() {
    return indices;
  }

  /**
   * Get the number of sorted columns
   *
   * @return number of sorted columns
   */
  public int size() {
    return (columns == null ? 0 : columns.length);
  }

  /**
   * Get the comparator name 
   *
   * @return  comparator name
   */
  public String getComparator() {
    return comparator;
  }

  /**
   * Check if the two SortInfo objects are equal
   *
   * @return true if one's sort columns is equal to a leading portion of the other's
   */
  public boolean equals(String[] sortcolumns, String comparator) throws IOException {
    if (sortcolumns == null || sortcolumns.length == 0)
    {
      return false;
    }
    for (String column : sortcolumns)
    {
    	if (schema.getColumn(column) == null)
            throw new IOException(column + " does not exist in schema");
    }
    if (this.columns.length <= sortcolumns.length)
    {
      for (int i = 0; i < this.columns.length; i++)
     {
       if (!this.columns[i].equals(sortcolumns[i]))
         return false;
     }
    } else {
      for (int i = 0; i < sortcolumns.length; i++)
     {
       if (!sortcolumns[i].equals(this.columns[i]))
         return false;
     }
    }
    if (this.comparator == null && comparator == null)
    {
      return true;
    } else if (this.comparator != null && comparator != null)
    {
      return (this.comparator.equals(comparator));
    } else {
      return false;
    }
  }

  /**
   * Build a SortInfo object from sort column names, schema, and comparator
   *
   * @param sortStr
   *                     comma-separated sort column names
   * @param schema
   *                     schema of the Zebra table for the sort columns
   * @param comparator
   *                     comparator name
   * @return             newly built SortInfo object
   */
  public static SortInfo parse(String sortStr, Schema schema, String comparator) throws IOException
  {
    if (sortStr == null || sortStr.trim().isEmpty())
    {
      return null;
    }
    String[] sortedColumns = sortStr.trim().split(SORTED_COLUMN_DELIMITER);
    int[] sortColIndices = new int[sortedColumns.length];
    ColumnType[] sortColTypes = new ColumnType[sortedColumns.length];
    Schema.ColumnSchema cs;
    for (int i = 0; i < sortedColumns.length; i++)
    {
      sortedColumns[i] = sortedColumns[i].trim();
      /*
       * sanity check the sort column's existence
       */
      if ((cs = schema.getColumn(sortedColumns[i])) == null)
        throw new IOException(sortedColumns[i] + " does not exist in schema");
      sortColIndices[i] = schema.getColumnIndex(sortedColumns[i]);
      sortColTypes[i] = schema.getColumn(sortedColumns[i]).getType();
    }
    String comparatorInUse = (sortedColumns.length > 0 ?
        (comparator == null || comparator.isEmpty() ?
          DEFAULT_COMPARATOR : comparator) : null);
    return new SortInfo(sortedColumns, sortColIndices, sortColTypes, comparatorInUse, schema);
  }

  /**
   * Build a string of comma-separated sort column names from an array of sort column names
   *
   * @param names an array of sort column names
   *
   * @return a string of comma-separated sort column names
   */
  public static String toSortString(String[] names)
  {
    if (names == null || names.length == 0)
      return null;

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < names.length; i++)
    {
      if (i > 0)
        sb.append(SORTED_COLUMN_DELIMITER);
      sb.append(names[i]);
    }
    return sb.toString();
  }
}
