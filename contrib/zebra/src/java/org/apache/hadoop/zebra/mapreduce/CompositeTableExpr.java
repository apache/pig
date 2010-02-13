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
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;


/**
 * An intermediate class to build concrete composite table expression classes.
 * Lengthy names are chosen to avoid naming conflicts.
 */
abstract class CompositeTableExpr extends TableExpr {
  public final static int INDENT_UNIT = 2;
  protected ArrayList<TableExpr> composite;

  protected CompositeTableExpr() {
    composite = new ArrayList<TableExpr>();
  }

  protected CompositeTableExpr(int n) {
    composite = new ArrayList<TableExpr>(n);
    for (int i = 0; i < n; ++i) {
      composite.add(null);
    }
  }

  protected CompositeTableExpr addCompositeTable(TableExpr expr) {
    composite.add(expr);
    return this;
  }

  protected CompositeTableExpr addCompositeTables(TableExpr[] exprs) {
    composite.ensureCapacity(composite.size() + exprs.length);
    for (TableExpr expr : exprs) {
      composite.add(expr);
    }
    return this;
  }

  protected CompositeTableExpr addCompositeTables(
      Collection<? extends TableExpr> exprs) {
    composite.addAll(exprs);
    return this;
  }

  protected CompositeTableExpr setCompositeTable(int i, TableExpr expr) {
    composite.set(i, expr);
    return this;
  }

  protected List<TableExpr> getCompositeTables() {
    return composite;
  }

  protected TableExpr getCompositeTable(int i) {
    TableExpr expr = composite.get(i);
    if (expr == null) {
      throw new RuntimeException("Incomplete Nary expression");
    }

    return expr;
  }

  @Override
  protected CompositeTableExpr decodeParam(StringReader in) throws IOException {
    composite.clear();
    int n = TableExprUtils.decodeInt(in);
    composite.ensureCapacity(n);
    for (int i = 0; i < n; ++i) {
      TableExpr expr = TableExpr.parse(in);
      composite.add(expr);
    }

    return this;
  }

  @Override
  protected CompositeTableExpr encodeParam(StringBuilder out) {
    int n = composite.size();
    TableExprUtils.encodeInt(out, n);
    for (int i = 0; i < composite.size(); ++i) {
      getCompositeTable(i).encode(out);
    }

    return this;
  }

  @Override
  public List<LeafTableInfo> getLeafTables(String projection) {
    ArrayList<LeafTableInfo> ret = new ArrayList<LeafTableInfo>();

    int n = composite.size();
    for (int i = 0; i < n; ++i) {
      ret.addAll(getCompositeTable(i).getLeafTables(projection));
    }

    return ret;
  }

  protected static class RowMappingEntry {
    final int rowIndex;
    final int fieldIndex;

    public RowMappingEntry(int ri, int fi) {
      rowIndex = ri;
      fieldIndex = fi;
    }

    public int getRowIndex() {
      return rowIndex;
    }

    public int getFieldIndex() {
      return fieldIndex;
    }
  }

  protected static class InferredProjection {
    /**
     * projections on each table in the composite. It should be of the same
     * dimension as composite.
     */
    final Schema[] subProjections;

    /**
     * The actual (adjusted) input projection.
     */
    final Schema projection;

    /**
     * For each column in the input projection, what is the corresponding sub
     * table (row index) and the corresponding projected column (field index).
     */
    final RowMappingEntry[] colMapping;

    InferredProjection(List<String>[] subProj, String[] proj,
        Map<String, RowMappingEntry> colMap) throws ParseException {
      subProjections = new Schema[subProj.length];
      for (int i = 0; i < subProj.length; ++i) {
        List<String> subProjection = subProj[i];
        subProjections[i] =
            new Schema(subProjection.toArray(new String[subProjection.size()]));
      }
      
      projection = new Schema(proj);
      
      this.colMapping = new RowMappingEntry[proj.length];
      
      for (int i = 0; i < proj.length; ++i) {
        colMapping[i] = colMap.get(proj[i]);
      }
    }

    public Schema[] getSubProjections() {
      return subProjections;
    }

    public RowMappingEntry[] getColMapping() {
      return colMapping;
    }

    public Schema getProjection() {
      return projection;
    }
  }

  /**
   * Given the input projection, infer the projections that should be applied on
   * individual table in the composite.
   * 
   * @param projection
   *          The expected projection
   * @return The inferred projection with other information.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  protected InferredProjection inferProjection(String projection,
      Configuration conf)
      throws IOException, ParseException {
    int n = composite.size();
    ArrayList<String>[] subProjections = new ArrayList[n];
    for (int i = 0; i < n; ++i) {
      subProjections[i] = new ArrayList<String>();
    }
    
    String[] columns;
    LinkedHashMap<String, Integer> colNameMap =
        new LinkedHashMap<String, Integer>();
    TreeMap<String, RowMappingEntry> colMapping =
        new TreeMap<String, RowMappingEntry>();

    /**
     * Create a collection of all column names and the corresponding index to
     * the composite.
     */
    for (int i = 0; i < n; ++i) {
      String[] cols = getCompositeTable(i).getSchema(conf).getColumns();
      for (int j = 0; j < cols.length; ++j) {
        if (colNameMap.get(cols[j]) != null) {
          colNameMap.put(cols[j], -1); // special marking on duplicated names.
        }
        else {
          colNameMap.put(cols[j], i);
        }
      }
    }

    if (projection == null) {
      Set<String> keySet = colNameMap.keySet();
      columns = keySet.toArray(new String[keySet.size()]);
    }
    else {
      columns = projection.trim().split(Schema.COLUMN_DELIMITER);
    }

    for (int i = 0; i < columns.length; ++i) {
      String col = columns[i];

      if (colMapping.get(col) != null) {
        throw new IllegalArgumentException(
            "Duplicate column names in projection");
      }

      Integer rowIndex = colNameMap.get(col);
      if (rowIndex == null) {
        colMapping.put(col, null);
      }
      else {
        if (rowIndex < 0) {
          // The column name appears in more than one table in composite.
          throw new RuntimeException("Ambiguous column in projection: "
              + col);
        }
        subProjections[rowIndex].add(columns[i]);
        colMapping.put(col, new RowMappingEntry(rowIndex,
            subProjections[rowIndex].size() - 1));
      }
    }

    return new InferredProjection(subProjections, columns, colMapping);
  }

  @Override
  public Schema getSchema(Configuration conf) throws IOException {
    Schema result = new Schema();
    for (Iterator<TableExpr> it = composite.iterator(); it.hasNext();) {
      TableExpr e = it.next();
      try {
        result.unionSchema(e.getSchema(conf));
      } catch (ParseException exc) {
        throw new IOException("Schema parsing failed :"+exc.getMessage());
      }
    }
    return result;
  }
  
  @Override
  public boolean sortedSplitCapable() {
    for (Iterator<TableExpr> it = composite.iterator(); it.hasNext();) {
      TableExpr e = it.next();
      if (!e.sortedSplitCapable()) return false;
    }

    return true;
  }
  
  @Override
  protected void dumpInfo(PrintStream ps, Configuration conf, int indent) throws IOException
  {
    for (Iterator<TableExpr> it = composite.iterator(); it.hasNext();) {
      TableExpr e = it.next();
      e.dumpInfo(ps, conf, indent+INDENT_UNIT);
    }
  }
}
