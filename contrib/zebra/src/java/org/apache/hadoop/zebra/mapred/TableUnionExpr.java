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
package org.apache.hadoop.zebra.mapred;

import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.BasicTableStatus;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.pig.data.Tuple;

/**
 * Table expression supporting a union of BasicTables.
 * 
 * @see <a href="doc-files/examples/ReadTableUnion.java">Usage example for
 *      UnionTableExpr</a>
 */
class TableUnionExpr extends CompositeTableExpr {
  /**
   * Add another BasicTable into the table-union.
   * 
   * @param expr
   *          The expression for the BasicTable to be added.
   * @return self.
   */
  public TableUnionExpr add(BasicTableExpr expr) {
    super.addCompositeTable(expr);
    return this;
  }

  /**
   * Add an array of BasicTables into the table-union.
   * 
   * @param exprs
   *          the expressions representing the BasicTables to be added.
   * @return self.
   */
  public TableUnionExpr add(BasicTableExpr[] exprs) {
    super.addCompositeTables(exprs);
    return this;
  }

  /**
   * Add a Collection of BasicTables into the table-union.
   * 
   * @param exprs
   *          the expressions representing the BasicTables to be added.
   * @return self.
   */
  public TableUnionExpr add(Collection<? extends BasicTableExpr> exprs) {
    super.addCompositeTables(exprs);
    return this;
  }
  
  @Override
  protected TableUnionExpr decodeParam(StringReader in) throws IOException {
    super.decodeParam(in);
    int n = composite.size();
    for (int i = 0; i < n; ++i) {
      if (!(composite.get(i) instanceof BasicTableExpr)) {
        throw new RuntimeException("Not a BasicTableExpr");
      }
    }
    return this;
  }

  @Override
  protected TableUnionExpr encodeParam(StringBuilder out) {
    super.encodeParam(out);
    return this;
  }

  @Override
  public TableScanner getScanner(BytesWritable begin, BytesWritable end,
      String projection, Configuration conf) throws IOException {
    int n = composite.size();
    if (n==0) {
      throw new IllegalArgumentException("Union of 0 table");
    }
    ArrayList<BasicTable.Reader> readers = new ArrayList<BasicTable.Reader>(n);
    String[] deletedCGsInUnion = getDeletedCGsPerUnion(conf);
    
    if (deletedCGsInUnion != null && deletedCGsInUnion.length != n)
      throw new IllegalArgumentException("Invalid string of deleted column group names: expected = "+
          n + " actual =" + deletedCGsInUnion.length);
    
    for (int i = 0; i < n; ++i) {
      String deletedCGs = (deletedCGsInUnion == null ? null : deletedCGsInUnion[i]);
      String[] deletedCGList = (deletedCGs == null ? null : 
        deletedCGs.split(BasicTable.DELETED_CG_SEPARATOR_PER_TABLE));
      BasicTableExpr expr = (BasicTableExpr) composite.get(i);
      BasicTable.Reader reader =
          new BasicTable.Reader(expr.getPath(), deletedCGList, conf);
      readers.add(reader);
    }

    String actualProjection = projection;
    if (actualProjection == null) {
      // Perform a union on all column names.
      LinkedHashSet<String> colNameSet = new LinkedHashSet<String>();
      for (int i = 0; i < n; ++i) {
        String[] cols = readers.get(i).getSchema().getColumns();
        for (String col : cols) {
          colNameSet.add(col);
        }
      }

      actualProjection = 
          Projection.getProjectionStr(colNameSet.toArray(new String[colNameSet.size()]));
    }
    
    ArrayList<TableScanner> scanners = new ArrayList<TableScanner>(n);
    try {
      for (int i=0; i<n; ++i) {
        BasicTable.Reader reader = readers.get(i);
        reader.setProjection(actualProjection);
        TableScanner scanner = readers.get(i).getScanner(begin, end, true);
        scanners.add(scanner);
      }
    } catch (ParseException e) {
    	throw new IOException("Projection parsing failed : "+e.getMessage());
    }
    
    if (scanners.isEmpty()) {
      return new NullScanner(actualProjection);
    }

    Integer[] virtualColumnIndices = Projection.getVirtualColumnIndices(projection);
    if (virtualColumnIndices != null && n == 1)
      throw new IllegalArgumentException("virtual column requires union of multiple tables");
    return new SortedTableUnionScanner(scanners, Projection.getVirtualColumnIndices(projection));
  }

  @Override
  public TableScanner getScanner(RowTableSplit split, String projection,
      Configuration conf) throws IOException, ParseException {
    BasicTableExpr expr = (BasicTableExpr) composite.get(split.getTableIndex());
    return expr.getScanner(split, projection, conf);
  }
}

/**
 * Union scanner.
 */
class SortedTableUnionScanner implements TableScanner {
  CachedTableScanner[] scanners;
  PriorityBlockingQueue<CachedTableScanner> queue;
  boolean synced = false;
  boolean hasVirtualColumns = false;
  Integer[] virtualColumnIndices = null;
  CachedTableScanner scanner = null; // the working scanner

  SortedTableUnionScanner(List<TableScanner> scanners, Integer[] vcolindices) throws IOException {
    if (scanners.isEmpty()) {
      throw new IllegalArgumentException("Zero-sized table union");
    }
    
    this.scanners = new CachedTableScanner[scanners.size()];
    queue =
        new PriorityBlockingQueue<CachedTableScanner>(scanners.size(),
            new Comparator<CachedTableScanner>() {

              @Override
              public int compare(CachedTableScanner o1, CachedTableScanner o2) {
                try {
                  return o1.getKey().compareTo(o2.getKey());
                }
                catch (IOException e) {
                  throw new RuntimeException("IOException: " + e.toString());
                }
              }

            });
    
    for (int i = 0; i < this.scanners.length; ++i) {
      TableScanner scanner = scanners.get(i);
      this.scanners[i] = new CachedTableScanner(scanner, i);
    }
    // initial fill-ins
    if (!atEnd())
    	scanner = queue.poll();
    virtualColumnIndices = vcolindices;
    hasVirtualColumns = (vcolindices != null && vcolindices.length != 0);
  }
  

  private void sync() throws IOException {
    if (synced == false) {
      queue.clear();
      for (int i = 0; i < scanners.length; ++i) {
        if (!scanners[i].atEnd()) {
          queue.add(scanners[i]);
        }
      }
      synced = true;
    }
  }
  
  @Override
  public boolean advance() throws IOException {
    sync();
    scanner.advance();
    if (!scanner.atEnd()) {
      queue.add(scanner);
    }
    scanner = queue.poll();
    return (scanner != null);
  }

  @Override
  public boolean atEnd() throws IOException {
    sync();
    return (scanner == null && queue.isEmpty());
  }

  @Override
  public String getProjection() {
    return scanners[0].getProjection();
  }
  
  @Override
  public Schema getSchema() {
    return scanners[0].getSchema();
  }

  @Override
  public void getKey(BytesWritable key) throws IOException {
    if (atEnd()) {
      throw new EOFException("No more rows to read");
    }
    
    key.set(scanner.getKey());
  }

  @Override
  public void getValue(Tuple row) throws IOException {
    if (atEnd()) {
      throw new EOFException("No more rows to read");
    }
  
    Tuple tmp = scanner.getValue();
    if (hasVirtualColumns)
    {
       for (int i = 0; i < virtualColumnIndices.length; i++)
       {
         tmp.set(virtualColumnIndices[i], scanner.getIndex());
       }
    }
    row.reference(tmp);
  }

  @Override
  public boolean seekTo(BytesWritable key) throws IOException {
    boolean rv = false;
    for (CachedTableScanner scanner : scanners) {
      rv = rv || scanner.seekTo(key);
    }
    synced = false;
    if (!atEnd())
      scanner = queue.poll();
    return rv;
  }

  @Override
  public void seekToEnd() throws IOException {
    for (CachedTableScanner scanner : scanners) {
      scanner.seekToEnd();
    }
    scanner = null;
    synced = false;
  }

  @Override
  public void close() throws IOException {
    for (CachedTableScanner scanner : scanners) {
      scanner.close();
    }
    queue.clear();
  }
}
