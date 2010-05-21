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

import java.io.IOException;
import java.io.StringReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.pig.data.Tuple;

/**
 * Table expression for reading a BasicTable.
 * 
 * @see <a href="doc-files/examples/ReadABasicTable.java">Usage example for
 *      BasicTableExpr</a>
 */
class BasicTableExpr extends TableExpr {
  private Path path;

  /**
   * default constructor.
   */
  public BasicTableExpr() {
    // no-op
  }

  /**
   * Constructor.
   * 
   * @param path
   *          Path of the BasicTable.
   */
  public BasicTableExpr(Path path) {
    this.path = path;
  }

  /**
   * Set the path.
   * 
   * @param path
   *          path to the BasicTable.
   * @return self.
   */
  public BasicTableExpr setPath(Path path) {
    this.path = path;
    return this;
  }

  /**
   * Get the path.
   * 
   * @return the path to the BasicTable.
   */
  public Path getPath() {
    return path;
  }

  @Override
  protected BasicTableExpr decodeParam(StringReader in) throws IOException {
    String strPath = TableExprUtils.decodeString(in);
    if (strPath == null) {
      throw new RuntimeException("Incomplete expression");
    }
    path = new Path(strPath);
    return this;
  }

  @Override
  protected BasicTableExpr encodeParam(StringBuilder out) {
    if (path == null) {
      throw new RuntimeException("Incomplete expression");
    }
    TableExprUtils.encodeString(out, path.toString());
    return this;
  }

  @Override
  public List<LeafTableInfo> getLeafTables(String projection) {
    ArrayList<LeafTableInfo> ret = new ArrayList<LeafTableInfo>(1);
    ret.add(new LeafTableInfo(path, projection));
    return ret;
  }

  @Override
  public TableScanner getScanner(BytesWritable begin, BytesWritable end,
      String projection, Configuration conf) throws IOException {
    String[] deletedCGs = getDeletedCGs(conf);
    BasicTable.Reader reader = new BasicTable.Reader(path, deletedCGs, conf);
    try {
      reader.setProjection(projection);
    } catch (ParseException e) {
    	throw new IOException("Projection parsing failed : "+e.getMessage());
    } 
    return reader.getScanner(begin, end, true);
  } 

  @Override
  public TableScanner getScanner(RowTableSplit split, String projection,
      Configuration conf) throws IOException, ParseException {
    return new BasicTableScanner(split, projection, conf);
  }

  @Override
  public Schema getSchema(Configuration conf) throws IOException {
    return BasicTable.Reader.getSchema(path, conf);
  }

  @Override
  public boolean sortedSplitCapable() {
    return true;
  }
  
  @Override
  protected void dumpInfo(PrintStream ps, Configuration conf, int indent) throws IOException
  {
    BasicTable.dumpInfo(path.toString(), ps, conf, indent);
  }

  /**
   * Basic Table Scanner
   */
  class BasicTableScanner implements TableScanner {
    private int tableIndex = -1;
    private Integer[] virtualColumnIndices = null;
    private TableScanner scanner = null;
    
    BasicTableScanner(RowTableSplit split, String projection,
        Configuration conf) throws IOException, ParseException, ParseException {
      tableIndex = split.getTableIndex();
      virtualColumnIndices = Projection.getVirtualColumnIndices(projection);
      BasicTable.Reader reader =
        new BasicTable.Reader(new Path(split.getPath()), getDeletedCGs(conf), conf);
      reader.setProjection(projection);
      scanner = reader.getScanner(true, split.getSplit());
    }
    
    @Override
    public boolean advance() throws IOException {
      return scanner.advance();
    }
    
    @Override
    public boolean atEnd() throws IOException {
      return scanner.atEnd();
    }
    
    @Override
    public Schema getSchema() {
      return scanner.getSchema();
    }
    
    @Override
    public void getKey(BytesWritable key) throws IOException {
      scanner.getKey(key);
    }
    
    @Override
    public void getValue(Tuple row) throws IOException {
      scanner.getValue(row);
      if (virtualColumnIndices != null)
      {
        for (int i = 0; i < virtualColumnIndices.length; i++)
        {
          row.set(virtualColumnIndices[i], tableIndex);
        }
      }
    }
    
    @Override
    public boolean seekTo(BytesWritable key) throws IOException {
      return scanner.seekTo(key);
    }
    
    @Override
    public void seekToEnd() throws IOException {
      scanner.seekToEnd();
    }
    
    @Override 
    public void close() throws IOException {
      scanner.close();
    }
    
    @Override
    public String getProjection() {
      return scanner.getProjection();
    }
  }
}
