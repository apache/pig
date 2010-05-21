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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.Tuple;

/**
 * Adaptor class to implement RecordReader on top of Scanner.
 * 
 * @Deprecated Use (@link org.apache.hadoop.zebra.mapreduce.TableRecordReader) instead
 */
@Deprecated
public class TableRecordReader implements RecordReader<BytesWritable, Tuple> {
  private final TableScanner scanner;
  private long count = 0;

  /**
   * 
   * @param expr
   *          Table expression
   * @param projection
   *          projection schema. Should never be null.
   * @param split
   *          the split to work on
   * @param conf
   *          JobConf object
   * @throws IOException
   */
  public TableRecordReader(TableExpr expr, String projection,
		  InputSplit split, JobConf conf) throws IOException, ParseException {
	  if( split instanceof RowTableSplit ) {
		  RowTableSplit rowSplit = (RowTableSplit)split;
		  scanner = expr.getScanner(rowSplit, projection, conf);
	  } else {
		  SortedTableSplit tblSplit = (SortedTableSplit)split;
		  scanner = expr.getScanner( tblSplit.getBegin(), tblSplit.getEnd(), projection, conf );
	  }
  }
  
  @Override
  public void close() throws IOException {
    scanner.close();
  }

  @Override
  public BytesWritable createKey() {
    return new BytesWritable();
  }

  @Override
  public Tuple createValue() {
    try {
      return TypesUtils.createTuple(Projection.getNumColumns(scanner.getProjection()));
    }
    catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public long getPos() throws IOException {
    return count;
  }

  @Override
  public float getProgress() throws IOException {
    return  (float)((scanner.atEnd()) ? 1.0 : 0);
  }

  @Override
  public boolean next(BytesWritable key, Tuple value) throws IOException {
    if (scanner.atEnd()) {
      return false;
    }
    scanner.getKey(key);
    scanner.getValue(value);
    scanner.advance();
    ++count;
    return true;
  }
  
  /**
   * Seek to the position at the first row which has the key
   * or just after the key; only applicable for sorted Zebra table
   *
   * @param key
   *          the key to seek on
   */
  public boolean seekTo(BytesWritable key) throws IOException {
    return scanner.seekTo(key);
  }
  
  /**
   * Check if the end of the input has been reached
   *
   * @return true if the end of the input is reached
   */
  public boolean atEnd() throws IOException {
	  return scanner.atEnd();
  }
}
