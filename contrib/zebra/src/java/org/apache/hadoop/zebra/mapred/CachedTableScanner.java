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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.Tuple;

/**
 * Utility class for those interested in writing their own TableExpr classes.
 * This class encapsulates a regular table scanner and changes the way how keys
 * and rows are accessed (with an internal cache).
 */
final class CachedTableScanner implements Closeable {
  private BytesWritable key;
  private Tuple row;
  private boolean keyReady;
  private boolean rowReady;
  private int index;
  private TableScanner scanner;

  /**
   * Constructor
   * 
   * @param scanner
   *          The scanner to be encapsulated
   * @throws IOException 
   */
  public CachedTableScanner(TableScanner scanner, int index) throws IOException {
    key = new BytesWritable();
    row = TypesUtils.createTuple(Projection.getNumColumns(scanner.getProjection()));
    keyReady = false;
    rowReady = false;
    this.index = index;
    this.scanner = scanner;
  }

  /**
   * Get the key at cursor.
   * 
   * @return The key at cursor
   * @throws IOException
   */
  public BytesWritable getKey() throws IOException {
    if (!keyReady) {
      scanner.getKey(key);
      keyReady = true;
    }
    return key;
  }

  /**
   * Get the value (Tuple) at cursor.
   * 
   * @return the value at cursor.
   * @throws IOException
   */
  public Tuple getValue() throws IOException {
    if (!rowReady) {
      scanner.getValue(row);
      rowReady = true;
    }
    return row;
  }

  /**
   * Get the table index in a union
   * 
   * @return the table index in union
   */
  public int getIndex() {
    return index;
    
  }
  /**
   * Seek to a row whose key is greater than or equal to the input key.
   * 
   * @param inKey
   *          the input key.
   * @return true if an exact matching key is found. false otherwise.
   * @throws IOException
   */
  public boolean seekTo(BytesWritable inKey) throws IOException {
    boolean ret = scanner.seekTo(inKey);
    reset();
    return ret;
  }

  /**
   * Advance the cursor.
   * 
   * @return whether the cursor actually moves.
   * @throws IOException
   */
  public boolean advance() throws IOException {
    boolean ret = scanner.advance();
    reset();
    return ret;
  }

  private void reset() throws IOException {
    row = TypesUtils.createTuple(Projection.getNumColumns(scanner.getProjection()));
    keyReady = false;
    rowReady = false;
  }

  /**
   * Is cursor at end?
   * 
   * @return Whether cursor is at the end.
   * @throws IOException
   */
  public boolean atEnd() throws IOException {
    return scanner.atEnd();
  }

  /**
   * Get the schema of the tuples returned from this scanner.
   * 
   * @return The schema of the tuples returned from this scanner.
   */
  public String getProjection() {
    return scanner.getProjection();
  }
  
  /**
   * Get the projected schema
   * @return The projected schema
   */
  public Schema getSchema() {
    return scanner.getSchema();
  }

  /**
   * Seek to the end of the scanner.
   * 
   * @throws IOException
   */
  public void seekToEnd() throws IOException {
    reset();
    scanner.seekToEnd();
  }

  /**
   * Close the scanner, release all resources.
   */
  @Override
  public void close() throws IOException {
    scanner.close();
  }
}
