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

package org.apache.hadoop.zebra.io;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.pig.data.Tuple;

/**
 * Scanner interface allows application to scan a range of rows of a table. It
 * is the caller's responsibility to close() the scanner after finishing using
 * it. Scanner allows one to fetch the key without fetching the value to allow
 * underlying implementation to instantiating tuple objects (Row) lazily.
 */
public interface TableScanner extends Closeable {
  /**
   * Test whether the cursor is at the end of the scan range.
   * 
   * @return Whether the cursor is at the end of the scan range.
   * @throws IOException
   */
  boolean atEnd() throws IOException;

  /**
   * Advance cursor to the next Row.
   * 
   * @return true if the cursor is moved. It will return true when the scanner
   *         moves the cursor from the last row to the end position.
   * @throws IOException
   */
  boolean advance() throws IOException;

  /**
   * Get the row key.
   * 
   * @param key
   *          The output parameter to hold the result.
   */
  void getKey(BytesWritable key) throws IOException;

  /**
   * Get the row.
   * 
   * @param row
   *          The output parameter to hold the result. It must conform to the
   *          schema that the scanner is aware of.
   * @see TableScanner#getSchema()
   * @throws IOException
   */
  void getValue(Tuple row) throws IOException;

  /**
   * Seek to the key that is greater or equal to the provided key, or we reach
   * the end. It is only applicable to sorted tables.
   * 
   * @param key
   *          The input key.
   * @return true if we find the exact match; false otherwise.
   * @throws IOException
   */
  boolean seekTo(BytesWritable key) throws IOException;

  /**
   * Seek to the end of the scan range.
   * 
   * @throws IOException
   */
  void seekToEnd() throws IOException;

  /**
   */
  public String getProjection();
  
  /**
   * Get the projection's schema
   */
  public Schema getSchema();
}
