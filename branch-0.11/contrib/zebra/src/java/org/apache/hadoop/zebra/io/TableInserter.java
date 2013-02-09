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
 * Inserter interface allows application to to insert a number of rows into
 * table.
 */
public interface TableInserter extends Closeable {
  /**
   * Insert a new row into the table.
   * 
   * @param key
   *          The row key.
   * @param row
   *          The row.
   */
  public void insert(BytesWritable key, Tuple row) throws IOException;

  /**
   * Get the schema of the underlying table we are writing to.
   * 
   * @return The schema of the underlying table.
   */
  public Schema getSchema();
}
