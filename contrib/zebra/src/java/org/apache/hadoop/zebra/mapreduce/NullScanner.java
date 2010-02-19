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

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.pig.data.Tuple;

/**
 * A scanner that contains no rows.
 */
class NullScanner implements TableScanner {
  String projection;

  public NullScanner(String projection) {
    this.projection = projection;
  }
  
  @Override
  public boolean advance() throws IOException {
    return false;
  }

  @Override
  public boolean atEnd() throws IOException {
    return true;
  }

  @Override
  public String getProjection() {
    return projection;
  }
  
  @Override
  public Schema getSchema() {
    Schema result;
    try {
       result = Projection.toSchema(projection);
    } catch (ParseException e) {
      throw new AssertionError("Invalid Projection: "+e.getMessage());
    }
    return result;
  }

  @Override
  public void getKey(BytesWritable key) throws IOException {
    throw new EOFException("No more rows to read");
  }

  @Override
  public void getValue(Tuple row) throws IOException {
    throw new EOFException("No more rows to read");
  }

  @Override
  public boolean seekTo(BytesWritable key) throws IOException {
    return false;
  }

  @Override
  public void seekToEnd() throws IOException {
    // Do nothing
  }

  @Override
  public void close() throws IOException {
    // Do nothing
  }
}
