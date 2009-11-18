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
package org.apache.hadoop.zebra.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.zebra.tfile.Utils;

/**
 * Status of a BasicTable. The status may be reported under some projection.
 * Projection is needed because the size of a BasicTable may be affected by the
 * projection.
 */
public final class BasicTableStatus implements Writable {
  long size;
  long rows;
  BytesWritable beginKey;
  BytesWritable endKey;

  /**
   * Get the begin key. If the begin key is unknown, null will be returned.
   * 
   * @return begin key.
   */
  public BytesWritable getBeginKey() {
    return beginKey;
  }

  /**
   * Get the end key. If the end key is unknown, null will be returned.
   * 
   * @return end key.
   */
  public BytesWritable getEndKey() {
    return endKey;
  }

  /**
   * Get the size in bytes of the BasicTable.
   * 
   * @return Size (in bytes) of all files that make up the BasicTable.
   */
  public long getSize() {
    return size;
  }

  /**
   * Get the # of rows of the BasicTable. If the # of rows is unknown, -1 will
   * be returned.
   * 
   * @return Number of rows of the BasicTable.
   */
  public long getRows() {
    return rows;
  }

  /**
   * @see Writable#readFields(DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    size = Utils.readVLong(in);
    rows = Utils.readVLong(in);
    if (rows == 0) {
      beginKey = null;
      endKey = null;
    }
    else {
      if (beginKey == null) {
        beginKey = new BytesWritable();
      }
      beginKey.readFields(in);
      if (endKey == null) {
        endKey = new BytesWritable();
      }
      endKey.readFields(in);
    }
  }

  /**
   * @see Writable#write(DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Utils.writeVLong(out, size);
    Utils.writeVLong(out, rows);
    if (rows > 0) {
      beginKey.write(out);
      endKey.write(out);
    }
  }
}
