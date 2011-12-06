/*
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
package org.apache.pig.impl.io;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Treats keys as offset in file and value as line. 
 */
public class BinStorageRecordReader extends RecordReader<Text, Tuple> {

  private long start;
  private long pos;
  private long end;
  private BufferedPositionedInputStream in;
  private Tuple value = null;
  public static final int RECORD_1 = 0x01;
  public static final int RECORD_2 = 0x02;
  public static final int RECORD_3 = 0x03;
  private DataInputStream inData = null;

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    if (start != 0) {
        fileIn.seek(start);
    }
    in = new BufferedPositionedInputStream(fileIn, start);
    inData = new DataInputStream(in);
  }
  
  public boolean nextKeyValue() throws IOException {
      int b = 0;
      //    skip to next record
      while (true) {
          if (in == null || in.getPosition() >=end) {
              return false;
          }
          // check if we saw RECORD_1 in our last attempt
          // this can happen if we have the following 
          // sequence RECORD_1-RECORD_1-RECORD_2-RECORD_3
          // After reading the second RECORD_1 in the above
          // sequence, we should not look for RECORD_1 again
          if(b != RECORD_1) {
              b = in.read();
              if(b != RECORD_1 && b != -1) {
                  continue;
              }
              if(b == -1) return false;
          }
          b = in.read();
          if(b != RECORD_2 && b != -1) {
              continue;
          }
          if(b == -1) return false;
          b = in.read();
          if(b != RECORD_3 && b != -1) {
              continue;
          }
          if(b == -1) return false;
          b = in.read();
          if(b != DataType.TUPLE && b != -1) {
              continue;
          }
          if(b == -1) return false;
          break;
      }
      try {
          // if we got here, we have seen RECORD_1-RECORD_2-RECORD_3-TUPLE_MARKER
          // sequence - lets now read the contents of the tuple 
          value = (Tuple)DataReaderWriter.readDatum(inData, DataType.TUPLE);
          pos=in.getPosition();
          return true;
      } catch (ExecException ee) {
          throw ee;
      }

  }

  @Override
  public Text getCurrentKey() {
      // the key is always null since we don't really have a key for each
      // input record
      return null;
  }

  @Override
  public Tuple getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}
