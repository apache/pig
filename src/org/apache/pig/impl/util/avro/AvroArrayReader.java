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

package org.apache.pig.impl.util.avro;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.collect.Lists;


/**
 * RecordReader for Avro files
 */
public final class AvroArrayReader
  extends RecordReader<NullWritable, GenericData.Record> {

  private FileReader<GenericData.Array<Object>> reader;
  private long start;
  private long end;
  private Schema schema;
  private GenericData.Array<Object> currentArray;

  /**
   * Creates new instance of AvroRecordReader.
   * @param s The input schema.
   */
  public AvroArrayReader(final Schema s) {
    schema = s;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  /**
   * Returns current value.
   * @return the current value
   * @throws IOException when an IO error occurs
   * @throws InterruptedException when interrupted
   */
  @Override
  public GenericData.Record getCurrentValue()
      throws IOException, InterruptedException {
    if (currentArray != null) {
      GenericData.Record r = new GenericData.Record(
          Schema.createRecord(
              Lists.newArrayList(
                  new Schema.Field(schema.getName(), schema, null, null))
              )
          );
      r.put(0, currentArray);
      return r;
    } else {
      return null;
    }
  }

  @Override
  public NullWritable getCurrentKey()
      throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f,
          ((float) (reader.tell() - start)) / ((float) (end - start)));
    }
  }

  @Override
  public void initialize(final InputSplit isplit, final TaskAttemptContext tc)
      throws IOException, InterruptedException {

    FileSplit fsplit = (FileSplit) isplit;
    start  = fsplit.getStart();
    end    = fsplit.getStart() + fsplit.getLength();
    DatumReader<GenericData.Array<Object>> datumReader
      = new GenericDatumReader<GenericData.Array<Object>>(schema);
    reader = DataFileReader.openReader(
        new FsInput(fsplit.getPath(), tc.getConfiguration()),
        datumReader);
    reader.sync(start);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    if (reader.pastSync(end)) {
      return false;
    }
    try {
      currentArray = reader.next();
    } catch (NoSuchElementException e) {
      return false;
    }
    return true;
  }

}
