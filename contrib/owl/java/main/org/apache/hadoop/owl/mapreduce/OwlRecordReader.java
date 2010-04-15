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
package org.apache.hadoop.owl.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;

/** The Owl wrapper for the underlying RecordReader, this ensures that the initialize on
 * the underlying record reader is done with the underlying split, not with OwlSplit.
 */
class OwlRecordReader extends RecordReader<BytesWritable, Tuple> {

    /** The underlying record reader to delegate to. */
    private RecordReader<BytesWritable, Tuple> baseRecordReader;

    /**
     * Instantiates a new owl record reader.
     * @param baseRecordReader the base record reader
     */
    public OwlRecordReader(RecordReader<BytesWritable, Tuple> baseRecordReader) {
        this.baseRecordReader = baseRecordReader;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext taskContext)
    throws IOException, InterruptedException {
        InputSplit baseSplit = split;

        if( split instanceof OwlSplit ) {
            baseSplit = ((OwlSplit) split).getBaseSplit();
        }

        baseRecordReader.initialize(baseSplit, taskContext);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public BytesWritable getCurrentKey() throws IOException, InterruptedException {
        return baseRecordReader.getCurrentKey();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public Tuple getCurrentValue() throws IOException, InterruptedException {
        return baseRecordReader.getCurrentValue();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return baseRecordReader.getProgress();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return baseRecordReader.nextKeyValue();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        baseRecordReader.close();
    }
}
