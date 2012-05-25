/*
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

package org.apache.pig.piggybank.storage.avro;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * This is an implementation of record reader which reads in avro data and 
 * convert them into <NullWritable, Writable> pairs.
 * 
 */
public class PigAvroRecordReader extends RecordReader<NullWritable, Writable> {

    private AvroStorageInputStream in;
    private DataFileReader<Object> reader;   /*reader of input avro data*/
    private long start;
    private long end;

    /**
     * constructor to initialize input and avro data reader
     */
    public PigAvroRecordReader(TaskAttemptContext context, FileSplit split,
                                    Schema schema) throws IOException {
        this.in = new AvroStorageInputStream(split.getPath(), context);
        if(schema == null)
            throw new IOException("Need to provide input avro schema");

        try {
          this.reader = new DataFileReader<Object>(in, new PigAvroDatumReader(schema));
        } catch (IOException e) {
          throw new IOException("Error initializing data file reader for file (" +
              split.getPath() + ")", e);
        }
        this.reader.sync(split.getStart()); // sync to start
        this.start = in.tell();
        this.end = split.getStart() + split.getLength();
    }

    @Override
    public float getProgress() throws IOException {
        return end == start ? 0.0f : Math.min(1.0f, (getPos() - start) / (float) (end - start));
    }

    public long getPos() throws IOException {
        return in.tell();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Writable getCurrentValue() throws IOException, InterruptedException {
        Object obj = reader.next();
        if (obj instanceof Tuple) {
            AvroStorageLog.details("Class =" + obj.getClass());
            return (Tuple) obj;
        } else {
            AvroStorageLog.details("Wrap calss " + obj.getClass() + " as a tuple.");
            return wrapAsTuple(obj);
        }
    }

    /**
     * Wrap non-tuple value as a tuple
     */
    protected Tuple wrapAsTuple(Object in) {
        Tuple tuple = TupleFactory.getInstance().newTuple();
        tuple.append(in);
        return tuple;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        // Nothing to do
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!reader.hasNext() || reader.pastSync(end))
            return false;

        return true;
    }

}
