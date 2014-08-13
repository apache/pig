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

import static org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.mapred.AvroOutputFormat.DEFAULT_DEFLATE_LEVEL;
import static org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY;
import static org.apache.avro.mapred.AvroOutputFormat.SYNC_INTERVAL_KEY;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;

/**
 * RecordWriter for Avro objects.
 */
public class AvroRecordWriter extends RecordWriter<NullWritable, Object> {

  private Schema schema = null;
  private DataFileWriter<GenericData.Record> writer;
  private Path out;
  private Configuration conf;

  /**
   * Creates new AvroRecordWriter.
   * @param s Schema for the files on this output path
   * @param o Output path
   * @param c Hadoop configuration
   * @throws IOException
   */
  public AvroRecordWriter(/*final Schema s, */final Path o, final Configuration c)
      throws IOException {
    out = o;
    conf = c;
  }

  // copied from org.apache.avro.mapred.AvroOutputFormat
  static void configureDataFileWriter(DataFileWriter<GenericData.Record> writer,
      JobConf job) throws UnsupportedEncodingException {
    if (FileOutputFormat.getCompressOutput(job)) {
      int level = job.getInt(DEFLATE_LEVEL_KEY,
          DEFAULT_DEFLATE_LEVEL);
      String codecName = job.get(AvroJob.OUTPUT_CODEC, DEFLATE_CODEC);
      CodecFactory factory = codecName.equals(DEFLATE_CODEC)
        ? CodecFactory.deflateCodec(level)
        : CodecFactory.fromString(codecName);
      writer.setCodec(factory);
    }

    // Do max as core-default.xml has io.file.buffer.size as 4K
    writer.setSyncInterval(job.getInt(SYNC_INTERVAL_KEY, Math.max(
            job.getInt("io.file.buffer.size", DEFAULT_SYNC_INTERVAL), DEFAULT_SYNC_INTERVAL)));

    // copy metadata from job
    for (Map.Entry<String,String> e : job) {
      if (e.getKey().startsWith(AvroJob.TEXT_PREFIX))
        writer.setMeta(e.getKey().substring(AvroJob.TEXT_PREFIX.length()),
                       e.getValue());
      if (e.getKey().startsWith(AvroJob.BINARY_PREFIX))
        writer.setMeta(e.getKey().substring(AvroJob.BINARY_PREFIX.length()),
                       URLDecoder.decode(e.getValue(), "ISO-8859-1")
                       .getBytes("ISO-8859-1"));
    }
  }

  @Override
  public void close(final TaskAttemptContext arg0)
      throws IOException, InterruptedException {
    writer.close();
  }

  @Override
  public void write(final NullWritable key, final Object value)
      throws IOException, InterruptedException {

    if (value instanceof GenericData.Record) {
      // whoo-hoo! already avro
      writer.append((GenericData.Record) value);
    } else if (value instanceof Tuple) {
      // pack the object into an Avro record
      writer.append(AvroStorageDataConversionUtilities
          .packIntoAvro((Tuple) value, schema));
    }
  }

  public void prepareToWrite(Schema s) throws IOException {
    if (s == null) {
      throw new IOException(
          this.getClass().getName() + ".prepareToWrite called with null schema");
    }
    schema = s;
    DatumWriter<GenericData.Record> datumWriter =
        new GenericDatumWriter<GenericData.Record>(s);
    writer = new DataFileWriter<GenericData.Record>(datumWriter);
    configureDataFileWriter(writer, new JobConf(conf));
    writer.create(s, out.getFileSystem(conf).create(out));

  }

}
