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

package org.apache.pig.builtin;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.LoadPushDown;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.avro.AvroRecordWriter;
import org.apache.pig.impl.util.avro.AvroStorageDataConversionUtilities;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.MetaData;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnWriter;
import org.apache.trevni.avro.AvroTrevniOutputFormat;
import org.apache.trevni.avro.HadoopInput;

import com.google.common.collect.Lists;

/**
 *
 * Pig Store/Load Function for Trevni.
 *
 */

public class TrevniStorage extends AvroStorage implements LoadPushDown{

  /**
   * Create new instance of TrevniStorage with no arguments (useful
   * for loading files without specifying parameters).
   */
  public TrevniStorage() {
    super();
  }

  /**
   * Create new instance of TrevniStorage.
   *  @param sn Specifies the input/output schema or record name.
   *  @param opts Options for AvroStorage:
   *  <li><code>-namespace</code> Namespace for an automatically generated
   *    output schema.</li>
   *  <li><code>-schemafile</code> Specifies URL for avro schema file
   *    from which to read the input schema (can be local file, hdfs,
   *    url, etc).</li>
   *  <li><code>-examplefile</code> Specifies URL for avro data file from
   *    which to copy the input schema (can be local file, hdfs, url, etc).</li>
   *  <li><code>-allowrecursive</code> Option to allow recursive schema
   *    definitions (default is false).</li>
   */
  public TrevniStorage(final String sn, final String opts) {
    super(sn, opts);
  }

  /*
   * @see org.apache.pig.LoadFunc#getInputFormat()
   */
  @Override
  public InputFormat<NullWritable, GenericData.Record> getInputFormat()
      throws IOException {

    class TrevniStorageInputFormat
    extends PigFileInputFormat<NullWritable, GenericData.Record> {

      @Override protected boolean isSplitable(JobContext jc, Path p) {
        return false;
      }

      @Override protected  List<FileStatus> listStatus(final JobContext job)
          throws IOException {
        List<FileStatus> results = Lists.newArrayList();
        job.getConfiguration().setBoolean("mapred.input.dir.recursive", true);
        for (FileStatus file : super.listStatus(job)) {
          if (Utils.VISIBLE_FILES.accept(file.getPath())) {
            results.add(file);
          }
        }
        return results;
      }

      @Override
      public RecordReader<NullWritable, GenericData.Record>
      createRecordReader(final InputSplit is, final TaskAttemptContext tc)
          throws IOException, InterruptedException {
        RecordReader<NullWritable, GenericData.Record> rr =
            new RecordReader<NullWritable, GenericData.Record>() {

          private FileSplit fsplit;
          private AvroColumnReader.Params params;
          private AvroColumnReader<GenericData.Record> reader;
          private float rows;
          private long row = 0;
          private GenericData.Record currentRecord = null;

          @Override
          public void close() throws IOException {
            reader.close();
          }

          @Override
          public NullWritable getCurrentKey()
              throws IOException, InterruptedException {
            return NullWritable.get();
          }

          @Override
          public Record getCurrentValue()
              throws IOException, InterruptedException {
            return currentRecord;
          }

          @Override
          public float getProgress()
              throws IOException, InterruptedException {
            return row / rows;
          }

          @Override
          public void initialize(final InputSplit isplit,
              final TaskAttemptContext tac)
                  throws IOException, InterruptedException {
            fsplit = (FileSplit) isplit;
            params = new AvroColumnReader.Params(
                new HadoopInput(fsplit.getPath(), tac.getConfiguration()));
            Schema inputSchema = getInputAvroSchema();
            params.setSchema(inputSchema);
            reader = new AvroColumnReader<GenericData.Record>(params);
            rows = reader.getRowCount();
          }

          @Override
          public boolean nextKeyValue()
              throws IOException, InterruptedException {
            if (reader.hasNext()) {
              currentRecord = reader.next();
              row++;
              return true;
            } else {
              return false;
            }
          }
        };

        // rr.initialize(is, tc);
        tc.setStatus(is.toString());
        return rr;
      }

    }

    return new TrevniStorageInputFormat();

  }

  /*
   * @see org.apache.pig.StoreFuncInterface#getOutputFormat()
   */
  @Override
  public OutputFormat<NullWritable, Object> getOutputFormat()
      throws IOException {
    class TrevniStorageOutputFormat
        extends FileOutputFormat<NullWritable, Object> {

      private Schema schema;

      TrevniStorageOutputFormat(final Schema s) {
        schema = s;
        if (s == null) {
          String schemaString = getProperties(
                AvroStorage.class, udfContextSignature)
              .getProperty(OUTPUT_AVRO_SCHEMA);
          if (schemaString != null) {
            schema = (new Schema.Parser()).parse(schemaString);
          }
        }

      }

      @Override
      public RecordWriter<NullWritable, Object>
          getRecordWriter(final TaskAttemptContext tc)
          throws IOException, InterruptedException {

        if (schema == null) {
          String schemaString = getProperties(
                AvroStorage.class, udfContextSignature)
              .getProperty(OUTPUT_AVRO_SCHEMA);
          if (schemaString != null) {
            schema = (new Schema.Parser()).parse(schemaString);
          }
          if (schema == null) {
            throw new IOException("Null output schema");
          }
        }

        final ColumnFileMetaData meta = new ColumnFileMetaData();

        for (Entry<String, String> e : tc.getConfiguration()) {
          if (e.getKey().startsWith(
              org.apache.trevni.avro.AvroTrevniOutputFormat.META_PREFIX)) {
            meta.put(e.getKey().substring(AvroJob.TEXT_PREFIX.length()), 
                e.getValue().getBytes(MetaData.UTF8));
          }
        }
        
        final Path dir = getOutputPath(tc);
        final FileSystem fs = FileSystem.get(tc.getConfiguration());
        final long blockSize = fs.getDefaultBlockSize();

        if (!fs.mkdirs(dir)) {
          throw new IOException("Failed to create directory: " + dir);
        }

        meta.setCodec("deflate");

        return new AvroRecordWriter(dir, tc.getConfiguration()) {
          private int part = 0;
          private Schema avroRecordWriterSchema;
          private AvroColumnWriter<GenericData.Record> writer;
          
          private void flush() throws IOException {
            Integer taskAttemptId = tc.getTaskAttemptID().getTaskID().getId();
            String partName = String.format("%05d_%03d", taskAttemptId, part++);
            OutputStream out = fs.create(
                new Path(dir, "part-" + partName + AvroTrevniOutputFormat.EXT));
            try {
              writer.writeTo(out);
            } finally {
              out.flush();
              out.close();
            }
          }

          @Override
          public void close(final TaskAttemptContext arg0)
              throws IOException, InterruptedException {
            flush();
          }

          @Override
          public void write(final NullWritable n, final Object o)
              throws IOException, InterruptedException {
            GenericData.Record r =
                AvroStorageDataConversionUtilities
                .packIntoAvro((Tuple) o, schema);
            writer.write(r);
            if (writer.sizeEstimate() >= blockSize) {
              flush();
              writer = new AvroColumnWriter<GenericData.Record>(
                  avroRecordWriterSchema, meta);
            }
          }

          @Override
          public void prepareToWrite(Schema s) throws IOException {
            avroRecordWriterSchema = s;
            writer = new AvroColumnWriter<GenericData.Record>(
                avroRecordWriterSchema, meta);
          }
        };
      }
    }

    return new TrevniStorageOutputFormat(schema);
  }

  @Override
  public  Schema getAvroSchema(Path p[], final Job job) throws IOException {

    ArrayList<FileStatus> statusList = new ArrayList<FileStatus>();
    FileSystem fs = FileSystem.get(p[0].toUri(), job.getConfiguration());
    for (Path temp : p) {
      for (FileStatus tempf : fs.globStatus(temp, Utils.VISIBLE_FILES)) {
        statusList.add(tempf);
      }
    }
    FileStatus[] statusArray = (FileStatus[]) statusList
        .toArray(new FileStatus[statusList.size()]);

    if (statusArray == null) {
      throw new IOException("Path " + p.toString() + " does not exist.");
    }
    
    if (statusArray.length == 0) {
      throw new IOException("No path matches pattern " + p.toString());
    }

    Path filePath = Utils.depthFirstSearchForFile(statusArray, fs);
    
    if (filePath == null) {
      throw new IOException("No path matches pattern " + p.toString());
    }

    AvroColumnReader.Params params =
        new AvroColumnReader.Params(
            new HadoopInput(filePath, job.getConfiguration()));
    AvroColumnReader<GenericData.Record> reader =
        new AvroColumnReader<GenericData.Record>(params);
    Schema s = reader.getFileSchema();
    reader.close();
    return s;
    }

}
