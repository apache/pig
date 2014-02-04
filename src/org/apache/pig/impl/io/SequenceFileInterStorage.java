/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.impl.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;

/**
 * Store tuples (BinSedesTuples, specifically) using sequence files to leverage
 * sequence file's compression features. Replacement for previous use of {@code TFileStorage}, which
 * had some edge cases that were not properly handled there.
 */
@InterfaceAudience.Private
public class SequenceFileInterStorage extends FileInputLoadFunc implements
StoreFuncInterface, LoadMetadata {
    private static final Log mLog = LogFactory.getLog(SequenceFileInterStorage.class);
    public static final String useLog = "SequenceFile storage in use";

    final private NullWritable KEY0 = NullWritable.get();

    RecordReader<NullWritable, BinSedesTuple> recReader;
    RecordWriter<NullWritable, BinSedesTuple> recWriter;

    /**
     * Simple binary nested reader format
     */
    public SequenceFileInterStorage() throws IOException {
        mLog.debug(useLog);
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (recReader.nextKeyValue()) {
                return recReader.getCurrentValue();
            }
            else {
                return null;
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void putNext(Tuple t) throws IOException {
        try {
            recWriter.write(KEY0, (BinSedesTuple) t);
        }
        catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() {
        return new SequenceFileInputFormat<Object, Tuple>();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
                    throws IOException {
        return Utils.getSchema(this, location, true, job);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        recReader = reader;
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
                    throws IOException {
        return LoadFunc.getAbsolutePath(location, curDir);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new SequenceFileTupleOutputFormat();
    }

    public static class SequenceFileTupleOutputFormat extends SequenceFileOutputFormat<NullWritable, BinSedesTuple> {
        protected SequenceFile.Writer getSequenceWriter(TaskAttemptContext context)
                throws IOException {
              Configuration conf = context.getConfiguration();

              CompressionCodec codec = null;
              CompressionType compressionType = CompressionType.NONE;
              if (getCompressOutput(context)) {
                // find the kind of compression to do
                compressionType = getOutputCompressionType(context);
                // find the right codec
                Class<?> codecClass = getOutputCompressorClass(context,
                                                               DefaultCodec.class);
                codec = (CompressionCodec)
                  ReflectionUtils.newInstance(codecClass, conf);
              }
              // get the path of the temporary output file
              Path file = getDefaultWorkFile(context, "");
              FileSystem fs = file.getFileSystem(conf);
              return SequenceFile.createWriter(fs, conf, file,
                       NullWritable.class,
                       BinSedesTuple.class,
                       compressionType,
                       codec,
                       context);
            }

            @Override
            public RecordWriter<NullWritable, BinSedesTuple>
                   getRecordWriter(TaskAttemptContext context
                                   ) throws IOException, InterruptedException {
              final SequenceFile.Writer out = getSequenceWriter(context);

              return new RecordWriter<NullWritable, BinSedesTuple>() {

                  @Override
                public void write(NullWritable key, BinSedesTuple value)
                    throws IOException {

                    out.append(key, value);
                  }

                  @Override
                public void close(TaskAttemptContext context) throws IOException {
                    out.close();
                  }
                };
            }
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        Utils.setMapredCompressionCodecProps(conf);
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        recWriter = writer;
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        StoreFunc.cleanupOnFailureImpl(location, job);

    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        // DEFAULT : do nothing
    }


}
