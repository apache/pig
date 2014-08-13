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

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigConfiguration;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;

/**
 * LOAD FUNCTION FOR PIG INTERNAL USE ONLY! This load function is used for
 * storing intermediate data between MR jobs of a pig query. The serialization
 * format of this load function can change in newer versions of pig, so this
 * should NOT be used to store any persistent data.
 */
@InterfaceAudience.Private
public class TFileStorage extends FileInputLoadFunc implements
                StoreFuncInterface, LoadMetadata {

    private static final Log mLog = LogFactory.getLog(TFileStorage.class);
    public static final String useLog = "TFile storage in use";

    private TFileRecordReader recReader = null;
    private TFileRecordWriter recWriter = null;

    /**
     * Simple binary nested reader format
     */
    public TFileStorage() throws IOException {
        mLog.debug(useLog);
    }

    @Override
    public Tuple getNext() throws IOException {
        if (recReader.nextKeyValue()) {
            return recReader.getCurrentValue();
        }
        else {
            return null;
        }
    }

    @Override
    public void putNext(Tuple t) throws IOException {
        try {
            recWriter.write(null, t);
        }
        catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public static class TFileInputFormat extends
                    PigFileInputFormat<Text, Tuple> {

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
         */
        @Override
        public RecordReader<Text, Tuple> createRecordReader(InputSplit split,
                        TaskAttemptContext context) throws IOException,
                        InterruptedException {
            return new TFileRecordReader();
        }

    }

    @Override
    public InputFormat getInputFormat() {
        return new TFileInputFormat();
    }

    @Override
    public int hashCode() {
        return 42;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        recReader = (TFileRecordReader) reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    public static class TFileOutputFormat
                    extends
                    FileOutputFormat<org.apache.hadoop.io.WritableComparable, Tuple> {

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
         */
        @Override
        public RecordWriter<WritableComparable, Tuple> getRecordWriter(
                        TaskAttemptContext job) throws IOException,
                        InterruptedException {
            Configuration conf = job.getConfiguration();
            String codec = conf.get(PigConfiguration.PIG_TEMP_FILE_COMPRESSION_CODEC, "");
            if (!codec.equals("lzo") && !codec.equals("gz") && !codec.equals("gzip"))
                throw new IOException(
                                "Invalid temporary file compression codec [" + codec + "]. Expected compression codecs are gz(gzip) and lzo");
            if (codec.equals("gzip")) {
                codec = "gz";
            }
            mLog.info(codec + " compression codec in use");
            Path file = getDefaultWorkFile(job, "");
            return new TFileRecordWriter(file, codec, conf);
        }
    }

    @Override
    public OutputFormat getOutputFormat() {
        return new TFileOutputFormat();
    }

    @Override
    public void prepareToWrite(RecordWriter writer) {
        this.recWriter = (TFileRecordWriter) writer;
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {

    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
                    throws IOException {
        return LoadFunc.getAbsolutePath(location, curDir);
    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
                    throws IOException {
        return null;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
                    throws IOException {
        return Utils.getSchema(this, location, true, job);
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
                    throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression plan) throws IOException {
        throw new UnsupportedOperationException();
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
        // DEFAULT: do nothing
    }
}
