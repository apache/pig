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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.pig.PigException;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.tools.bzip2r.CBZip2OutputStream;

/**
 * The better half of PigInputFormat which is responsible
 * for the Store functionality. It is the exact mirror
 * image of PigInputFormat having RecordWriter instead
 * of a RecordReader.
 */
public class PigOutputFormat implements OutputFormat<WritableComparable, Tuple> {
    public static final String PIG_OUTPUT_FUNC = "pig.output.func";

    /**
     * In general, the mechanism for an OutputFormat in Pig to get hold of the storeFunc
     * and the metadata information (for now schema and location provided for the store in
     * the pig script) is through the following Utility static methods:
     * {@link org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil#getStoreFunc(JobConf)} 
     * - this will get the {@link org.apache.pig.StoreFunc} reference to use in the RecordWriter.write()
     * {@link MapRedUtil#getStoreConfig(JobConf)} - this will get the {@link org.apache.pig.StoreConfig}
     * reference which has metadata like the location (the string supplied with store statement in the script)
     * and the {@link org.apache.pig.impl.logicalLayer.schema.Schema} of the data. The OutputFormat
     * should NOT use the location in the StoreConfig to write the output if the location represents a 
     * Hadoop dfs path. This is because when "speculative execution" is turned on in Hadoop, multiple
     * attempts for the same task (for a given partition) may be running at the same time. So using the
     * location will mean that these different attempts will over-write each other's output.
     * The OutputFormat should use 
     * {@link org.apache.hadoop.mapred.FileOutputFormat#getWorkOutputPath(JobConf)}
     * which will provide a safe output directory into which the OutputFormat should write
     * the part file (given by the name argument in the getRecordWriter() call).
     */
    public RecordWriter<WritableComparable, Tuple> getRecordWriter(FileSystem fs, JobConf job,
            String name, Progressable progress) throws IOException {
        Path outputDir = FileOutputFormat.getWorkOutputPath(job);
        return getRecordWriter(fs, job, outputDir, name, progress);
    }

    public PigRecordWriter getRecordWriter(FileSystem fs, JobConf job,
            Path outputDir, String name, Progressable progress)
            throws IOException {
        StoreFunc store = MapRedUtil.getStoreFunc(job);

        String parentName = FileOutputFormat.getOutputPath(job).getName();
        int suffixStart = parentName.lastIndexOf('.');
        if (suffixStart != -1) {
            String suffix = parentName.substring(suffixStart);
            if (suffix.equals(".bz") || suffix.equals(".bz2")) {
                name = name + suffix;
            }
        }
        return new PigRecordWriter(fs, new Path(outputDir, name), store);
    }

    public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
        // TODO We really should validate things here
        return;
    }

    static public class PigRecordWriter implements
            RecordWriter<WritableComparable, Tuple> {
        private OutputStream os = null;

        private StoreFunc sfunc = null;

        public PigRecordWriter(FileSystem fs, Path file, StoreFunc sfunc)
                throws IOException {
            this.sfunc = sfunc;
            fs.delete(file, true);
            this.os = fs.create(file);
            String name = file.getName();
            if (name.endsWith(".bz") || name.endsWith(".bz2")) {
                os = new CBZip2OutputStream(os);
            }
            this.sfunc.bindTo(os);
        }

        /**
         * We only care about the values, so we are going to skip the keys when
         * we write.
         * 
         * @see org.apache.hadoop.mapred.RecordWriter#write(Object, Object)
         */
        public void write(WritableComparable key, Tuple value)
                throws IOException {
            this.sfunc.putNext(value);
        }

        public void close(Reporter reporter) throws IOException {
            sfunc.finish();
            os.close();
        }

    }
}
