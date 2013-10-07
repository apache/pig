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
package org.apache.pig.piggybank.storage.hiverc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveRCOutputFormat extends FileOutputFormat<NullWritable, Writable> {

        private static final Logger LOG = LoggerFactory.getLogger(RCFileOutputFormat.class);

        public static String COMPRESSION_CODEC_CONF = "rcfile.output.compression.codec";

        public static String DEFAULT_EXTENSION = ".rc";
        public static String EXTENSION_OVERRIDE_CONF = "rcfile.output.filename.extension"; // "none" disables it.

        /**
         * set number of columns into the given configuration.
         *
         * @param conf
         *          configuration instance which need to set the column number
         * @param columnNum
         *          column number for RCFile's Writer
         *
         */
        public static void setColumnNumber(Configuration conf, int columnNum) {
          assert columnNum > 0;
          conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNum);
        }

        /**
         * Returns the number of columns set in the conf for writers.
         *
         * @param conf
         * @return number of columns for RCFile's writer
         */
        public static int getColumnNumber(Configuration conf) {
          return conf.getInt(RCFile.COLUMN_NUMBER_CONF_STR, 0);
        }

        protected RCFile.Writer createRCFileWriter(TaskAttemptContext job,
                                                   Text columnMetadata)
                                                   throws IOException {
          Configuration conf = job.getConfiguration();

          // override compression codec if set.
          String codecOverride = conf.get(COMPRESSION_CODEC_CONF);
          if (codecOverride != null) {
            conf.setBoolean("mapred.output.compress", true);
            conf.set("mapred.output.compression.codec", codecOverride);
          }

          CompressionCodec codec = null;
          if (getCompressOutput(job)) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
          }

          Metadata metadata = null;

          String ext = conf.get(EXTENSION_OVERRIDE_CONF, DEFAULT_EXTENSION);
          Path file = getDefaultWorkFile(job, ext.equalsIgnoreCase("none") ? null : ext);

          LOG.info("writing to rcfile " + file.toString());

          return new RCFile.Writer(file.getFileSystem(conf), conf, file, job, metadata, codec);
        }

        /**
         * RecordWriter wrapper around an RCFile.Writer
         */
        static protected class Writer extends RecordWriter<NullWritable, Writable> {

          private final RCFile.Writer rcfile;

          protected Writer(HiveRCOutputFormat outputFormat,
                           TaskAttemptContext job,
                           Text columnMetadata) throws IOException {
            rcfile = outputFormat.createRCFileWriter(job, columnMetadata);
          }

          @Override
          public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            rcfile.close();
          }

          @Override
          public void write(NullWritable key, Writable value) throws IOException, InterruptedException {
            rcfile.append(value);
          }
        }

        @Override
        public RecordWriter<NullWritable, Writable> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {
          return new Writer(this, job, null);
        }

}
