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

import static org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The OutputFormat for avro data.
 *
 */
public class PigAvroOutputFormat extends FileOutputFormat<NullWritable, Object> {

    /** The file name extension for avro data files. */
    public final static String EXT = ".avro";

    /** The configuration key for Avro deflate level. */
    public static final String DEFLATE_LEVEL_KEY = "avro.mapred.deflate.level";

    /** The default deflate level. */
    public static final int DEFAULT_DEFLATE_LEVEL = 1;

    /** The configuration key for the Avro codec. */
    public static final String OUTPUT_CODEC = "avro.output.codec";

    /** The deflate codec */
    public static final String DEFLATE_CODEC = "deflate";

    /** The configuration key for Avro sync interval. */
    public static final String SYNC_INTERVAL_KEY = "avro.mapred.sync.interval";

    /* avro schema of output data */
    private Schema schema = null;

    /**
     * default constructor
     */
    public PigAvroOutputFormat() {
    }

    /**
     * construct with specified output schema
     * @param s             output schema
     */
    public PigAvroOutputFormat(Schema s) {
        schema = s;
    }

    /**
     * Enable output compression using the deflate codec and
     * specify its level.
     */
    public static void setDeflateLevel(Job job, int level) {
        FileOutputFormat.setCompressOutput(job, true);
        job.getConfiguration().setInt(DEFLATE_LEVEL_KEY, level);

    }

    @Override
    public RecordWriter<NullWritable, Object> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        if (schema == null)
            throw new IOException("Must provide a schema");

        Configuration conf = context.getConfiguration();

        DataFileWriter<Object> writer = new DataFileWriter<Object>(new PigAvroDatumWriter(schema));

        if (FileOutputFormat.getCompressOutput(context)) {
            int level = conf.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
            String codecName = conf.get(OUTPUT_CODEC, DEFLATE_CODEC);
            CodecFactory factory = codecName.equals(DEFLATE_CODEC)
                ? CodecFactory.deflateCodec(level)
                : CodecFactory.fromString(codecName);
            writer.setCodec(factory);
        }

        // Do max as core-default.xml has io.file.buffer.size as 4K
        writer.setSyncInterval(conf.getInt(SYNC_INTERVAL_KEY, Math.max(
                conf.getInt("io.file.buffer.size", DEFAULT_SYNC_INTERVAL), DEFAULT_SYNC_INTERVAL)));

        Path path = getDefaultWorkFile(context, EXT);
        writer.create(schema, path.getFileSystem(conf).create(path));
        return new PigAvroRecordWriter(writer);
    }

}
