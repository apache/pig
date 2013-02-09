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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;

@SuppressWarnings("unchecked")
public class PigTextOutputFormat extends
        TextOutputFormat<WritableComparable, Tuple> {
   
    private final byte fieldDel;
    
    protected static class PigLineRecordWriter extends
            TextOutputFormat.LineRecordWriter<WritableComparable, Tuple> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        static {
            try {
                newline = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8
                        + " encoding");
            }
        }
        
        private final byte fieldDel;
        
        public PigLineRecordWriter(DataOutputStream out, byte fieldDel) {
            super(out);
            this.fieldDel = fieldDel;
        }
        
        public synchronized void write(WritableComparable key, Tuple value)
                throws IOException {
                int sz = value.size();
                for (int i = 0; i < sz; i++) {
                    StorageUtil.putField(out, value.get(i));
                    if (i != sz - 1) {
                        out.writeByte(fieldDel);
                    }
                }
                out.write(newline);       
        }
    }
    
    public PigTextOutputFormat(byte delimiter) {
        super();
        fieldDel = delimiter;
    }
    
    @Override
    public RecordWriter<WritableComparable, Tuple> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = 
                getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new PigLineRecordWriter(fileOut, fieldDel);
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new PigLineRecordWriter(new DataOutputStream
                                   (codec.createOutputStream(fileOut)), fieldDel);
        }
    }
}
