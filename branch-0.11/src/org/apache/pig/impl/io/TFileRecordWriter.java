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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.InterSedes;
import org.apache.pig.data.InterSedesFactory;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;

/**
 * A record reader used to write data compatible with {@link InterRecordWriter}
 * It uses the default InterSedes object for serialization.
 */
public class TFileRecordWriter extends
                RecordWriter<org.apache.hadoop.io.WritableComparable, Tuple> {

    final private BytesWritable KEY0 = new BytesWritable(new byte[0]);
    private static InterSedes sedes = InterSedesFactory.getInterSedesInstance();
    /**
     * the outputstream to write out on
     */
    Writer writer = null;
    FSDataOutputStream fileOut = null;

    /**
     * 
     */
    public TFileRecordWriter(Path file, String codec, Configuration conf)
                    throws IOException {
        FileSystem fs = file.getFileSystem(conf);
        fileOut = fs.create(file, false);
        writer = new Writer(fileOut, 1024 * 1024, codec, null, conf);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
                    InterruptedException {
        if (writer != null) writer.close();
        if (fileOut != null) fileOut.close();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
     */
    @Override
    public void write(WritableComparable wc, Tuple t) throws IOException,
                    InterruptedException {
        DataOutputStream outputKey = writer.prepareAppendKey(KEY0.getLength());
        try {
            outputKey.write(KEY0.getBytes(), 0, KEY0.getLength());
        }
        finally {
            outputKey.close();
        }
        // we really only want to write the tuple (value) out here
        DataOutputStream outputValue = writer.prepareAppendValue(-1);

        try {
            sedes.writeDatum(outputValue, t);
        }
        finally {
            outputValue.close();
        }
    }

}
