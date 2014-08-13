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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

/**
 *
 */
public class BinStorageRecordWriter extends
        RecordWriter<org.apache.hadoop.io.WritableComparable, Tuple> {

    public static final int RECORD_1 = 0x01;
    public static final int RECORD_2 = 0x02;
    public static final int RECORD_3 = 0x03;

    /**
     * the outputstream to write out on
     */
    private DataOutputStream out;
    
    /**
     * 
     */
    public BinStorageRecordWriter(DataOutputStream out) {
        this.out = out;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void close(TaskAttemptContext arg0) throws IOException,
            InterruptedException {
        out.close();        
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
     */
    @Override
    public void write(WritableComparable wc, Tuple t) throws IOException,
            InterruptedException {
        // we really only want to write the tuple (value) out here
        out.write(RECORD_1);
        out.write(RECORD_2);
        out.write(RECORD_3);
        DataReaderWriter.writeDatum(out, t);
        
    }

}
