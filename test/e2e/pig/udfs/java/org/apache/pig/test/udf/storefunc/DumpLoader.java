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

package org.apache.pig.test.udf.storefunc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;

// This is a loader for data produced by DumpStore.
// Line format: (field1, field2, ... fieldn)\n

public class DumpLoader extends FileInputLoadFunc {
    protected RecordReader in = null;

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }                                                                                           
            Text value = (Text) in.getCurrentValue();
            String line = value.toString();
            Tuple t = DefaultTupleFactory.getInstance().newTuple();
            String tmp = line.substring(1, line.length() - 2);
            String[] fields = tmp.split(",");
            int i;
            for (i = 0; i < fields.length; i++) {
                t.append(fields[i].trim());
            }
            return t;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
    
    @Override
    public InputFormat getInputFormat() {
        return new TextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

}
	
