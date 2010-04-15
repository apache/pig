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

package org.apache.hadoop.owl.testdriver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.lang.Byte;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.mapreduce.OwlPartitionValues;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class SampleRecordReaderImpl extends RecordReader {

    private TaskAttemptContext savedTaskAttemptContext = null;
    private InputSplit savedInputSplit = null;
    private JobContext savedJobContext = null;
    private OwlLoaderInfo savedLoaderInfo = null;
    private OwlSchema savedOriginalSchema = null;
    private OwlSchema savedOutputSchema = null;

    Map<BytesWritable,Tuple> data = null;
    Iterator<BytesWritable> dataIter = null;
    BytesWritable currentKey = null;
    Tuple currentValue = null;
    float numProcessed = 0;
    private int numRecords;
    private OwlPartitionValues partitionValues = null;

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if ((data == null) || (data.size() == 0)){
            return (float) 1.0;
        }else{
            return (numProcessed / data.size()) ;
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if ((dataIter != null)&&(dataIter.hasNext())){
            numProcessed++;
            currentKey = dataIter.next();
            currentValue = SampleDataGenerator.transformTuple(
                    data.get(currentKey),this.savedOriginalSchema,this.savedOutputSchema
            );
            return true;
        }else{
            return false;
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
        this.savedInputSplit = split;
        this.savedTaskAttemptContext = taskAttemptContext;
    }

    public void setOwlParams(JobContext savedJobContext,
            OwlLoaderInfo savedLoaderInfo,
            OwlSchema savedOriginalSchema, OwlSchema savedOutputSchema, int numRecords) throws ExecException, OwlException {
        this.savedJobContext = savedJobContext;
        this.savedLoaderInfo = savedLoaderInfo;
        this.savedOriginalSchema  = savedOriginalSchema;
        this.savedOutputSchema = savedOutputSchema;
        this.numRecords = numRecords;

        if ((savedOutputSchema == null)){
            savedOutputSchema = savedOriginalSchema;
        }

        _generateMockData();
        dataIter = data.keySet().iterator();
    }


    public void setPartitionValues(OwlPartitionValues partitionValues) {
        this.partitionValues  = partitionValues;
    }

    private void _generateMockData() throws ExecException, OwlException {
        data = new HashMap<BytesWritable,Tuple>();
        if (((SampleInputSplitImpl)this.savedInputSplit).getDataGen() != null){
            SampleDataGenerator gen = ((SampleInputSplitImpl)this.savedInputSplit).getDataGen();
            for ( Tuple tuple : gen.getTuples()){
                BytesWritable key = new BytesWritable(
                        ((DataByteArray)gen.generateBytes()).get()
                );
                data.put(key,tuple);
            }
        } else {
            for (int i = 0 ; i < this.numRecords ; i++){
                BytesWritable key = new BytesWritable(new byte[] { Byte.valueOf((new Integer(i)).toString()).byteValue() });
                Tuple tuple = SampleDataGenerator.generateTuple(this.savedOutputSchema);
                data.put(key,tuple);
            }
        }
    }



}
