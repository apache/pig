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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.owl.mapreduce.OwlPartitionValues;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.pig.data.Tuple;


public class SampleInputFormatImpl extends InputFormat<BytesWritable, Tuple> {

    private static final int DEFAULT_NUM_SPLITS = 1;
    private static final int DEFAULT_NUM_RECORDS = 8;

    private JobContext savedJobContext = null;
    private String savedLocation = null;

    private OwlSchema savedOriginalSchema = null;
    private OwlSchema savedOutputSchema = null;
    private OwlLoaderInfo savedLoaderInfo = null;
    private int numSplits = DEFAULT_NUM_SPLITS;
    private int numRecords = DEFAULT_NUM_RECORDS;
    private OwlPartitionValues partitionValues = null;
    private Map<Path, SampleDataGenerator> seededData;

    @SuppressWarnings("unchecked")
    @Override
    public RecordReader<BytesWritable, Tuple> createRecordReader(InputSplit split,
            TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        SampleRecordReaderImpl rr = new SampleRecordReaderImpl();
        rr.initialize(split, taskAttemptContext);
        rr.setOwlParams(savedJobContext,savedLoaderInfo,savedOriginalSchema,savedOutputSchema,numRecords);
        rr.setPartitionValues(partitionValues);

        return rr;
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
    InterruptedException {
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();

        if ((seededData != null) && (seededData.size() > 0)){
            for (Path key : seededData.keySet()){
                if ((seededData.get(key).getKeyPrefix().equalsIgnoreCase(savedLocation)) || (key.toString().equalsIgnoreCase(savedLocation)) ){
                    InputSplit split = new SampleInputSplitImpl(seededData.get(key));
                    splits.add(split);
                }
            }
        }else{
            for (int i = 0; i < this.numSplits ; i++){
                InputSplit split = new SampleInputSplitImpl();
                splits.add(split);
            }
        }
        return splits;
    }

    public void setJobContext(JobContext jobContext) {
        this.savedJobContext = jobContext;
    }

    public void setSavedLocation(String location) {
        this.savedLocation = location;
    }

    public void setOriginalSchema(OwlSchema schema) {
        this.savedOriginalSchema = schema;
    }

    public void setOutputSchema(OwlSchema schema) {
        this.savedOutputSchema = schema;
    }

    public void setLoaderInfo(OwlLoaderInfo loaderInfo) {
        this.savedLoaderInfo = loaderInfo;
    }

    public void setNumSplits(int numSplits){
        this.numSplits  = numSplits;
    }

    public void setNumRecords(int numRecords){
        this.numRecords  = numRecords;
    }

    public void setPartitionValues(OwlPartitionValues partitionValues) {
        this.partitionValues = partitionValues;
    }

    public void setDataGenerationSpec(
            Map<Path, SampleDataGenerator> seededData) {
        this.seededData = seededData;

    }

}
