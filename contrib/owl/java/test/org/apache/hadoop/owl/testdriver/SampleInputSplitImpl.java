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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataReaderWriter;

public class SampleInputSplitImpl extends InputSplit implements Writable{

    private OwlSchema savedOutputSchema = null;
    private String savedLocation = null;
    private OwlLoaderInfo savedLoaderInfo = null;
    private JobContext savedJobContext = null;

    private Map<BytesWritable,Tuple> data = null;
    private long numBytes = 0;

    private String[] locations;
    private SampleDataGenerator dataGen = null;

    public SampleInputSplitImpl(){
        locations = new String[] {};
    }

    public SampleInputSplitImpl(SampleDataGenerator dataGen){
        this.setDataGen(dataGen);
        locations = new String[] { dataGen.getPath().toString() } ;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        // really not determinable unless the RecordReader were already invocated? Return 0 for now.
        return numBytes;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return locations;
    }

    /**
     * @param dataGen the dataGen to set
     */
    public void setDataGen(SampleDataGenerator dataGen) {
        this.dataGen = dataGen;
    }

    /**
     * @return the dataGen
     */
    public SampleDataGenerator getDataGen() {
        return dataGen;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String dataGenSerializedMetaInfo = WritableUtils.readString(in);

        Long numTuples = WritableUtils.readVLong(in);

        List<Tuple> tuples = new ArrayList<Tuple>();

        for (int i = 0 ; i < numTuples ; i++ ){
            tuples.add((Tuple)DataReaderWriter.readDatum(in));
        }

        this.dataGen = new SampleDataGenerator(dataGenSerializedMetaInfo,tuples);
        locations = new String[] { dataGen.getPath().toString() } ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String dataGenSerializedMetaInfo = dataGen.getSerializedJSONMetaInfo();
        WritableUtils.writeString(out,dataGenSerializedMetaInfo);

        List<Tuple> tuples = dataGen.getTuples();
        WritableUtils.writeVLong(out,new Long(tuples.size()));
        for (int i = 0 ; i < tuples.size() ; i++ ){
            tuples.get(i).write(out);
        }

    }

}

