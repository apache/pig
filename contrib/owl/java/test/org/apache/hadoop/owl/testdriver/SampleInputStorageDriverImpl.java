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
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.owl.mapreduce.OwlInputFormat;
import org.apache.hadoop.owl.mapreduce.OwlInputStorageDriver;
import org.apache.hadoop.owl.mapreduce.OwlPartitionValues;
import org.apache.hadoop.owl.mapreduce.OwlInputFormat.OwlOperation;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.pig.data.Tuple;

public class SampleInputStorageDriverImpl extends OwlInputStorageDriver {

    static Map<Path,SampleDataGenerator> seededData = new TreeMap<Path,SampleDataGenerator>();

    SampleInputFormatImpl savedSampleInputFormatImpl = null;

    public SampleInputStorageDriverImpl(){
        savedSampleInputFormatImpl = new SampleInputFormatImpl();
        savedSampleInputFormatImpl.setDataGenerationSpec(seededData);
    }

    @Override
    public InputFormat<BytesWritable, Tuple> getInputFormat(
            OwlLoaderInfo loaderInfo) {
        savedSampleInputFormatImpl.setLoaderInfo(loaderInfo);
        return savedSampleInputFormatImpl;
    }

    @Override
    public boolean isFeatureSupported(OwlOperation operation)
    throws IOException {
        // for now, we deny all feature support till(when/if) we add them one by one
        if (operation.equals(OwlOperation.PREDICATE_PUSHDOWN)){
            return false;
        } else {
            return false;
        }
    }

    @Override
    public void setInputPath(JobContext jobContext, String location)
    throws IOException {
        savedSampleInputFormatImpl.setJobContext(jobContext);
        savedSampleInputFormatImpl.setSavedLocation(location);
    }

    @Override
    public void setOriginalSchema(JobContext jobContext, OwlSchema schema)
    throws IOException {
        savedSampleInputFormatImpl.setJobContext(jobContext);
        savedSampleInputFormatImpl.setOriginalSchema(schema);
    }

    @Override
    public void setOutputSchema(JobContext jobContext, OwlSchema schema)
    throws IOException {
        savedSampleInputFormatImpl.setJobContext(jobContext);
        savedSampleInputFormatImpl.setOutputSchema(schema);
    }

    @Override
    public void setPartitionValues(JobContext jobContext,
            OwlPartitionValues partitionValues) throws IOException {
        savedSampleInputFormatImpl.setJobContext(jobContext);
        savedSampleInputFormatImpl.setPartitionValues(partitionValues);        
    }

}
