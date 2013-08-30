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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStoreImpl;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.tools.pigstats.mapreduce.MRPigStatsUtil;
import org.apache.pig.tools.pigstats.PigStatusReporter;
/**
 * This class is used to have a POStore write to DFS via a output
 * collector/record writer. It sets up a modified job configuration to
 * force a write to a specific subdirectory of the main output
 * directory. This is done so that multiple output directories can be
 * used in the same job. 
 */
@SuppressWarnings("unchecked")
public class MapReducePOStoreImpl extends POStoreImpl {
            
    private TaskAttemptContext context;
    
    private PigStatusReporter reporter;

    private RecordWriter writer;
           
    public MapReducePOStoreImpl(TaskInputOutputContext context) {
        // get a copy of the Configuration so that changes to the
        // configuration below (like setting the output location) do
        // not affect the caller's copy
        Configuration outputConf = new Configuration(context.getConfiguration());
        PigStatusReporter.setContext(context);
        reporter = PigStatusReporter.getInstance();
       
        // make a copy of the Context to use here - since in the same
        // task (map or reduce) we could have multiple stores, we should
        // make this copy so that the same context does not get over-written
        // by the different stores.
        
        this.context = HadoopShims.createTaskAttemptContext(outputConf, 
                context.getTaskAttemptID());
    }
    
    @Override
    public StoreFuncInterface createStoreFunc(POStore store) 
            throws IOException {
 
        StoreFuncInterface storeFunc = store.getStoreFunc();

        // call the setStoreLocation on the storeFunc giving it the
        // Job. Typically this will result in the OutputFormat of the
        // storeFunc storing the output location in the Configuration
        // in the Job. The PigOutFormat.setLocation() method will merge 
        // this modified Configuration into the configuration of the
        // Context we have
        PigOutputFormat.setLocation(context, store);
        OutputFormat outputFormat = storeFunc.getOutputFormat();

        // create a new record writer
        try {
            writer = outputFormat.getRecordWriter(context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
 
        storeFunc.prepareToWrite(writer);
        
        return storeFunc;
    }

    @Override
    public void tearDown() throws IOException {
        if (writer != null) {
            try {
                writer.close(context);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            writer = null;
        }
    }

    @Override
    public void cleanUp() throws IOException {
        if (writer != null) {
            try {
                writer.close(context);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            writer = null;
        }
    }
    
    public Counter createRecordCounter(POStore store) {
        String name = MRPigStatsUtil.getMultiStoreCounterName(store);
        return (name == null) ? null : reporter.getCounter(
                MRPigStatsUtil.MULTI_STORE_COUNTER_GROUP, name); 
    }
}
