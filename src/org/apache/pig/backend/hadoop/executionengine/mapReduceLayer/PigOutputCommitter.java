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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;

/**
 * A specialization of the default FileOutputCommitter to allow
 * pig to inturn delegate calls to the OutputCommiter(s) of the 
 * StoreFunc(s)' OutputFormat(s).
 */
public class PigOutputCommitter extends OutputCommitter {
    
    /**
     * OutputCommitter(s) of Store(s) in the map
     */
    List<Pair<OutputCommitter, POStore>> mapOutputCommitters;
    
    /**
     * OutputCommitter(s) of Store(s) in the reduce
     */
    List<Pair<OutputCommitter, POStore>> reduceOutputCommitters;
    
    /**
     * Store(s) in the map
     */
    List<POStore> mapStores;
    
    /**
     * Store(s) in the reduce
     */
    List<POStore> reduceStores;
    
    /**
     * @param context
     * @throws IOException
     */
    public PigOutputCommitter(TaskAttemptContext context)
            throws IOException {
        // create and store the map and reduce output committers
        mapOutputCommitters = getCommitters(context, 
                JobControlCompiler.PIG_MAP_STORES);
        reduceOutputCommitters = getCommitters(context, 
                JobControlCompiler.PIG_REDUCE_STORES);
        
    }

    /**
     * @param conf
     * @param storeLookupKey
     * @return
     * @throws IOException 
     */
    @SuppressWarnings("unchecked")
    private List<Pair<OutputCommitter, POStore>> getCommitters(
            TaskAttemptContext context,
            String storeLookupKey) throws IOException {
        Configuration conf = context.getConfiguration();
        
        // if there is a udf in the plan we would need to know the import
        // path so we can instantiate the udf. This is required because
        // we will be deserializing the POStores out of the plan in the next
        // line below. The POStore inturn has a member reference to the Physical
        // plan it is part of - so the deserialization goes deep and while
        // deserializing the plan, the udf.import.list may be needed.
        PigContext.setPackageImportList((ArrayList<String>)ObjectSerializer.
                deserialize(conf.get("udf.import.list")));
        LinkedList<POStore> stores = (LinkedList<POStore>) ObjectSerializer.
        deserialize(conf.get(storeLookupKey));
        List<Pair<OutputCommitter, POStore>> committers = 
            new ArrayList<Pair<OutputCommitter,POStore>>();
        for (POStore store : stores) {
            StoreFunc sFunc = store.getStoreFunc();
            
            TaskAttemptContext updatedContext = setUpContext(context, store);
            try {
                committers.add(new Pair<OutputCommitter, POStore>(
                        sFunc.getOutputFormat().getOutputCommitter(
                                updatedContext), store));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return committers;
        
    }
    
    private TaskAttemptContext setUpContext(TaskAttemptContext context, 
            POStore store) throws IOException {
        // make a copy of the context so that the actions after this call
        // do not end up updating the same context
        TaskAttemptContext contextCopy = new TaskAttemptContext(
                context.getConfiguration(), context.getTaskAttemptID());
        
        // call setLocation() on the storeFunc so that if there are any
        // side effects like setting map.output.dir on the Configuration
        // in the Context are needed by the OutputCommitter, those actions
        // will be done before the committer is created. Also the String 
        // version of StoreConfig and StoreFunc for the specific store need
        // to be set up in the context in case the committer needs them
        PigOutputFormat.setLocation(contextCopy, store);
        return contextCopy;   
    }
    
    private JobContext setUpContext(JobContext context, 
            POStore store) throws IOException {
        // make a copy of the context so that the actions after this call
        // do not end up updating the same context
        JobContext contextCopy = new JobContext(
                context.getConfiguration(), context.getJobID());
        
        // call setLocation() on the storeFunc so that if there are any
        // side effects like setting map.output.dir on the Configuration
        // in the Context are needed by the OutputCommitter, those actions
        // will be done before the committer is created. Also the String 
        // version of StoreConfig and StoreFunc for the specific store need
        // to be set up in the context in case the committer needs them
        PigOutputFormat.setLocation(contextCopy, store);
        return contextCopy;   
    }

    private void storeCleanup(POStore store, Configuration conf)
            throws IOException {
        StoreFunc storeFunc = store.getStoreFunc();
        if (storeFunc instanceof StoreMetadata) {
            Schema schema = store.getSchema();
            if (schema != null) {
                ((StoreMetadata) storeFunc).storeSchema(
                        new ResourceSchema(schema, store.getSortInfo()), store.getSFile()
                                .getFileName(), conf);
            }
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.FileOutputCommitter#cleanupJob(org.apache.hadoop.mapred.JobContext)
     */
    @Override
    public void cleanupJob(JobContext context) throws IOException {
        // call clean up on all map and reduce committers
        for (Pair<OutputCommitter, POStore> mapCommitter : mapOutputCommitters) {            
            JobContext updatedContext = setUpContext(context, 
                    mapCommitter.second);
            storeCleanup(mapCommitter.second, context.getConfiguration());
            mapCommitter.first.cleanupJob(updatedContext);
        }
        for (Pair<OutputCommitter, POStore> reduceCommitter : 
            reduceOutputCommitters) {            
            JobContext updatedContext = setUpContext(context, 
                    reduceCommitter.second);
            storeCleanup(reduceCommitter.second, context.getConfiguration());
            reduceCommitter.first.cleanupJob(updatedContext);
        }
       
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#abortTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {        
        if(context.getTaskAttemptID().isMap()) {
            for (Pair<OutputCommitter, POStore> mapCommitter : 
                mapOutputCommitters) {
                TaskAttemptContext updatedContext = setUpContext(context, 
                        mapCommitter.second);
                mapCommitter.first.abortTask(updatedContext);
            } 
        } else {
            for (Pair<OutputCommitter, POStore> reduceCommitter : 
                reduceOutputCommitters) {
                TaskAttemptContext updatedContext = setUpContext(context, 
                        reduceCommitter.second);
                reduceCommitter.first.abortTask(updatedContext);
            } 
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        if(context.getTaskAttemptID().isMap()) {
            for (Pair<OutputCommitter, POStore> mapCommitter : 
                mapOutputCommitters) {
                TaskAttemptContext updatedContext = setUpContext(context, 
                        mapCommitter.second);
                mapCommitter.first.commitTask(updatedContext);
            } 
        } else {
            for (Pair<OutputCommitter, POStore> reduceCommitter : 
                reduceOutputCommitters) {
                TaskAttemptContext updatedContext = setUpContext(context, 
                        reduceCommitter.second);
                reduceCommitter.first.commitTask(updatedContext);
            } 
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#needsTaskCommit(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public boolean needsTaskCommit(TaskAttemptContext context)
            throws IOException {
        boolean needCommit = false;
        if(context.getTaskAttemptID().isMap()) {
            for (Pair<OutputCommitter, POStore> mapCommitter : 
                mapOutputCommitters) {
                TaskAttemptContext updatedContext = setUpContext(context, 
                        mapCommitter.second);
                needCommit = needCommit || 
                mapCommitter.first.needsTaskCommit(updatedContext);
            } 
            return needCommit;
        } else {
            for (Pair<OutputCommitter, POStore> reduceCommitter : 
                reduceOutputCommitters) {
                TaskAttemptContext updatedContext = setUpContext(context, 
                        reduceCommitter.second);
                needCommit = needCommit || 
                reduceCommitter.first.needsTaskCommit(updatedContext);
            } 
            return needCommit;
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#setupJob(org.apache.hadoop.mapreduce.JobContext)
     */
    @Override
    public void setupJob(JobContext context) throws IOException {
        // call set up on all map and reduce committers
        for (Pair<OutputCommitter, POStore> mapCommitter : mapOutputCommitters) {
            JobContext updatedContext = setUpContext(context, 
                    mapCommitter.second);
            mapCommitter.first.setupJob(updatedContext);
        }
        for (Pair<OutputCommitter, POStore> reduceCommitter : 
            reduceOutputCommitters) {
            JobContext updatedContext = setUpContext(context, 
                    reduceCommitter.second);
            reduceCommitter.first.setupJob(updatedContext);
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        if(context.getTaskAttemptID().isMap()) {
            for (Pair<OutputCommitter, POStore> mapCommitter : 
                mapOutputCommitters) {
                TaskAttemptContext updatedContext = setUpContext(context, 
                        mapCommitter.second);
                mapCommitter.first.setupTask(updatedContext);
            } 
        } else {
            for (Pair<OutputCommitter, POStore> reduceCommitter : 
                reduceOutputCommitters) {
                TaskAttemptContext updatedContext = setUpContext(context, 
                        reduceCommitter.second);
                reduceCommitter.first.setupTask(updatedContext);
            } 
        }
    }
}
