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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * A specialization of the default FileOutputCommitter to allow
 * pig to inturn delegate calls to the OutputCommiter(s) of the 
 * StoreFunc(s)' OutputFormat(s).
 */
public class PigOutputCommitter extends OutputCommitter {
    
    /**
     * OutputCommitter(s) of Store(s) in the map
     */
    List<OutputCommitter> mapOutputCommitters;
    
    /**
     * OutputCommitter(s) of Store(s) in the reduce
     */
    List<OutputCommitter> reduceOutputCommitters;
    
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
    private List<OutputCommitter> getCommitters(TaskAttemptContext context,
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
        List<OutputCommitter> committers = new ArrayList<OutputCommitter>();
        for (POStore store : stores) {
            StoreFunc sFunc = store.getStoreFunc();
            
            // call setLocation() on the storeFunc so that if there are any
            // side effects like setting map.output.dir on the Configuration
            // in the Context are needed by the OutputCommitter, those actions
            // will be done before the committer is created.
            PigOutputFormat.setLocation(context, sFunc, 
                    store.getSFile().getFileName());
            try {
                committers.add(sFunc.getOutputFormat().
                        getOutputCommitter(context));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return committers;
        
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.FileOutputCommitter#cleanupJob(org.apache.hadoop.mapred.JobContext)
     */
    @Override
    public void cleanupJob(JobContext context) throws IOException {
        // call clean up on all map and reduce committers
        for (OutputCommitter mapCommitter : mapOutputCommitters) {
            mapCommitter.cleanupJob(context);
        }
        for (OutputCommitter reduceCommitter : reduceOutputCommitters) {
            reduceCommitter.cleanupJob(context);
        }
       
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#abortTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        if(context.getTaskAttemptID().isMap()) {
            for (OutputCommitter mapCommitter : mapOutputCommitters) {
                mapCommitter.abortTask(context);
            } 
        } else {
            for (OutputCommitter reduceCommitter : reduceOutputCommitters) {
                reduceCommitter.abortTask(context);
            } 
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        if(context.getTaskAttemptID().isMap()) {
            for (OutputCommitter mapCommitter : mapOutputCommitters) {
                mapCommitter.commitTask(context);
            } 
        } else {
            for (OutputCommitter reduceCommitter : reduceOutputCommitters) {
                reduceCommitter.commitTask(context);
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
            for (OutputCommitter mapCommitter : mapOutputCommitters) {
                needCommit = needCommit || mapCommitter.needsTaskCommit(context);
            } 
            return needCommit;
        } else {
            for (OutputCommitter reduceCommitter : reduceOutputCommitters) {
                needCommit = needCommit || reduceCommitter.needsTaskCommit(context);
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
        for (OutputCommitter mapCommitter : mapOutputCommitters) {
            mapCommitter.setupJob(context);
        }
        for (OutputCommitter reduceCommitter : reduceOutputCommitters) {
            reduceCommitter.setupJob(context);
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter#setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        if(context.getTaskAttemptID().isMap()) {
            for (OutputCommitter mapCommitter : mapOutputCommitters) {
                mapCommitter.setupTask(context);
            } 
        } else {
            for (OutputCommitter reduceCommitter : reduceOutputCommitters) {
                reduceCommitter.setupTask(context);
            } 
        }
    }
}
