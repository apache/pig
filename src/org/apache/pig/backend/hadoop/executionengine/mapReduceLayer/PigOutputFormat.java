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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.OverwritableStoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * The better half of PigInputFormat which is responsible
 * for the Store functionality. It is the exact mirror
 * image of PigInputFormat having RecordWriter instead
 * of a RecordReader.
 */
@SuppressWarnings("unchecked")
public class PigOutputFormat extends OutputFormat<WritableComparable, Tuple> {
    
    private enum Mode { SINGLE_STORE, MULTI_STORE};

    /** hadoop job output directory */
    public static final String MAPRED_OUTPUT_DIR = "mapred.output.dir";
    /** hadoop partition number */ 
    public static final String MAPRED_TASK_PARTITION = "mapred.task.partition";
    
    /** the temporary directory for the multi store */
    public static final String PIG_MAPRED_OUTPUT_DIR = "pig.mapred.output.dir";
    /** the relative path that can be used to build a temporary
     * place to store the output from a number of map-reduce tasks*/
    public static final String PIG_TMP_PATH =  "pig.tmp.path";
    
    List<POStore> reduceStores = null;
    List<POStore> mapStores = null;
    Configuration currentConf = null;
    
    @Override
    public RecordWriter<WritableComparable, Tuple> getRecordWriter(TaskAttemptContext taskattemptcontext)
                throws IOException, InterruptedException {
        setupUdfEnvAndStores(taskattemptcontext);
        if(mapStores.size() + reduceStores.size() == 1) {
            // single store case
            POStore store;
            if(mapStores.size() == 1) {
                store = mapStores.get(0);
            } else {
                store = reduceStores.get(0);
            }
            StoreFuncInterface sFunc = store.getStoreFunc();
            // set output location
            PigOutputFormat.setLocation(taskattemptcontext, store);
            // The above call should have update the conf in the JobContext
            // to have the output location - now call checkOutputSpecs()
            RecordWriter writer = sFunc.getOutputFormat().getRecordWriter(
                    taskattemptcontext);
            return new PigRecordWriter(writer, sFunc, Mode.SINGLE_STORE);
        } else {
           // multi store case - in this case, all writing is done through
           // MapReducePOStoreImpl - set up a dummy RecordWriter
           return new PigRecordWriter(null, null, Mode.MULTI_STORE);
        }
    }

    /**
     * Wrapper class which will delegate calls to the actual RecordWriter - this
     * should only get called in the single store case.
     */
    @SuppressWarnings("unchecked")
    static public class PigRecordWriter
            extends RecordWriter<WritableComparable, Tuple> {
        
        /**
         * the actual RecordWriter
         */
        private RecordWriter wrappedWriter;
        
        /**
         * the StoreFunc for the single store
         */
        private StoreFuncInterface sFunc;
        
        /**
         * Single Query or multi query
         */
        private Mode mode;
        
        public PigRecordWriter(RecordWriter wrappedWriter, StoreFuncInterface sFunc, 
                Mode mode)
                throws IOException {            
            this.mode = mode;
            
            if(mode == Mode.SINGLE_STORE) {
                this.wrappedWriter = wrappedWriter;
                this.sFunc = sFunc;
                this.sFunc.prepareToWrite(this.wrappedWriter);
            }
        }

        /**
         * We only care about the values, so we are going to skip the keys when
         * we write.
         * 
         * @see org.apache.hadoop.mapreduce.RecordWriter#write(Object, Object)
         */
        @Override
        public void write(WritableComparable key, Tuple value)
                throws IOException, InterruptedException {
            if(mode == Mode.SINGLE_STORE) {
                sFunc.putNext(value);
            } else {
                throw new IOException("Internal Error: Unexpected code path");
            }
        }

        @Override
        public void close(TaskAttemptContext taskattemptcontext) throws 
        IOException, InterruptedException {
            if(mode == Mode.SINGLE_STORE) {
                wrappedWriter.close(taskattemptcontext);
            }
        }

    }
    
    /**
     * Before delegating calls to underlying OutputFormat or OutputCommitter
     * Pig needs to ensure the Configuration in the JobContext contains
     * the output location and StoreFunc 
     * for the specific store - so set these up in the context for this specific 
     * store
     * @param jobContext the {@link JobContext}
     * @param store the POStore
     * @throws IOException on failure
     */
    public static void setLocation(JobContext jobContext, POStore store) throws 
    IOException {
        Job storeJob = new Job(jobContext.getConfiguration());
        StoreFuncInterface storeFunc = store.getStoreFunc();
        String outputLocation = store.getSFile().getFileName();
        storeFunc.setStoreLocation(outputLocation, storeJob);
        
        // the setStoreLocation() method would indicate to the StoreFunc
        // to set the output location for its underlying OutputFormat.
        // Typically OutputFormat's store the output location in the
        // Configuration - so we need to get the modified Configuration
        // containing the output location (and any other settings the
        // OutputFormat might have set) and merge it with the Configuration
        // we started with so that when this method returns the Configuration
        // supplied as input has the updates.
        ConfigurationUtil.mergeConf(jobContext.getConfiguration(), 
                storeJob.getConfiguration());
    }

    @Override
    public void checkOutputSpecs(JobContext jobcontext) throws IOException, InterruptedException {
        setupUdfEnvAndStores(jobcontext);
        checkOutputSpecsHelper(mapStores, jobcontext);
        checkOutputSpecsHelper(reduceStores, jobcontext);
    }

    private void checkOutputSpecsHelper(List<POStore> stores, JobContext 
            jobcontext) throws IOException, InterruptedException {
        for (POStore store : stores) {
            // make a copy of the original JobContext so that
            // each OutputFormat get a different copy 
            JobContext jobContextCopy = HadoopShims.createJobContext(
                    jobcontext.getConfiguration(), jobcontext.getJobID());
            
            // set output location
            PigOutputFormat.setLocation(jobContextCopy, store);
            
            StoreFuncInterface sFunc = store.getStoreFunc();
            OutputFormat of = sFunc.getOutputFormat();
            
            // The above call should have update the conf in the JobContext
            // to have the output location - now call checkOutputSpecs()
            try {
                of.checkOutputSpecs(jobContextCopy);
            } catch (IOException ioe) {
                boolean shouldThrowException = true;
                if (sFunc instanceof OverwritableStoreFunc) {
                    if (((OverwritableStoreFunc) sFunc).shouldOverwrite()) {
                        if (ioe instanceof FileAlreadyExistsException
                                || ioe instanceof org.apache.hadoop.fs.FileAlreadyExistsException) {
                            shouldThrowException = false;
                        }
                    }
                }
                if (shouldThrowException)
                    throw ioe;
            }
        }
    }
    /**
     * @param currentConf2
     * @param storeLookupKey
     * @return
     * @throws IOException 
     */
    private List<POStore> getStores(Configuration conf, String storeLookupKey) 
    throws IOException {
        return (List<POStore>)ObjectSerializer.deserialize(
                conf.get(storeLookupKey));
    }
    
    
    private void setupUdfEnvAndStores(JobContext jobcontext)
    throws IOException{
        Configuration newConf = jobcontext.getConfiguration();
        
        // We setup UDFContext so in StoreFunc.getOutputFormat, which is called inside 
        // construct of PigOutputCommitter, can make use of it
        MapRedUtil.setupUDFContext(newConf);
        
        // if there is a udf in the plan we would need to know the import
        // path so we can instantiate the udf. This is required because
        // we will be deserializing the POStores out of the plan in the next
        // line below. The POStore inturn has a member reference to the Physical
        // plan it is part of - so the deserialization goes deep and while
        // deserializing the plan, the udf.import.list may be needed.
        if(! isConfPropEqual("udf.import.list", currentConf, newConf)){
            PigContext.setPackageImportList((ArrayList<String>)ObjectSerializer.
                    deserialize(newConf.get("udf.import.list")));
        }
        if(! isConfPropEqual(JobControlCompiler.PIG_MAP_STORES, currentConf, newConf)){
            mapStores = getStores(newConf, JobControlCompiler.PIG_MAP_STORES);
        }
        if(! isConfPropEqual(JobControlCompiler.PIG_REDUCE_STORES, currentConf, newConf)){
            reduceStores = getStores(newConf, JobControlCompiler.PIG_REDUCE_STORES);
        }
        //keep a copy of the config, so some steps don't need to be taken unless
        // config properties have changed (eg. creating stores).
        currentConf = new Configuration(newConf);
    }
    
    /**
     * Check if given property prop is same in configurations conf1, conf2
     * @param prop
     * @param conf1
     * @param conf2
     * @return true if both are equal 
     */
    private boolean isConfPropEqual(String prop, Configuration conf1,
            Configuration conf2) {
        if( (conf1 == null || conf2 == null) && (conf1 != conf2) ){
            return false;
        }
        String str1 = conf1.get(prop);
        String str2 = conf2.get(prop);
        if( (str1 == null || str2 == null) && (str1 != str2) ){
            return false;
        }
        return str1.equals(str2);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext 
            taskattemptcontext) throws IOException, InterruptedException {
        setupUdfEnvAndStores(taskattemptcontext);
        
        // we return an instance of PigOutputCommitter to Hadoop - this instance
        // will wrap the real OutputCommitter(s) belonging to the store(s)
        return new PigOutputCommitter(taskattemptcontext,
                mapStores,
                reduceStores);
    }
}
