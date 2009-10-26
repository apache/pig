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
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.StoreConfig;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStoreImpl;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * This class is used to have a POStore write to DFS via a output
 * collector/record writer. It sets up a modified job configuration to
 * force a write to a specific subdirectory of the main output
 * directory. This is done so that multiple output directories can be
 * used in the same job. Since the hadoop framework requires a
 * reporter to be available to create the record writer the main
 * function (createStoreFunc) has to be called from within a map or
 * reduce function.
 */
public class MapReducePOStoreImpl extends POStoreImpl {

    private TaskAttemptContext context;
    
    @SuppressWarnings("unchecked")
    private RecordWriter writer;
    
    private Configuration job;
    
    private final Log log = LogFactory.getLog(getClass());
    
    public static final String PIG_STORE_CONFIG = "pig.store.config";
    
    public MapReducePOStoreImpl(TaskAttemptContext context) {
        this.context = context;
        this.job = context.getConfiguration();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public StoreFunc createStoreFunc(FileSpec sFile, Schema schema) 
        throws IOException {

        // If the StoreFunc associate with the POStore is implements
        // getStorePreparationClass() and returns a non null value,
        // then it could be wanting to implement OutputFormat for writing out to hadoop
        // Check if this is the case, if so, use the OutputFormat class the 
        // StoreFunc gives us else use our default PigOutputFormat
        Object storeFunc = PigContext.instantiateFuncFromSpec(sFile.getFuncSpec());
        Class sPrepClass = null;
        try {
            sPrepClass = ((StoreFunc)storeFunc).getStorePreparationClass();
        } catch(AbstractMethodError e) {
            // this is for backward compatibility wherein some old StoreFunc
            // which does not implement getStorePreparationClass() is being
            // used. In this case, we want to just use PigOutputFormat
            sPrepClass = null;
        }
        Class<? extends OutputFormat<?,?>> outputClass = null;
        if (sPrepClass != null && OutputFormat.class.isAssignableFrom(sPrepClass)) {
            outputClass = sPrepClass; 
        } else {
            outputClass = PigOutputFormat.class;
        }

        // PigOuputFormat will look for pig.storeFunc to actually
        // write stuff out.
        // serialize the store func spec using ObjectSerializer
        // ObjectSerializer.serialize() uses default java serialization
        // and then further encodes the output so that control characters
        // get encoded as regular characters. Otherwise any control characters
        // in the store funcspec would break the job.xml which is created by
        // hadoop from the jobconf.
        job.set("pig.storeFunc", ObjectSerializer.serialize(sFile.getFuncSpec().toString()));

        // We set the output dir to the final location of the output,
        // the output dir set in the original job config points to the
        // temp location for the multi store which we set to PIG output dir.        
        if (job.get(PigOutputFormat.PIG_MAPRED_OUTPUT_DIR) == null) {
            job.set(PigOutputFormat.PIG_MAPRED_OUTPUT_DIR, job.get(PigOutputFormat.MAPRED_OUTPUT_DIR));
        }
        Path outputDir = new Path(sFile.getFileName()).makeQualified(FileSystem.get(job));
        job.set(PigOutputFormat.MAPRED_OUTPUT_DIR, outputDir.toString());

        // Set the schema
        job.set(PIG_STORE_CONFIG, 
                       ObjectSerializer.serialize(new StoreConfig(outputDir.toString(), schema)));

        // Set the relative path that can be used to build a temporary
        // place to store the output from a number of map-reduce tasks.
        String tmpPath = PlanHelper.makeStoreTmpPath(sFile.getFileName());
        job.set(PigOutputFormat.PIG_TMP_PATH, tmpPath);
                
        OutputFormat outputFormat
             = ReflectionUtils.newInstance(outputClass, job);
        
        // create a new record writer
        try {
            writer = outputFormat.getRecordWriter(context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
 
        // return an output collector using the writer we just created.
        return new StoreFuncAdaptor(writer);

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

    /**
     * This is a simple adaptor class to allow the physical store operator
     * to be used in the map reduce case. It will allow to use an output
     * collector instead of an output stream to write tuples.
     */
    //We intentionally skip type checking in backend for performance reasons
    @SuppressWarnings("unchecked")
    private class StoreFuncAdaptor implements StoreFunc {
        private RecordWriter<PigNullableWritable, Writable> collector;
        
        public StoreFuncAdaptor(RecordWriter<PigNullableWritable, Writable> collector) {
            this.collector = collector;
        }
        
        @Override
        public void bindTo(OutputStream os) throws IOException {
        }
        
        @Override
        public void putNext(Tuple f) throws IOException {
            try {
                collector.write(null,f);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        
        @Override
        public void finish() throws IOException {
        }

        @Override
        public Class getStorePreparationClass() throws IOException {
            return null;
        }
    }
}
