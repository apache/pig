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
package org.apache.pig.backend.hadoop.executionengine.fetch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStoreImpl;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.impl.PigContext;

/**
 * This class is used to have a POStore write the output to the underlying storage
 * via a output collector/record writer in case of a fetch task. It sets up dummy context
 * objects which otherwise would be initialized by the Hadoop job itself.
 */
public class FetchPOStoreImpl extends POStoreImpl {

    private PigContext pc;
    private RecordWriter<?, ?> writer;
    private TaskAttemptContext context;
    private OutputCommitter outputCommitter;

    public FetchPOStoreImpl(PigContext pc) {
        this.pc = pc;
    }

    @Override
    public StoreFuncInterface createStoreFunc(POStore store) throws IOException {

        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        StoreFuncInterface storeFunc = store.getStoreFunc();
        JobContext jc = HadoopShims.createJobContext(conf, new JobID());

        OutputFormat<?, ?> outputFormat = storeFunc.getOutputFormat();
        PigOutputFormat.setLocation(jc, store);
        context = HadoopShims.createTaskAttemptContext(conf, HadoopShims.getNewTaskAttemptID());
        PigOutputFormat.setLocation(context, store);

        try {
            outputFormat.checkOutputSpecs(jc);
        }
        catch (InterruptedException e) {
            throw new IOException(e);
        }

        try {
            outputCommitter = outputFormat.getOutputCommitter(context);
            outputCommitter.setupJob(jc);
            outputCommitter.setupTask(context);
            writer = outputFormat.getRecordWriter(context);
        }
        catch (InterruptedException e) {
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
            }
            catch (InterruptedException e) {
                throw new IOException(e);
            }
            writer = null;
        }
        if (outputCommitter.needsTaskCommit(context))
            outputCommitter.commitTask(context);
        HadoopShims.commitOrCleanup(outputCommitter, context);
    }

    @Override
    public void cleanUp() throws IOException {
        if (writer != null) {
            try {
                writer.close(context);
            }
            catch (InterruptedException e) {
                throw new IOException(e);
            }
            writer = null;
        }
        HadoopShims.commitOrCleanup(outputCommitter, context);
    }

}
