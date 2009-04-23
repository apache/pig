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

import org.apache.pig.data.Tuple;

import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.pig.StoreFunc;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStoreImpl;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;

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

    private PigContext pc;
    private StoreFunc storer;
    private FileSpec sFile;
    private Reporter reporter;
    private RecordWriter writer;
    private JobConf job;

    private final Log log = LogFactory.getLog(getClass());
    
    public MapReducePOStoreImpl(JobConf job) {
        this.job = job;
    }

    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public StoreFunc createStoreFunc(FileSpec sFile) throws IOException {

        // set up a new job conf
        JobConf outputConf = new JobConf(job);
        String tmpPath = PlanHelper.makeStoreTmpPath(sFile.getFileName());

        // Right now we're always using PigOutputFormat.
        outputConf.setOutputFormat(PigOutputFormat.class);

        // PigOuputFormat will look for pig.storeFunc to actually
        // write stuff out.
        outputConf.set("pig.storeFunc", sFile.getFuncSpec().toString());

        // We set the output dir to the final location of the output,
        // the output dir set in the original job config points to the
        // temp location for the multi store.
        Path outputDir = new Path(sFile.getFileName()).makeQualified(FileSystem.get(outputConf));
        outputConf.set("mapred.output.dir", outputDir.toString());

        // The workpath is set to a unique-per-store subdirectory of
        // the current working directory.
        String workPath = outputConf.get("mapred.work.output.dir");
        outputConf.set("mapred.work.output.dir",
                       new Path(workPath, tmpPath).toString());
        OutputFormat outputFormat = outputConf.getOutputFormat();

        // Generate a unique part name (part-<task_partition_number>).
        String fileName = getPartName(outputConf);
        
        // create a new record writer
        writer = outputFormat.getRecordWriter(FileSystem.get(outputConf), 
                                              outputConf, fileName, reporter);

        // return an output collector using the writer we just created.
        return new StoreFuncAdaptor(new OutputCollector() 
            {
                @SuppressWarnings({"unchecked"})
                public void collect(Object key, Object value) throws IOException {
                    writer.write(key,value);
                }
            });
    }

    @Override
    public void tearDown() throws IOException{
        if (writer != null) {
            writer.close(reporter);
            writer = null;
        }
    }

    @Override
    public void cleanUp() throws IOException{
        if (writer != null) {
            writer.close(reporter);
            writer = null;
        }
    }

    private String getPartName(JobConf conf) {
        int partition = conf.getInt("mapred.task.partition", -1);   

        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMinimumIntegerDigits(5);
        numberFormat.setGroupingUsed(false);

        return "part-" + numberFormat.format(partition);
    }

    /**
     * This is a simple adaptor class to allow the physical store operator
     * to be used in the map reduce case. It will allow to use an output
     * collector instead of an output stream to write tuples.
     */
    private class StoreFuncAdaptor implements StoreFunc {
        private OutputCollector collector;
        
        public StoreFuncAdaptor(OutputCollector collector) {
            this.collector = collector;
        }
        
        @Override
        public void bindTo(OutputStream os) throws IOException {
        }
        
        @Override
        public void putNext(Tuple f) throws IOException {
            collector.collect(null,f);
        }
        
        @Override
        public void finish() throws IOException {
        }
    }
}
