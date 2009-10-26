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
import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.pig.PigException;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.tools.bzip2r.CBZip2OutputStream;

/**
 * The better half of PigInputFormat which is responsible
 * for the Store functionality. It is the exact mirror
 * image of PigInputFormat having RecordWriter instead
 * of a RecordReader.
 */
@SuppressWarnings("unchecked")
public class PigOutputFormat extends OutputFormat<WritableComparable, Tuple> {

    /** hadoop job output directory */
    public static final String MAPRED_OUTPUT_DIR = "mapred.output.dir";
    /** hadoop partition number */ 
    public static final String MAPRED_TASK_PARTITION = "mapred.task.partition";
    
    /** the temporary directory for the multi store */
    public static final String PIG_MAPRED_OUTPUT_DIR = "pig.mapred.output.dir";
    /** the relative path that can be used to build a temporary
     * place to store the output from a number of map-reduce tasks*/
    public static final String PIG_TMP_PATH =  "pig.tmp.path";
    
    private FileOutputCommitter committer = null;
    
    private final Log log = LogFactory.getLog(getClass());
    
    /**
     * In general, the mechanism for an OutputFormat in Pig to get hold of the storeFunc
     * and the metadata information (for now schema and location provided for the store in
     * the pig script) is through the following Utility static methods:
     * {@link org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil#getStoreFunc(Configuration)} 
     * - this will get the {@link org.apache.pig.StoreFunc} reference to use in the RecordWriter.write()
     * {@link MapRedUtil#getStoreConfig(Configuration)} - this will get the {@link org.apache.pig.StoreConfig}
     * reference which has metadata like the location (the string supplied with store statement in the script)
     * and the {@link org.apache.pig.impl.logicalLayer.schema.Schema} of the data. The OutputFormat
     * should NOT use the location in the StoreConfig to write the output if the location represents a 
     * Hadoop dfs path. This is because when "speculative execution" is turned on in Hadoop, multiple
     * attempts for the same task (for a given partition) may be running at the same time. So using the
     * location will mean that these different attempts will over-write each other's output.
     * The OutputFormat should use 
     * {@link org.apache.hadoop.mapred.FileOutputFormat#getWorkOutputPath(JobConf)}
     * which will provide a safe output directory into which the OutputFormat should write
     * the part file (given by the name argument in the getRecordWriter() call).
     */
    private PigRecordWriter getRecordWriter(FileSystem fs, TaskAttemptContext context,
                    Path outputDir, String name) throws IOException {
        
        StoreFunc store = MapRedUtil.getStoreFunc(context.getConfiguration());
        
        String parentName = FileOutputFormat.getOutputPath(context).getName();
        int suffixStart = parentName.lastIndexOf('.');
        if (suffixStart != -1) {
            String suffix = parentName.substring(suffixStart);
            if (suffix.equals(".bz") || suffix.equals(".bz2")) {
                name = name + suffix;
            }
        }

        return new PigRecordWriter(fs, new Path(outputDir, name), store);
    }

    @SuppressWarnings("unchecked")
    static public class PigRecordWriter
            extends RecordWriter<WritableComparable, Tuple> {
        
        private OutputStream os = null;

        private StoreFunc sfunc = null;

        public PigRecordWriter(FileSystem fs, Path file, StoreFunc sfunc)
                throws IOException {            
            this.sfunc = sfunc;
            fs.delete(file, true);
            this.os = fs.create(file);
            String name = file.getName();
            if (name.endsWith(".bz") || name.endsWith(".bz2")) {
                os = new CBZip2OutputStream(os);
            }
            this.sfunc.bindTo(os);
        }

        /**
         * We only care about the values, so we are going to skip the keys when
         * we write.
         * 
         * @see org.apache.hadoop.mapreduce.RecordWriter#write(Object, Object)
         */
        @Override
        public void write(WritableComparable key, Tuple value)
                throws IOException, InterruptedException{
            this.sfunc.putNext(value);
        }

        @Override
        public void close(TaskAttemptContext context) 
                throws IOException, InterruptedException {
            sfunc.finish();
            os.close();            
        }

    }

    @Override
    public void checkOutputSpecs(JobContext context) 
            throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (committer == null) {
            Path output = null;            
            if (context.getConfiguration().get(PIG_MAPRED_OUTPUT_DIR) != null) {
                output = new Path(context.getConfiguration().get(PIG_MAPRED_OUTPUT_DIR));
            } else {
                output = new Path(context.getConfiguration().get(MAPRED_OUTPUT_DIR));
            }            
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public RecordWriter getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        FileOutputCommitter committer = (FileOutputCommitter)getOutputCommitter(context);
        Path outputDir = committer.getWorkPath();
        Configuration conf = context.getConfiguration();
        String tmpPath = conf.get(PIG_TMP_PATH);
        if (tmpPath != null) {
            outputDir = new Path(committer.getWorkPath(), tmpPath);
        } 
        return getRecordWriter(FileSystem.get(conf), context, outputDir, getPartName(conf));
    }
    
    private String getPartName(Configuration conf) {
        int partition = conf.getInt(MAPRED_TASK_PARTITION, -1);   

        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMinimumIntegerDigits(5);
        numberFormat.setGroupingUsed(false);

        return "part-" + numberFormat.format(partition);
    }

}
