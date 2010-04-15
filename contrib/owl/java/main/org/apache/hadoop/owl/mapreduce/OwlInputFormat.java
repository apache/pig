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
package org.apache.hadoop.owl.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.pig.data.Tuple;

/** The InputFormat to use to read data from Owl */
public class OwlInputFormat extends InputFormat<BytesWritable, Tuple> {

    //The keys used to store info into the job Configuration
    static final String OWL_KEY_BASE = "mapreduce.lib.owl";
    static final String OWL_KEY_OUTPUT_SCHEMA = OWL_KEY_BASE + ".output.schema";
    static final String OWL_KEY_JOB_INFO =  OWL_KEY_BASE + ".job.info";
    static final String OWL_KEY_PREDICATE = OWL_KEY_BASE + ".predicate.string";

    /**
     * Enumeration of possible Owl Operations
     */
    public enum OwlOperation {
        PREDICATE_PUSHDOWN
    }

    /**
     * Set the input to use for the Job. This queries the metadata server with
     * the specified partition predicates, gets the matching partitions, puts
     * the information in the conf object. The inputInfo object is updated with
     * information needed in the client context
     * @param job the job object
     * @param inputInfo the owl table input info
     * @throws IOException the exception in communicating with the owl server
     */
    public static void setInput(Job job,
            OwlTableInputInfo inputInfo) throws IOException {
        OwlInitializeInput.setInput(job, inputInfo);
    }

    /**
     * Gets the list of features supported for the given partitions. If any one
     * of the underlying InputFormat's does not support the feature and it cannot
     * be implemented by Owl, then the feature is not returned.
     * @param inputInfo  the owl table input info
     * @return the storage features supported for the partitions selected
     *         by the setInput call 
     * @throws IOException 
     */
    public static List<OwlOperation> getSupportedFeatures(
            OwlTableInputInfo inputInfo) throws IOException {
        return OwlFeatureSupport.getSupportedFeatures(inputInfo.getJobInfo());
    }

    /**
     * Checks if the specified operation is supported for the given partitions.
     * If any one of the underlying InputFormat's does not support the operation
     * and it cannot be implemented by Owl, then returns false. Else returns true.
     * @param inputInfo the owl table input info
     * @param operation the operation to check for
     * @return true, if the feature is supported for selected partitions
     * @throws IOException 
     */
    public static boolean isFeatureSupported(OwlTableInputInfo inputInfo,
            OwlOperation operation) throws IOException {
        return OwlFeatureSupport.isFeatureSupported(inputInfo.getJobInfo(), operation);
    }

    /**
     * Set the predicate filter for pushdown to the storage driver.
     * @param job the job object
     * @param predicate the predicate filter, an arbitrary AND/OR filter
     * @return true, if the specified predicate can be filtered for the
     *         given partitions
     * @throws IOException the exception
     */
    public static boolean setPredicate(Job job,
            String predicate) throws IOException {
        return OwlFeatureSupport.setPredicate(job, predicate);
    }

    /**
     * Set the schema for the Tuple data returned by OwlInputFormat.
     * @param job the job object
     * @param schema the schema to use as the consolidated schema
     */
    public static void setOutputSchema(Job job,
            OwlSchema schema) throws IOException {
        job.getConfiguration().set(
                OWL_KEY_OUTPUT_SCHEMA, SerializeUtil.serialize(schema));
    }

    /**
     * Logically split the set of input files for the job. Returns the
     * underlying InputFormat's splits
     * @param jobContext the job context object
     * @return the splits, an OwlInputSplit wrapper over the storage
     *         driver InputSplits
     * @throws IOException or InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
    throws IOException, InterruptedException {

        //Get the job info from the configuration,
        //throws exception if not initialized
        OwlJobInfo jobInfo = getJobInfo(jobContext);

        List<InputSplit> outputSplits = new ArrayList<InputSplit>();
        List<OwlPartitionInfo> partitionInfoList = jobInfo.getPartitions();
        if(partitionInfoList == null ) {
            //No partitions match the specified partition filter
            return outputSplits;
        }

        //For each matching partition, call getSplits on the underlying InputFormat
        for(OwlPartitionInfo partitionInfo : partitionInfoList) {
            Job localJob = new Job(jobContext.getConfiguration()); 
            OwlInputStorageDriver storageDriver = getInputDriverInstance(
                    partitionInfo.getLoaderInfo());

            //Pass all required information to the storage driver
            initStorageDriver(storageDriver, localJob, partitionInfo, jobInfo.getTableSchema());

            //Get the input format for the storage driver
            InputFormat<BytesWritable, Tuple> inputFormat = 
                storageDriver.getInputFormat(partitionInfo.getLoaderInfo());

            //Call getSplit on the storage drivers InputFormat, create an
            //OwlSplit for each underlying split
            List<InputSplit> splits = inputFormat.getSplits(localJob);

            for(InputSplit split : splits) {
                OwlSplit owlSplit= new OwlSplit(
                        partitionInfo,
                        split,
                        jobInfo.getTableSchema());

                outputSplits.add(owlSplit);
            }
        }

        return outputSplits;
    }

    /**
     * Create the RecordReader for the given InputSplit. Returns the underlying 
     * RecordReader if the required operations are supported and schema matches
     * with OwlTable schema. Returns an OwlRecordReader if operations need to
     * be implemented in owl.
     * @param split the split
     * @param taskContext the task attempt context
     * @return the record reader instance, either an OwlRecordReader(later) or
     *         the underlying storage driver's RecordReader
     * @throws IOException or InterruptedException
     */
    @Override
    public RecordReader<BytesWritable, Tuple> createRecordReader(InputSplit split,
            TaskAttemptContext taskContext) throws IOException, InterruptedException {

        OwlSplit owlSplit = (OwlSplit) split;
        OwlPartitionInfo partitionInfo = owlSplit.getPartitionInfo(); 

        //If running through a Pig job, the OwlJobInfo will not be available in the
        //backend process context (since OwlLoader works on a copy of the JobContext and does
        //not call OwlInputFormat.setInput in the backend process).
        //So this function should NOT attempt to read the JobInfo.

        OwlInputStorageDriver storageDriver = 
            getInputDriverInstance(partitionInfo.getLoaderInfo());

        //Pass all required information to the storage driver
        initStorageDriver(storageDriver, taskContext, partitionInfo, owlSplit.getTableSchema());

        //Get the input format for the storage driver
        InputFormat<BytesWritable, Tuple> inputFormat = 
            storageDriver.getInputFormat(partitionInfo.getLoaderInfo());

        //Create the underlying input formats record record and an Owl wrapper
        RecordReader<BytesWritable, Tuple> recordReader = 
            inputFormat.createRecordReader(owlSplit.getBaseSplit(), taskContext);

        return new OwlRecordReader(recordReader);
    }

    /**
     * Gets the OwlTable schema for the table specified in the OwlInputFormat.setInput call
     * on the specified job context. This information is available only after OwlInputFormat.setInput
     * has been called for a JobContext.
     * @param context the context
     * @return the table schema
     * @throws OwlException if OwlInputFromat.setInput has not been called for the current context
     */
    public static OwlSchema getTableSchema(JobContext context) throws OwlException {
        OwlJobInfo jobInfo = getJobInfo(context);
        return jobInfo.getTableSchema();
    }

    /**
     * Gets the OwlJobInfo object by reading the Configuration and deserializing
     * the string. If JobInfo is not present in the configuration, throws an
     * exception since that means OwlInputFormat.setInput has not been called. 
     * @param jobContext the job context
     * @return the OwlJobInfo object
     * @throws OwlException the owl exception
     */
    private static OwlJobInfo getJobInfo(JobContext jobContext) throws OwlException {
        String jobString = jobContext.getConfiguration().get(OWL_KEY_JOB_INFO);
        if( jobString == null ) {
            throw new OwlException(ErrorType.ERROR_INPUT_UNINITIALIZED);
        }

        return (OwlJobInfo) SerializeUtil.deserialize(jobString);
    }


    /**
     * Initializes the storage driver instance. Passes on the required
     * schema information, path info and arguments for the supported
     * features to the storage driver. 
     * @param storageDriver the storage driver
     * @param context the job context
     * @param partitionInfo the partition info
     * @param tableSchema the owl table level schema 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void initStorageDriver(OwlInputStorageDriver storageDriver,
            JobContext context, OwlPartitionInfo partitionInfo,
            OwlSchema tableSchema) throws IOException {

        storageDriver.setInputPath(context, partitionInfo.getLocation());
        storageDriver.setOriginalSchema(context, partitionInfo.getPartitionSchema());
        storageDriver.setPartitionValues(context, partitionInfo.getPartitionValues());

        //Set the output schema. Use the schema given by user if set, otherwise use the
        //table level schema
        OwlSchema outputSchema = null;
        String outputSchemaString = context.getConfiguration().get(OWL_KEY_OUTPUT_SCHEMA);
        if( outputSchemaString != null ) {
            outputSchema = (OwlSchema) SerializeUtil.deserialize(outputSchemaString);
        } else {
            outputSchema = tableSchema;
        }

        storageDriver.setOutputSchema(context, outputSchema);

        //If predicate is set in jobConf, pass it to storage driver
        String predicate = context.getConfiguration().get(OWL_KEY_PREDICATE);
        if( predicate != null ) {
            storageDriver.setPredicate(context, predicate);        
        }
    }

    /**
     * Gets the input driver instance.
     * @param loaderInfo the loader info
     * @return the input driver instance
     * @throws OwlException 
     */
    @SuppressWarnings("unchecked")
    private OwlInputStorageDriver getInputDriverInstance(
            OwlLoaderInfo loaderInfo) throws OwlException {
        try {
            Class<? extends OwlInputStorageDriver> driverClass = 
                (Class<? extends OwlInputStorageDriver>)
                Class.forName(loaderInfo.getInputDriverClass());
            return driverClass.newInstance();
        } catch(Exception e) {
            throw new OwlException(ErrorType.ERROR_CREATE_STORAGE_DRIVER,
                    loaderInfo.getInputDriverClass(), e);
        }
    }
}
