/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.piggybank.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.piggybank.storage.allloader.LoadFuncHelper;
import org.apache.pig.piggybank.storage.partition.PathPartitionHelper;

/**
 * The AllLoader provides the ability to point pig at a folder that contains
 * files in multiple formats e.g. PlainText, Gz, Bz, Lzo, HiveRC etc and have
 * the LoadFunc(s) automatically selected based on the file extension. <br/>
 * <b>How this works:<b/><br/>
 * The file extensions are mapped in the pig.properties via the property
 * file.extension.loaders.
 * 
 * <p/>
 * <b>file.extension.loaders format</b>
 * <ul>
 * <li>[file extension]:[loader func spec]</li>
 * <li>[file-extension]:[optional path tag]:[loader func spec]</li>
 * <li>[file-extension]:[optional path tag]:[sequence file key value writer
 * class name]:[loader func spec]</li>
 * </ul>
 * 
 * <p/>
 * The file.extension.loaders property associate pig loaders with file
 * extensions, if a file does not have an extension the AllLoader will look at
 * the first three bytes of a file and try to guess its format bassed on:
 * <ul>
 * <li>[ -119, 76, 90 ] = lzo</li>
 * <li>[ 31, -117, 8 ] = gz</li>
 * <li>[ 66, 90, 104 ] = bz2</li>
 * <li>[ 83, 69, 81 ] = seq</li>
 * </ul>
 * <br/>
 * The loader associated with that extension will then be used.
 * <p/>
 * 
 * <b>Path partitioning</b> The AllLoader supports hive style path partitioning
 * e.g. /log/type1/daydate=2010-11-01<br/>
 * "daydate" will be considered a partition key and filters can be written
 * against this.<br/>
 * Note that the filter should go into the AllLoader contructor e.g.<br/>
 * a = LOAD 'input' using AllLoader('daydate<\"2010-11-01\"')<br/>
 * 
 * <b>Path tags</b> AllLoader supports configuring different loaders for the
 * same extension based on there file path.<br/>
 * E.g.<br/>
 * We have the paths /log/type1, /log/type2<br/>
 * For each of these directories we'd like to use different loaders.<br/>
 * So we use setup our loaders:<br/>
 * file.extension.loaders:gz:type1:MyType1Loader, gz:type2:MyType2Loader<br/>
 * 
 * 
 * <p/>
 * <b>Sequence files<b/> Sequence files also support using the Path tags for
 * loader selection but has an extra configuration option that relates to the
 * Key Class used to write the Sequence file.<br/>
 * E.g. for HiveRC this value is: org.apache.hadoop.hive.ql.io.RCFile so we can
 * setup our sequence file formatting:<br/>
 * file.extension.loaders:seq::org.apache.hadoop.hive.ql.io.RCFile:
 * MyHiveRCLoader, seq::DefaultSequenceFileLoader<br/>
 * 
 * <p/>
 * <b>Schema</b> The JsoneMetadata schema loader is supported and the schema
 * will be loaded using this loader.<br/>
 * In case this fails, the schema can be loaded using the default schema
 * provided.
 * 
 */
public class AllLoader extends FileInputLoadFunc implements LoadMetadata,
        StoreMetadata, LoadPushDown {

    private static final Logger LOG = Logger.getLogger(AllLoader.class);

    private static final String PROJECTION_ID = AllLoader.class.getName()
            + ".projection";

    transient LoadFunc childLoadFunc;
    transient boolean supportPushDownProjection = false;
    transient RequiredFieldList requiredFieldList;
    transient SortedSet<Integer> requiredFieldHashSet;

    transient TupleFactory tupleFactory = TupleFactory.getInstance();
    transient ResourceSchema schema;

    String signature;

    /**
     * Implements the logic for searching partition keys and applying parition
     * filtering
     */
    transient PathPartitionHelper pathPartitionerHelper = new PathPartitionHelper();
    transient Map<String, String> currentPathPartitionKeyMap;
    transient String[] partitionColumns;

    transient JsonMetadata jsonMetadata;
    transient boolean partitionKeysSet = false;

    LoadFuncHelper loadFuncHelper = null;

    transient Configuration conf;
    transient Path currentPath;

    String constructorPassedPartitionFilter;

    public AllLoader() {
        jsonMetadata = new JsonMetadata();
    }

    public AllLoader(String partitionFilter) {
        this();
        LOG.debug("PartitionFilter: " + partitionFilter.toString());

        constructorPassedPartitionFilter = partitionFilter;

    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
        // called on the front end
        conf = job.getConfiguration();
        loadFuncHelper = new LoadFuncHelper(conf);

        if (constructorPassedPartitionFilter != null) {

            pathPartitionerHelper.setPartitionFilterExpression(
                    constructorPassedPartitionFilter, AllLoader.class,
                    signature);

        }

        getPartitionKeys(location, job);
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return new Utf8StorageConverter();
    }

    @Override
    public AllLoaderInputFormat getInputFormat() throws IOException {
        // this plugs the AllLoaderInputFormat into the system, which in turn
        // will plug in the AllRecordReader
        // the AllRecordReader will select and create the correct LoadFunc
        return new AllLoaderInputFormat(signature);
    }

    @Override
    public void prepareToRead(
            @SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
            throws IOException {

        AllReader allReader = (AllReader) reader;

        if (currentPath == null || !(currentPath.equals(allReader.path))) {
            currentPathPartitionKeyMap = (partitionColumns == null) ? null
                    : pathPartitionerHelper
                            .getPathPartitionKeyValues(allReader.path
                                    .toString());
            currentPath = allReader.path;
        }

        childLoadFunc = allReader.prepareLoadFuncForReading(split);

        String projectProperty = getUDFContext().getProperty(PROJECTION_ID);

        if (projectProperty != null) {

            // load the required field list from the current UDF context
            ByteArrayInputStream input = new ByteArrayInputStream(
                    Base64.decodeBase64(projectProperty.getBytes("UTF-8")));

            ObjectInputStream objInput = new ObjectInputStream(input);

            try {
                requiredFieldList = (RequiredFieldList) objInput.readObject();
            } catch (ClassNotFoundException e) {
                throw new FrontendException(e.toString(), e);
            } finally {
                IOUtils.closeStream(objInput);
            }

            if (childLoadFunc.getClass().isAssignableFrom(LoadPushDown.class)) {
                supportPushDownProjection = true;
                ((LoadPushDown) childLoadFunc)
                        .pushProjection(requiredFieldList);
            } else {
                if (requiredFieldList != null) {
                    requiredFieldHashSet = new TreeSet<Integer>();
                    for (RequiredField requiredField : requiredFieldList
                            .getFields()) {
                        requiredFieldHashSet.add(requiredField.getIndex());
                    }
                }
            }

        }

    }

    @Override
    public Tuple getNext() throws IOException {
        // delegate work to the child load func selected based on the file type
        // and other criteria
        // We do support PushDown Projection if the LoadFunc does not so
        // in this method we need to look at the childLoadFunc flag
        // (supportPushDownProjection )
        // if true we use the getNext method as is, if not we remove the fields
        // not required in the spushDownProjection.

        Tuple tuple = null;

        if (supportPushDownProjection) {
            tuple = childLoadFunc.getNext();
        } else if ((tuple = childLoadFunc.getNext()) != null) {
            // ----- If the function does not support projection we do it here

            if (requiredFieldHashSet != null) {

                Tuple projectedTuple = tupleFactory
                        .newTuple(requiredFieldHashSet.size());
                int i = 0;
                int tupleSize = tuple.size();

                for (int index : requiredFieldHashSet) {
                    if (index < tupleSize) {
                        // add the tuple columns
                        projectedTuple.set(i++, tuple.get(index));
                    } else {
                        // add the partition columns
                        projectedTuple.set(i++, currentPathPartitionKeyMap
                                .get(partitionColumns[index - tupleSize]));
                    }
                }

                tuple = projectedTuple;
            }

        }

        return tuple;
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    @Override
    public RequiredFieldResponse pushProjection(
            RequiredFieldList requiredFieldList) throws FrontendException {
        // save the required field list to the UDFContext properties.

        Properties properties = getUDFContext();

        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        ObjectOutputStream objOut = null;
        try {
            objOut = new ObjectOutputStream(byteArray);
            objOut.writeObject(requiredFieldList);
        } catch (IOException e) {
            throw new FrontendException(e.toString(), e);
        } finally {
            IOUtils.closeStream(objOut);
        }

        // write out the whole required fields list as a base64 string
        try {
            properties.setProperty(PROJECTION_ID,
                    new String(Base64.encodeBase64(byteArray.toByteArray()),
                            "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new FrontendException(e.toString(), e);
        }

        return new RequiredFieldResponse(true);
    }

    /**
     * Tries to determine the LoadFunc by using the LoadFuncHelper to identify a
     * loader for the first file in the location directory.<br/>
     * If no LoadFunc can be determine ad FrontendException is thrown.<br/>
     * If the LoadFunc implements the LoadMetadata interface and returns a non
     * null schema this schema is returned.
     * 
     * @param location
     * @param job
     * @return
     * @throws IOException
     */
    private ResourceSchema getSchemaFromLoadFunc(String location, Job job)
            throws IOException {

        ResourceSchema schema = null;

        if (loadFuncHelper == null) {
            loadFuncHelper = new LoadFuncHelper(job.getConfiguration());
        }

        Path firstFile = loadFuncHelper.determineFirstFile(location);

        if (childLoadFunc == null) {

            // choose loader
            FuncSpec funcSpec = loadFuncHelper.determineFunction(location,
                    firstFile);

            if (funcSpec == null) {
                // throw front end exception, no loader could be determined.
                throw new FrontendException(
                        "No LoadFunction could be determined for " + location);
            }

            childLoadFunc = (LoadFunc) PigContext
                    .instantiateFuncFromSpec(funcSpec);
        }

        LOG.debug("Found LoadFunc:  " + childLoadFunc.getClass().getName());

        if (childLoadFunc instanceof LoadMetadata) {
            schema = ((LoadMetadata) childLoadFunc).getSchema(firstFile.toUri()
                    .toString(), job);
            LOG.debug("Found schema " + schema + " from loadFunc:  "
                    + childLoadFunc.getClass().getName());
        }

        return schema;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {

        if (schema == null) {
            ResourceSchema foundSchema = jsonMetadata.getSchema(location, job);

            // determine schema from files in location
            if (foundSchema == null) {
                foundSchema = getSchemaFromLoadFunc(location, job);

            }

            // only add the partition keys if the schema is not null
            // we use the partitionKeySet to only set partition keys once.
            if (!(partitionKeysSet || foundSchema == null)) {
                String[] keys = getPartitionColumns(location, job);

                if (!(keys == null || keys.length == 0)) {

                    // re-edit the pigSchema to contain the new partition keys.
                    ResourceFieldSchema[] fields = foundSchema.getFields();

                    LOG.debug("Schema: " + Arrays.toString(fields));

                    ResourceFieldSchema[] newFields = Arrays.copyOf(fields,
                            fields.length + keys.length);

                    int index = fields.length;

                    for (String key : keys) {
                        newFields[index++] = new ResourceFieldSchema(
                                new FieldSchema(key, DataType.CHARARRAY));
                    }

                    foundSchema.setFields(newFields);

                    LOG.debug("Added partition fields: " + keys
                            + " to loader schema");
                    LOG.debug("Schema is: " + Arrays.toString(newFields));
                }

                partitionKeysSet = true;

            }

            schema = foundSchema;
        }

        return schema;
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public void storeStatistics(ResourceStatistics stats, String location,
            Job job) throws IOException {

    }

    @Override
    public void storeSchema(ResourceSchema schema, String location, Job job)
            throws IOException {
        jsonMetadata.storeSchema(schema, location, job);
    }

    /**
     * Reads the partition columns
     * 
     * @param location
     * @param job
     * @return
     */
    private String[] getPartitionColumns(String location, Job job) {

        if (partitionColumns == null) {
            // read the partition columns from the UDF Context first.
            // if not in the UDF context then read it using the PathPartitioner.

            Properties properties = getUDFContext();

            if (properties == null) {
                properties = new Properties();
            }

            String partitionColumnStr = properties
                    .getProperty(PathPartitionHelper.PARTITION_COLUMNS);

            if (partitionColumnStr == null
                    && !(location == null || job == null)) {
                // if it hasn't been written yet.
                Set<String> partitionColumnSet;

                try {
                    partitionColumnSet = pathPartitionerHelper
                            .getPartitionKeys(location, job.getConfiguration());
                } catch (IOException e) {

                    RuntimeException rte = new RuntimeException(e);
                    rte.setStackTrace(e.getStackTrace());
                    throw rte;

                }

                if (partitionColumnSet != null) {

                    StringBuilder buff = new StringBuilder();

                    int i = 0;
                    for (String column : partitionColumnSet) {
                        if (i++ != 0) {
                            buff.append(',');
                        }

                        buff.append(column);
                    }

                    String buffStr = buff.toString().trim();

                    if (buffStr.length() > 0) {

                        properties.setProperty(
                                PathPartitionHelper.PARTITION_COLUMNS,
                                buff.toString());
                    }

                    partitionColumns = partitionColumnSet
                            .toArray(new String[] {});

                }

            } else {
                // the partition columns has been set already in the UDF Context
                if (partitionColumnStr != null) {
                    String split[] = partitionColumnStr.split(",");
                    Set<String> partitionColumnSet = new LinkedHashSet<String>();
                    if (split.length > 0) {
                        for (String splitItem : split) {
                            partitionColumnSet.add(splitItem);
                        }
                    }

                    partitionColumns = partitionColumnSet
                            .toArray(new String[] {});
                }

            }

        }

        return partitionColumns;

    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
            throws IOException {

        String[] partitionKeys = getPartitionColumns(location, job);

        if (partitionKeys == null) {
            throw new NullPointerException("INDUCED");
        }
        LOG.info("Get Parition Keys for: " + location + " keys: "
                + Arrays.toString(partitionKeys));

        return partitionKeys;
    }

    // --------------- Save Signature and PartitionFilter Expression
    // ----------------- //
    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature;
        super.setUDFContextSignature(signature);
    }

    private Properties getUDFContext() {
        return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                new String[] { signature });
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
        LOG.debug("PartitionFilter: " + partitionFilter.toString());

        pathPartitionerHelper.setPartitionFilterExpression(
                partitionFilter.toString(), AllLoader.class, signature);

    }

    /**
     * InputFormat that encapsulates the correct input format based on the file
     * type.
     * 
     */
    public static class AllLoaderInputFormat extends
            FileInputFormat<Writable, Writable> {

        transient PathPartitionHelper partitionHelper = new PathPartitionHelper();
        String udfSignature;

        public AllLoaderInputFormat(String udfSignature) {
            super();
            this.udfSignature = udfSignature;
        }

        @Override
        protected List<FileStatus> listStatus(JobContext jobContext)
                throws IOException {

            List<FileStatus> files = partitionHelper.listStatus(jobContext,
                    AllLoader.class, udfSignature);

            if (files == null)
                files = super.listStatus(jobContext);

            return files;

        }

        @Override
        public RecordReader<Writable, Writable> createRecordReader(
                InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {

            // this method plugs the AllReader into the system, and the
            // AllReader will when called select the correct LoadFunc
            // return new AllReader(udfSignature);
            return new AllReader(udfSignature);
        }

    }

    /**
     * This is where the logic is for selecting the correct Loader.
     * 
     */
    public static class AllReader extends RecordReader<Writable, Writable> {

        LoadFunc selectedLoadFunc;
        RecordReader<Writable, Writable> selectedReader;
        LoadFuncHelper loadFuncHelper = null;
        String udfSignature;
        Path path;

        public AllReader(String udfSignature) {
            this.udfSignature = udfSignature;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void initialize(InputSplit inputSplit,
                TaskAttemptContext taskAttemptContext) throws IOException,
                InterruptedException {

            FileSplit fileSplit = (FileSplit) inputSplit;

            path = fileSplit.getPath();
            String fileName = path.toUri().toString();

            // select the correct load function and initialise
            loadFuncHelper = new LoadFuncHelper(
                    taskAttemptContext.getConfiguration());

            FuncSpec funcSpec = loadFuncHelper.determineFunction(fileName);

            if (funcSpec == null) {
                throw new IOException("Cannot determine LoadFunc for "
                        + fileName);
            }

            selectedLoadFunc = (LoadFunc) PigContext
                    .instantiateFuncFromSpec(funcSpec);

            selectedLoadFunc.setUDFContextSignature(udfSignature);
            selectedLoadFunc.setLocation(fileName,
                    new Job(taskAttemptContext.getConfiguration(),
                            taskAttemptContext.getJobName()));

            selectedReader = selectedLoadFunc.getInputFormat()
                    .createRecordReader(fileSplit, taskAttemptContext);

            selectedReader.initialize(fileSplit, taskAttemptContext);

            LOG.info("Using LoadFunc " + selectedLoadFunc.getClass().getName()
                    + " on " + fileName);

        }

        // ---------------------- all functions below this line delegate work to
        // the selectedReader ------------//

        public LoadFunc prepareLoadFuncForReading(PigSplit split)
                throws IOException {

            selectedLoadFunc.prepareToRead(selectedReader, split);
            return selectedLoadFunc;

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return selectedReader.nextKeyValue();
        }

        @Override
        public Writable getCurrentKey() throws IOException,
                InterruptedException {
            return selectedReader.getCurrentKey();
        }

        @Override
        public Writable getCurrentValue() throws IOException,
                InterruptedException {
            return selectedReader.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return selectedReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            selectedReader.close();
        }

    }

}
