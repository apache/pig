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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStruct;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.piggybank.storage.hiverc.HiveRCInputFormat;
import org.apache.pig.piggybank.storage.hiverc.HiveRCRecordReader;
import org.apache.pig.piggybank.storage.hiverc.HiveRCSchemaUtil;
import org.apache.pig.piggybank.storage.partition.PathPartitionHelper;

/**
 * Loader for Hive RC Columnar files.<br/>
 * Supports the following types:<br/>
 * *
 * <table>
 * <tr>
 * <th>Hive Type</th>
 * <th>Pig Type from DataType</th>
 * </tr>
 * <tr>
 * <td>string</td>
 * <td>CHARARRAY</td>
 * </tr>
 * <tr>
 * <td>int</td>
 * <td>INTEGER</td>
 * </tr>
 * <tr>
 * <td>bigint or long</td>
 * <td>LONG</td>
 * </tr>
 * <tr>
 * <td>float</td>
 * <td>float</td>
 * </tr>
 * <tr>
 * <td>double</td>
 * <td>DOUBLE</td>
 * </tr>
 * <tr>
 * <td>boolean</td>
 * <td>BOOLEAN</td>
 * </tr>
 * <tr>
 * <td>byte</td>
 * <td>BYTE</td>
 * </tr>
 * <tr>
 * <td>array</td>
 * <td>TUPLE</td>
 * </tr>
 * <tr>
 * <td>map</td>
 * <td>MAP</td>
 * </tr>
 * </table>
 * 
 * <p/>
 * <b>Partitions</b><br/>
 * The input paths are scanned by the loader for [partition name]=[value]
 * patterns in the subdirectories.<br/>
 * If detected these partitions are appended to the table schema.<br/>
 * For example if you have the directory structure:<br/>
 * 
 * <pre>
 * /user/hive/warehouse/mytable
 * 				/year=2010/month=02/day=01
 * </pre>
 * 
 * The mytable schema is (id int,name string).<br/>
 * The final schema returned in pig will be (id:int, name:chararray,
 * year:chararray, month:chararray, day:chararray).<br/>
 * <p/>
 * Usage 1:
 * <p/>
 * To load a hive table: uid bigint, ts long, arr ARRAY<string,string>, m
 * MAP<String, String> <br/>
 * <code>
 * <pre>
 * a = LOAD 'file' USING HiveColumnarLoader("uid bigint, ts long, arr array<string,string>, m map<string,string>");
 * -- to reference the fields
 * b = FOREACH GENERATE a.uid, a.ts, a.arr, a.m;
 * </pre> 
 * </code>
 * <p/>
 * Usage 2:
 * <p/>
 * To load a hive table: uid bigint, ts long, arr ARRAY<string,string>, m
 * MAP<String, String> only processing dates 2009-10-01 to 2009-10-02 in a <br/>
 * date partitioned hive table.<br/>
 * <b>Old Usage</b><br/>
 * <b>Note:</b> The partitions can be filtered by using pig's FILTER operator.<br/>
 * <code>
 * <pre>
 * a = LOAD 'file' USING HiveColumnarLoader("uid bigint, ts long, arr array<string,string>, m map<string,string>", "2009-10-01:2009-10-02");
 * -- to reference the fields
 * b = FOREACH GENERATE a.uid, a.ts, a.arr, a.m;
 * </pre> 
 * </code> <br/>
 * <b>New Usage</b/><br/>
 * <code>
 * <pre>
 * a = LOAD 'file' USING HiveColumnarLoader("uid bigint, ts long, arr array<string,string>, m map<string,string>");
 * f = FILTER a BY daydate>='2009-10-01' AND daydate >='2009-10-02';
 * </pre>
 * </code>
 * <p/>
 * Usage 3:
 * <p/>
 * To load a hive table: uid bigint, ts long, arr ARRAY<string,string>, m
 * MAP<String, String> only reading column uid and ts for dates 2009-10-01 to
 * 2009-10-02.<br/ <br/>
 * <b>Old Usage</b><br/>
 * <b>Note:<b/> This behaviour is now supported in pig by LoadPushDown adding
 * the columns needed to be loaded like below is ignored and pig will
 * automatically send the columns used by the script to the loader.<br/>
 * <code>
 * <pre>
 * a = LOAD 'file' USING HiveColumnarLoader("uid bigint, ts long, arr array<string,string>, m map<string,string>");
 * f = FILTER a BY daydate>='2009-10-01' AND daydate >='2009-10-02';
 * -- to reference the fields
 * b = FOREACH a GENERATE uid, ts, arr, m;
 * </pre> 
 * </code>
 * <p/>
 * <b>Issues</b>
 * <p/>
 * <u>Table schema definition</u><br/>
 * The schema definition must be column name followed by a space then a comma
 * then no space and the next column name and so on.<br/>
 * This so column1 string, column2 string will not work, it must be column1
 * string,column2 string
 * <p/>
 * <u>Partitioning</u><br/>
 * Partitions must be in the format [partition name]=[partition value]<br/>
 * Only strings are supported in the partitioning.<br/>
 * Partitions must follow the same naming convention for all sub directories in
 * a table<br/>
 * For example:<br/>
 * The following is not valid:<br/>
 * 
 * <pre>
 *     mytable/hour=00
 *     mytable/day=01/hour=00
 * </pre>
 * 
 **/
public class HiveColumnarLoader extends FileInputLoadFunc implements
	LoadMetadata, LoadPushDown {

    public static final String PROJECTION_ID = HiveColumnarLoader.class
	    .getName() + ".projection";

    public static final String DATE_RANGE = HiveColumnarLoader.class.getName()
	    + ".date-range";

    /**
     * Regex to filter out column names
     */
    protected static final Pattern pcols = Pattern.compile("[a-zA-Z_0-9]*[ ]");
    protected static final Log LOG = LogFactory
	    .getLog(HiveColumnarLoader.class);

    protected TupleFactory tupleFactory = TupleFactory.getInstance();

    String signature = "";

    // we need to save the dateRange from the constructor if provided to add to
    // the UDFContext only when the signature is available.
    String dateRange = null;

    HiveRCRecordReader reader;

    ColumnarSerDe serde = null;
    Configuration conf = null;

    ResourceSchema pigSchema;
    boolean partitionKeysSet = false;

    BytesRefArrayWritable buff = null;

    private Properties props;
    private HiveConf hiveConf;

    transient int[] requiredColumns;

    transient Set<String> partitionColumns;

    /**
     * Implements the logic for searching partition keys and applying parition
     * filtering
     */
    transient PathPartitionHelper pathPartitionerHelper = new PathPartitionHelper();

    transient Path currentPath = null;
    transient Map<String, String> currentPathPartitionKeyMap;

    /**
     * Table schema should be a space and comma separated string describing the
     * Hive schema.<br/>
     * For example uid BIGINT, pid long, means 1 column of uid type BIGINT and
     * one column of pid type LONG.<br/>
     * The types are not case sensitive.
     * 
     * @param table_schema
     *            This property cannot be null
     */
    public HiveColumnarLoader(String table_schema) {
	setup(table_schema);
    }

    /**
     * This constructor is for backward compatibility.
     * 
     * Table schema should be a space and comma separated string describing the
     * Hive schema.<br/>
     * For example uid BIGINT, pid long, means 1 column of uid type BIGINT and
     * one column of pid type LONG.<br/>
     * The types are not case sensitive.
     * 
     * @param table_schema
     *            This property cannot be null
     * @param dateRange
     *            String
     * @param columns
     *            String not used any more
     */
    public HiveColumnarLoader(String table_schema, String dateRange,
	    String columns) {
	setup(table_schema);

	this.dateRange = dateRange;
    }

    /**
     * This constructor is for backward compatibility.
     * 
     * Table schema should be a space and comma separated string describing the
     * Hive schema.<br/>
     * For example uid BIGINT, pid long, means 1 column of uid type BIGINT and
     * one column of pid type LONG.<br/>
     * The types are not case sensitive.
     * 
     * @param table_schema
     *            This property cannot be null
     * @param dateRange
     *            String
     */
    public HiveColumnarLoader(String table_schema, String dateRange) {
	setup(table_schema);

	this.dateRange = dateRange;
    }

    private Properties getUDFContext() {
	return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
		new String[] { signature });
    }

    @Override
    public InputFormat<LongWritable, BytesRefArrayWritable> getInputFormat()
	    throws IOException {
	LOG.info("Signature: " + signature);
	return new HiveRCInputFormat(signature);
    }

    @Override
    public Tuple getNext() throws IOException {
	Tuple tuple = null;

	try {
	    if (reader.nextKeyValue()) {

		BytesRefArrayWritable buff = reader.getCurrentValue();
		ColumnarStruct struct = readColumnarStruct(buff);

		tuple = readColumnarTuple(struct, reader.getSplitPath());
	    }

	} catch (InterruptedException e) {
	    throw new IOException(e.toString(), e);
	}

	return tuple;
    }

    @Override
    public void prepareToRead(
	    @SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
	    throws IOException {

	this.reader = (HiveRCRecordReader) reader;

	// check that the required indexes actually exist i.e. the columns that
	// should be read.
	// assuming this is always defined simplifies the readColumnarTuple
	// logic.

	int requiredIndexes[] = getRequiredColumns();
	if (requiredIndexes == null) {

	    int fieldLen = pigSchema.getFields().length;

	    // if any the partition keys should already exist
	    String[] partitionKeys = getPartitionKeys(null, null);
	    if (partitionKeys != null) {
		fieldLen = partitionKeys.length;
	    }

	    requiredIndexes = new int[fieldLen];

	    for (int i = 0; i < fieldLen; i++) {
		requiredIndexes[i] = i;
	    }

	    this.requiredColumns = requiredIndexes;
	}

	try {
	    serde = new ColumnarSerDe();
	    serde.initialize(hiveConf, props);
	} catch (SerDeException e) {
	    LOG.error(e.toString(), e);
	    throw new IOException(e);
	}

    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
	FileInputFormat.setInputPaths(job, location);
    }

    /**
     * Does the configuration setup and schema parsing and setup.
     * 
     * @param table_schema
     *            String
     * @param columnsToRead
     *            String
     */
    private void setup(String table_schema) {

	if (table_schema == null)
	    throw new RuntimeException(
		    "The table schema must be defined as colname type, colname type.  All types are hive types");

	// create basic configuration for hdfs and hive
	conf = new Configuration();
	hiveConf = new HiveConf(conf, SessionState.class);

	// parse the table_schema string
	List<String> types = HiveRCSchemaUtil.parseSchemaTypes(table_schema);
	List<String> cols = HiveRCSchemaUtil.parseSchema(pcols, table_schema);

	List<FieldSchema> fieldSchemaList = new ArrayList<FieldSchema>(
		cols.size());

	for (int i = 0; i < cols.size(); i++) {
	    fieldSchemaList.add(new FieldSchema(cols.get(i), HiveRCSchemaUtil
		    .findPigDataType(types.get(i))));
	}

	pigSchema = new ResourceSchema(new Schema(fieldSchemaList));

	props = new Properties();

	// setting table schema properties for ColumnarSerDe
	// these properties are never changed by the columns to read filter,
	// because the columnar serde needs to now the
	// complete format of each record.
	props.setProperty(Constants.LIST_COLUMNS,
		HiveRCSchemaUtil.listToString(cols));
	props.setProperty(Constants.LIST_COLUMN_TYPES,
		HiveRCSchemaUtil.listToString(types));

    }

    /**
     * Uses the ColumnarSerde to deserialize the buff:BytesRefArrayWritable into
     * a ColumnarStruct instance.
     * 
     * @param buff
     *            BytesRefArrayWritable
     * @return ColumnarStruct
     */
    private ColumnarStruct readColumnarStruct(BytesRefArrayWritable buff) {
	// use ColumnarSerDe to deserialize row
	ColumnarStruct struct = null;
	try {
	    struct = (ColumnarStruct) serde.deserialize(buff);
	} catch (SerDeException e) {
	    LOG.error(e.toString(), e);
	    throw new RuntimeException(e.toString(), e);
	}

	return struct;
    }

    /**
     * Only read the columns that were requested in the constructor.<br/>
     * 
     * @param struct
     *            ColumnarStruct
     * @param path
     *            Path
     * @return Tuple
     * @throws IOException
     */
    private Tuple readColumnarTuple(ColumnarStruct struct, Path path)
	    throws IOException {

	int[] columnIndexes = getRequiredColumns();
	// the partition keys if any will already be in the UDFContext here.
	String[] partitionKeys = getPartitionKeys(null, null);
	// only if the path has changed should be run the
	if (currentPath == null || !currentPath.equals(path)) {
	    currentPathPartitionKeyMap = (partitionKeys == null) ? null
		    : pathPartitionerHelper.getPathPartitionKeyValues(path
			    .toString());
	    currentPath = path;
	}

	// if the partitionColumns is null this value will stop the for loop
	// below from trynig to add any partition columns
	// that do not exist
	int partitionColumnStartIndex = Integer.MAX_VALUE;

	if (!(partitionColumns == null || partitionColumns.size() == 0)) {
	    // partition columns are always appended to the schema fields.
	    partitionColumnStartIndex = pigSchema.getFields().length;

	}

	// create tuple with determined previous size
	Tuple t = tupleFactory.newTuple(columnIndexes.length);

	// read in all columns
	for (int i = 0; i < columnIndexes.length; i++) {
	    int columnIndex = columnIndexes[i];

	    if (columnIndex < partitionColumnStartIndex) {
		Object obj = struct.getField(columnIndex);
		Object pigType = HiveRCSchemaUtil
			.extractPigTypeFromHiveType(obj);

		t.set(i, pigType);

	    } else {
		// read the partition columns
		// will only be executed if partitionColumns is not null
		String key = partitionKeys[columnIndex
			- partitionColumnStartIndex];
		Object value = currentPathPartitionKeyMap.get(key);
		t.set(i, value);

	    }

	}

	return t;
    }

    /**
     * Will parse the required columns from the UDFContext properties if the
     * requiredColumns[] variable is null, or else just return the
     * requiredColumns.
     * 
     * @return int[]
     */
    private int[] getRequiredColumns() {

	if (requiredColumns == null) {
	    Properties properties = getUDFContext();

	    String projectionStr = properties.getProperty(PROJECTION_ID);

	    if (projectionStr != null) {
		String[] split = projectionStr.split(",");
		int columnIndexes[] = new int[split.length];

		int index = 0;
		for (String splitItem : split) {
		    columnIndexes[index++] = Integer.parseInt(splitItem);
		}

		requiredColumns = columnIndexes;
	    }

	}

	return requiredColumns;
    }

    /**
     * Reads the partition columns
     * 
     * @param location
     * @param job
     * @return
     */
    private Set<String> getPartitionColumns(String location, Job job) {

	if (partitionColumns == null) {
	    // read the partition columns from the UDF Context first.
	    // if not in the UDF context then read it using the PathPartitioner.

	    Properties properties = getUDFContext();

	    if (properties == null)
		properties = new Properties();

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

		    partitionColumns = partitionColumnSet;

		}

	    } else {
		// the partition columns has been set already in the UDF Context
		if (partitionColumnStr != null) {
		    String split[] = partitionColumnStr.split(",");
		    partitionColumns = new LinkedHashSet<String>();
		    if (split.length > 0) {
			for (String splitItem : split) {
			    partitionColumns.add(splitItem);
			}
		    }
		}

	    }

	}

	return partitionColumns;

    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
	    throws IOException {
	Set<String> partitionKeys = getPartitionColumns(location, job);

	return partitionKeys == null ? null : partitionKeys
		.toArray(new String[] {});
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
	    throws IOException {

	if (!partitionKeysSet) {
	    Set<String> keys = getPartitionColumns(location, job);

	    if (!(keys == null || keys.size() == 0)) {

		// re-edit the pigSchema to contain the new partition keys.
		ResourceFieldSchema[] fields = pigSchema.getFields();

		LOG.debug("Schema: " + Arrays.toString(fields));

		ResourceFieldSchema[] newFields = Arrays.copyOf(fields,
			fields.length + keys.size());

		int index = fields.length;

		for (String key : keys) {
		    newFields[index++] = new ResourceFieldSchema(
			    new FieldSchema(key, DataType.CHARARRAY));
		}

		pigSchema.setFields(newFields);

		LOG.debug("Added partition fields: " + keys
			+ " to loader schema");
		LOG.debug("Schema is: " + Arrays.toString(newFields));
	    }

	    partitionKeysSet = true;

	}

	return pigSchema;
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
	    throws IOException {
	return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
	    throws IOException {
	getUDFContext().setProperty(
		PathPartitionHelper.PARITITION_FILTER_EXPRESSION,
		partitionFilter.toString());
    }

    @Override
    public List<OperatorSet> getFeatures() {
	return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    @Override
    public RequiredFieldResponse pushProjection(
	    RequiredFieldList requiredFieldList) throws FrontendException {

	// save the required field list to the UDFContext properties.
	StringBuilder buff = new StringBuilder();

	int i = 0;
	for (RequiredField f : requiredFieldList.getFields()) {
	    if (i++ != 0)
		buff.append(',');

	    buff.append(f.getIndex());
	}

	Properties properties = getUDFContext();

	properties.setProperty(PROJECTION_ID, buff.toString());

	return new RequiredFieldResponse(true);
    }

    @Override
    public void setUDFContextSignature(String signature) {
	super.setUDFContextSignature(signature);

	LOG.debug("Signature: " + signature);
	this.signature = signature;
	
	// this provides backwards compatibility
	// the HiveRCInputFormat will read this and if set will perform the
	// needed partitionFiltering
	if (dateRange != null) {
	    getUDFContext().setProperty(DATE_RANGE, dateRange);
	}
	
    }

}
