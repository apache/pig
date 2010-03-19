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
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.pig.ResourceStatistics;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
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

/**
 * Loader for Hive RC Columnar files.<br/>
 * Supports the following types:<br/>
 *  * <table>
 *  <tr><th>Hive Type</th><th>Pig Type from DataType</th></tr>
 *  <tr><td>string</td><td>CHARARRAY</td></tr>
 *  <tr><td>int</td><td>INTEGER</td></tr>
 *  <tr><td>bigint or long</td><td>LONG</td></tr>
 *  <tr><td>float</td><td>float</td></tr>
 *  <tr><td>double</td><td>DOUBLE</td></tr>
 *  <tr><td>boolean</td><td>BOOLEAN</td></tr>
 *  <tr><td>byte</td><td>BYTE</td></tr>
 *  <tr><td>array</td><td>TUPLE</td></tr>
 *  <tr><td>map</td><td>MAP</td></tr>
 * </table>
 * 
 *<br/>
 *
 *Usage 1:<br/>
 *To load a hive table: uid bigint, ts long, arr ARRAY<string,string>, m MAP<String, String>
 *<code>
 * a = LOAD 'file' USING HiveColumnarLoader("uid bigint, ts long, arr array<string,string>, m map<string,string>");
 *-- to reference the fields
 * b = FOREACH GENERATE a.uid, a.ts, a.arr, a.m; 
 *</code>
 *<p/>
 *Usage 2:<br/>
 *To load a hive table: uid bigint, ts long, arr ARRAY<string,string>, m MAP<String, String> only processing dates 2009-10-01 to 2009-10-02 in a <br/>
 *date partitioned hive table.<br/>
 *<code>
 * a = LOAD 'file' USING HiveColumnarLoader("uid bigint, ts long, arr array<string,string>, m map<string,string>", "2009-10-01:2009-10-02");
 *-- to reference the fields
 * b = FOREACH GENERATE a.uid, a.ts, a.arr, a.m; 
 *</code>
 *<p/>
 *Usage 3:<br/>
 *To load a hive table: uid bigint, ts long, arr ARRAY<string,string>, m MAP<String, String> only reading column uid and ts.<br/
 *<code>
 * a = LOAD 'file' USING HiveColumnarLoader("uid bigint, ts long, arr array<string,string>, m map<string,string>", "", "uid,ts");
 *-- to reference the fields
 * b = FOREACH a GENERATE uid, ts, arr, m; 
 *</code>
 *<p/>
 *Usage 4:<br/>
 *To load a hive table: uid bigint, ts long, arr ARRAY<string,string>, m MAP<String, String> only reading column uid and ts for dates 2009-10-01 to 2009-10-02.<br/
 *<code>
 * a = LOAD 'file' USING HiveColumnarLoader("uid bigint, ts long, arr array<string,string>, m map<string,string>", "2009-10-01:2009-10-02", "uid,ts");
 *-- to reference the fields
 * b = FOREACH a GENERATE uid, ts, arr, m; 
 *</code> 
 *<p/>
 *<b>Issues</b><p/>
 *<u>Table schema definition</u><br/>
 *The schema definition must be column name followed by a space then a comma then no space and the next column name and so on.<br/>
 *This so column1 string, column2 string will not word, it must be column1 string,column2 string
 *<p/>
 *<u>Date partitioning</u><br/>
 *Hive date partition folders must have format daydate=[date].
 **/
public class HiveColumnarLoader extends FileInputLoadFunc implements LoadMetadata, LoadPushDown{

    private static final String PROJECTION_ID = HiveColumnarLoader.class.getName() + ".projection";

    public static final String DAY_DATE_COLUMN = "daydate";

    private static final Text text = new Text();

    /**
     * Regex to filter out column names
     */
    protected static final Pattern pcols = Pattern.compile("[a-zA-Z_0-9]*[ ]");
    protected static final Log LOG = LogFactory.getLog(HiveColumnarLoader.class);
    protected TupleFactory tupleFactory = TupleFactory.getInstance();

    HiveRCRecordReader reader;

    ColumnarSerDe serde = null;
    Configuration conf = null;

    ResourceSchema pigSchema;

    String table_schema;

    BytesRefArrayWritable buff = null;

    String currentDate;

    private String dateRange = null;
    boolean applyDateRanges = false;

    private boolean applyColumnRead = false;
    private Properties props;
    private HiveConf hiveConf;
    private int[] columnToReadPositions;

    /**
     * Table schema should be a space and comma separated string describing the Hive schema.<br/>
     * For example uid BIGINT, pid long, means 1 column of uid type BIGINT and one column of pid type LONG.<br/>
     * The types are not case sensitive.
     * @param table_schema This property cannot be null
     */

    public HiveColumnarLoader(String table_schema) {
        //tells all read methods to not apply date range checking
        applyDateRanges = false;
        setup(table_schema, false, null);
    }

    /**
     * Table schema should be a space and comma separated string describing the Hive schema.<br/>
     * For example uid BIGINT, pid long, means 1 column of uid type BIGINT and one column of pid type LONG.<br/>
     * The types are not case sensitive.
     * @param table_schema This property cannot be null
     * @param dateRange must have format yyyy-MM-dd:yyy-MM-dd only dates between these two dates inclusively will be considered.
     */
    public HiveColumnarLoader(String table_schema, String dateRange) {  
        applyDateRanges = (dateRange != null && dateRange.trim().length() > 0);
        this.dateRange = dateRange;
        setup(table_schema, applyDateRanges, null);
    }


    public HiveColumnarLoader(String table_schema, String dateRange, String columns) {
        applyDateRanges = (dateRange != null && dateRange.trim().length() > 0);
        this.dateRange = dateRange;

        setup(table_schema, applyDateRanges, columns);
    }


    @Override
    public InputFormat<LongWritable, BytesRefArrayWritable> getInputFormat() throws IOException {
        return new HiveRCInputFormat(dateRange);
    }

    @Override
    public Tuple getNext() throws IOException {
        Tuple tuple = null;

        try {
            if(reader.nextKeyValue()){

                BytesRefArrayWritable buff = reader.getCurrentValue();
                ColumnarStruct struct = readColumnarStruct(buff);

                if(applyColumnRead) tuple = readColumnarTuple(struct);
                else tuple = readWholeRow(struct);
            }

        } catch (InterruptedException e) {
            throw new IOException(e.toString(), e);
        }


        return tuple;
    }


    @SuppressWarnings("unchecked")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
    throws IOException {

        this.reader = (HiveRCRecordReader)reader;

        // If the date range applies, set the location for each date range
        if(applyDateRanges)
            currentDate = HiveRCSchemaUtil.extractDayDate(this.reader.getSplitPath().toString());

        // All fields in a hive rc file are thrift serialized, the ColumnarSerDe is used for serialization and deserialization
        try {
            serde = new ColumnarSerDe();
            serde.initialize(hiveConf, props);
        } catch (SerDeException e) {
            LOG.error(e.toString(), e);
            throw new IOException(e);
        }

    }

    @Override
    public void setLocation(String locationStr, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, locationStr);
    }

    /**
     * Does the configuration setup and schema parsing and setup.
     * @param table_schema String
     * @param includeDayDateColumn boolean
     * @param columnsToRead String
     */
    private void setup(String table_schema, boolean includeDayDateColumn, String columnsToRead){


        if(table_schema == null)
            throw new RuntimeException("The table schema must be defined as colname type, colname type.  All types are hive types");

        this.table_schema = table_schema;

        //create basic configuration for hdfs and hive
        conf = new Configuration();
        hiveConf = new HiveConf(conf, SessionState.class);

        //parse the table_schema string
        List<String> types = HiveRCSchemaUtil.parseSchemaTypes(table_schema);
        List<String> cols = HiveRCSchemaUtil.parseSchema(pcols, table_schema);
        List<FieldSchema> fieldschema = null;

        //all columns must have types defined
        if(types.size() != cols.size())
            throw new RuntimeException("Each column in the schema must have a type defined");


        //check if previous projection exists
        if(columnsToRead == null){
            Properties properties = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            String projection = properties.getProperty(PROJECTION_ID);
            if(projection != null && !projection.isEmpty())
                columnsToRead = projection;
        }


        //re-check columnsToRead
        if (columnsToRead == null) {

            fieldschema = new ArrayList<FieldSchema>(cols.size());

            for(int i = 0; i < cols.size(); i++){
                fieldschema.add(new FieldSchema(cols.get(i), HiveRCSchemaUtil.findPigDataType(types.get(i))));
            }


        } else {
            //compile list for column filtering
            Set<String> columnToReadList = HiveRCSchemaUtil.compileSet(columnsToRead);

            if(columnToReadList.size() < 1)
                throw new RuntimeException("Error parsing columns: " + columnsToRead);

            applyColumnRead = true;
            int columnToReadLen = columnToReadList.size();

            fieldschema = new ArrayList<FieldSchema>(columnToReadLen);


            //--- create Pig Schema and add columnToReadPositions.
            columnToReadPositions = new int[columnToReadLen];

            int len = cols.size();
            String columnName = null;
            int colArrayPosindex = 0;

            for(int i = 0; i < len; i++){
                //i is the column position
                columnName = cols.get(i);
                if(columnToReadList.contains(columnName)){
                    //if the column is contained in the columnList then add its position to the columnPositions array and to the pig schema
                    columnToReadPositions[colArrayPosindex++] = i;

                    fieldschema.add(new FieldSchema(columnName, HiveRCSchemaUtil.findPigDataType(types.get(i))));
                }

            }


            //sort column positions
            Arrays.sort(columnToReadPositions);

        }

        if(includeDayDateColumn){
            fieldschema.add(new FieldSchema(DAY_DATE_COLUMN, DataType.CHARARRAY));
        }

        pigSchema = new ResourceSchema(new Schema(fieldschema));

        props =  new Properties();

        //   setting table schema properties for ColumnarSerDe
        //   these properties are never changed by the columns to read filter, because the columnar serde needs to now the 
        //   complete format of each record.
        props.setProperty(Constants.LIST_COLUMNS, HiveRCSchemaUtil.listToString(cols));
        props.setProperty(Constants.LIST_COLUMN_TYPES, HiveRCSchemaUtil.listToString(types));

    }

    /**
     * Uses the ColumnarSerde to deserialize the buff:BytesRefArrayWritable into a ColumnarStruct instance.
     * @param buff BytesRefArrayWritable
     * @return ColumnarStruct
     */
    private ColumnarStruct readColumnarStruct(BytesRefArrayWritable buff){
        //use ColumnarSerDe to deserialize row
        ColumnarStruct struct = null;
        try {
            struct = (ColumnarStruct)serde.deserialize(buff);
        } catch (SerDeException e) {
            LOG.error(e.toString(), e);
            throw new RuntimeException(e.toString(), e);
        }

        return struct;
    }
    /**
     * Only read the columns that were requested in the constructor.<br/> 
     * @param struct ColumnarStruct
     * @return Tuple
     * @throws IOException
     */
    private Tuple readColumnarTuple(ColumnarStruct struct) throws IOException{


        int columnToReadLen = columnToReadPositions.length;

        //create tuple with determined previous size
        Tuple t = tupleFactory.newTuple( (applyDateRanges)?columnToReadLen + 1 : columnToReadLen );

        int index = 0;

        //read in all columns
        for(int i = 0; i < columnToReadLen; i++){
            index = columnToReadPositions[i];

            Object obj = struct.getField(index, text);

            t.set(i, HiveRCSchemaUtil.extractPigTypeFromHiveType(obj));

        }

        if(applyDateRanges){
            //see creation of tuple if applyDateRanges == true the length of the tuple is columnToReadLen + 1
            t.set(columnToReadLen, currentDate);
        }


        return t;
    }

    /**
     * Read all columns in the row
     * @param struct
     * @return Tuple
     * @throws IOException
     */
    private Tuple readWholeRow(ColumnarStruct struct) throws IOException {

        //create tuple
        Tuple t = tupleFactory.newTuple();
        //read row fields
        List<Object> values = struct.getFieldsAsList(text);
        //for each value in the row convert to the correct pig type
        if(values != null && values.size() > 0){

            for(Object value : values){
                t.append(HiveRCSchemaUtil.extractPigTypeFromHiveType(value));
            }

        }

        if(applyDateRanges){
            t.append(currentDate);
        }

        return t;

    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
    throws IOException {
        return null;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
    throws IOException {
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
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    @Override
    public RequiredFieldResponse pushProjection(
            RequiredFieldList requiredFieldList) throws FrontendException {

        StringBuilder buff = new StringBuilder();
        ResourceFieldSchema[] fields = pigSchema.getFields();

        String fieldName = null;

        for(RequiredField f : requiredFieldList.getFields()){
            fieldName = fields[f.getIndex()].getName();
            if(!fieldName.equals(DAY_DATE_COLUMN))
                buff.append(fieldName).append(",");
        }

        String projectionStr = buff.substring(0, buff.length()-1);

        setup(table_schema, applyDateRanges, projectionStr);

        Properties properties = UDFContext.getUDFContext().getUDFProperties( 
                this.getClass());

        if(!projectionStr.isEmpty())
            properties.setProperty( PROJECTION_ID, projectionStr );

        return new RequiredFieldResponse(true);
    }


}
