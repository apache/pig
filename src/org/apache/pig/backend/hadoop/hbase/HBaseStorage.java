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
package org.apache.pig.backend.hadoop.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.hbase.HBaseTableInputFormat.HBaseTableIFBuilder;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;

import com.google.common.collect.Lists;

/**
 * A HBase implementation of LoadFunc and StoreFunc
 * 
 * TODO(dmitriy) test that all this stuff works
 * TODO(dmitriy) documentation
 */
public class HBaseStorage extends LoadFunc implements StoreFuncInterface, LoadPushDown, OrderedLoadFunc {
    
    private static final Log LOG = LogFactory.getLog(HBaseStorage.class);

    private final static String STRING_CASTER = "UTF8StorageConverter";
    private final static String BYTE_CASTER = "HBaseBinaryConverter";
    private final static String CASTER_PROPERTY = "pig.hbase.caster";
    
    private List<byte[][]> columnList_ = Lists.newArrayList();
    private HTable m_table;
    private Configuration m_conf;
    private RecordReader reader;
    private RecordWriter writer;
    private Scan scan;

    private final CommandLine configuredOptions_;
    private final static Options validOptions_ = new Options();
    private final static CommandLineParser parser_ = new GnuParser();
    private boolean loadRowKey_;
    private final long limit_;
    private final int caching_;

    protected transient byte[] gt_;
    protected transient byte[] gte_;
    protected transient byte[] lt_;
    protected transient byte[] lte_;

    private LoadCaster caster_;

    private ResourceSchema schema_;

    private static void populateValidOptions() { 
        validOptions_.addOption("loadKey", false, "Load Key");
        validOptions_.addOption("gt", true, "Records must be greater than this value " +
                "(binary, double-slash-escaped)");
        validOptions_.addOption("lt", true, "Records must be less than this value (binary, double-slash-escaped)");   
        validOptions_.addOption("gte", true, "Records must be greater than or equal to this value");
        validOptions_.addOption("lte", true, "Records must be less than or equal to this value");
        validOptions_.addOption("caching", true, "Number of rows scanners should cache");
        validOptions_.addOption("limit", true, "Per-region limit");
        validOptions_.addOption("caster", true, "Caster to use for converting values. A class name, " +
                "HBaseBinaryConverter, or Utf8StorageConverter. For storage, casters must implement LoadStoreCaster.");
    }

    /**
     * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or store the cells of the
     * provided columns.
     * 
     * @param columnList
     *            columnlist that is a presented string delimited by space.
     * @throws ParseException when unale to parse arguments
     * @throws IOException 
     */
    public HBaseStorage(String columnList) throws ParseException, IOException {
        this(columnList,"");
    }

    /**
     * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or store. 
     * @param columnList
     * @param optString Loader options. Known options:<ul>
     * <li>-loadKey=(true|false)  Load the row key as the first column
     * <li>-gt=minKeyVal
     * <li>-lt=maxKeyVal 
     * <li>-gte=minKeyVal
     * <li>-lte=maxKeyVal
     * <li>-limit=numRowsPerRegion max number of rows to retrieve per region
     * <li>-caching=numRows  number of rows to cache (faster scans, more memory).
     * </ul>
     * @throws ParseException 
     * @throws IOException 
     */
    public HBaseStorage(String columnList, String optString) throws ParseException, IOException {
        populateValidOptions();
        String[] colNames = columnList.split(" ");
        String[] optsArr = optString.split(" ");
        try {
            configuredOptions_ = parser_.parse(validOptions_, optsArr);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "[-loadKey] [-gt] [-gte] [-lt] [-lte] [-caching] [-caster]", validOptions_ );
            throw e;
        }

        loadRowKey_ = configuredOptions_.hasOption("loadKey");  
        for (String colName : colNames) {
            columnList_.add(Bytes.toByteArrays(colName.split(":")));
        }

        m_conf = HBaseConfiguration.create();
        String defaultCaster = m_conf.get(CASTER_PROPERTY, STRING_CASTER);
        String casterOption = configuredOptions_.getOptionValue("caster", defaultCaster);
        if (STRING_CASTER.equalsIgnoreCase(casterOption)) {
            caster_ = new Utf8StorageConverter();
        } else if (BYTE_CASTER.equalsIgnoreCase(casterOption)) {
            caster_ = new HBaseBinaryConverter();
        } else {
            try {
                @SuppressWarnings("unchecked")
                Class<LoadCaster> casterClass = (Class<LoadCaster>) Class.forName(casterOption);
                caster_ = casterClass.newInstance();
            } catch (ClassCastException e) {
                LOG.error("Congifured caster does not implement LoadCaster interface.");
                throw new IOException(e);
            } catch (ClassNotFoundException e) {
                LOG.error("Configured caster class not found.", e);
                throw new IOException(e);
            } catch (InstantiationException e) {
                LOG.error("Unable to instantiate configured caster " + casterOption, e);
                throw new IOException(e);
            } catch (IllegalAccessException e) {
                LOG.error("Illegal Access Exception for configured caster " + casterOption, e); 
                throw new IOException(e);
            }
        }

        caching_ = Integer.valueOf(configuredOptions_.getOptionValue("caching", "100"));
        limit_ = Long.valueOf(configuredOptions_.getOptionValue("limit", "-1"));
        initScan();	    
    }

    private void initScan() {
        scan = new Scan();
        // Set filters, if any.
        if (configuredOptions_.hasOption("gt")) {
            gt_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("gt")));
            addFilter(CompareOp.GREATER, gt_);
        }
        if (configuredOptions_.hasOption("lt")) {
            lt_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("lt")));
            addFilter(CompareOp.LESS, lt_);
        }
        if (configuredOptions_.hasOption("gte")) {
            gte_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("gte")));
            addFilter(CompareOp.GREATER_OR_EQUAL, gte_);
        }
        if (configuredOptions_.hasOption("lte")) {
            lte_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("lte")));
            addFilter(CompareOp.LESS_OR_EQUAL, lte_);
        }
    }

    private void addFilter(CompareOp op, byte[] val) {
        LOG.info("Adding filter " + op.toString() + " with value " + Bytes.toStringBinary(val));
        FilterList scanFilter = (FilterList) scan.getFilter();
        if (scanFilter == null) {
            scanFilter = new FilterList();
        }
        scanFilter.addFilter(new RowFilter(op, new BinaryComparator(val)));
        scan.setFilter(scanFilter);
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (reader.nextKeyValue()) {
                ImmutableBytesWritable rowKey = (ImmutableBytesWritable) reader
                .getCurrentKey();
                Result result = (Result) reader.getCurrentValue();
                int tupleSize=columnList_.size();
                if (loadRowKey_){
                    tupleSize++;
                }
                Tuple tuple=TupleFactory.getInstance().newTuple(tupleSize);

                int startIndex=0;
                if (loadRowKey_){
                    tuple.set(0, new DataByteArray(rowKey.get()));
                    startIndex++;
                }
                for (int i=0;i<columnList_.size();++i){
                     byte[] cell=result.getValue(columnList_.get(i)[0],columnList_.get(i)[1]);
                     if (cell!=null)
                         tuple.set(i+startIndex, new DataByteArray(cell));
                     else
                         tuple.set(i+startIndex, null);
                }
                return tuple;
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return null;
    }

    @Override
    public InputFormat getInputFormat() {      
        TableInputFormat inputFormat = new HBaseTableIFBuilder()
        .withLimit(limit_)
        .withGt(gt_)
        .withGte(gte_)
        .withLt(lt_)
        .withLte(lte_)
        .withConf(m_conf)
        .build();
        return inputFormat;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        this.reader = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        job.getConfiguration().setBoolean("pig.noSplitCombination", true);
        // Make sure the HBase, ZooKeeper, and Guava jars get shipped.
        TableMapReduceUtil.addDependencyJars(job.getConfiguration(), 
            org.apache.hadoop.hbase.client.HTable.class,
            com.google.common.collect.Lists.class,
            org.apache.zookeeper.ZooKeeper.class);

        String tablename = location;
        if (location.startsWith("hbase://")){
           tablename = location.substring(8);
        }
        if (m_table == null) {
            m_table = new HTable(m_conf, tablename);
        }
        HBaseConfiguration.addHbaseResources(m_conf);
        m_table.setScannerCaching(caching_);
        m_conf.set(TableInputFormat.INPUT_TABLE, tablename);
        for (byte[][] col : columnList_) {
            scan.addColumn(col[0], col[1]);
        }
        m_conf.set(TableInputFormat.SCAN, convertScanToString(scan));
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir)
    throws IOException {
        return location;
    }

    private static String convertScanToString(Scan scan) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            scan.write(dos);
            return Base64.encodeBytes(out.toByteArray());
        } catch (IOException e) {
            LOG.error(e);
            return "";
        }

    }

    /**
     * Set up the caster to use for reading values out of, and writing to, HBase. 
     */
    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return caster_;
    }
    
    /*
     * StoreFunc Methods
     * @see org.apache.pig.StoreFuncInterface#getOutputFormat()
     */
    
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        TableOutputFormat outputFormat = new TableOutputFormat();
        HBaseConfiguration.addHbaseResources(m_conf);
        outputFormat.setConf(m_conf);
        return outputFormat;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        if (! (caster_ instanceof LoadStoreCaster)) {
            LOG.error("Caster must implement LoadStoreCaster for writing to HBase.");
            throw new IOException("Bad Caster " + caster_.getClass());
        }
        schema_ = s;
    }

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple t) throws IOException {
        ResourceFieldSchema[] fieldSchemas = (schema_ == null) ? null : schema_.getFields();
        Put put=new Put(objToBytes(t.get(0), 
                (fieldSchemas == null) ? DataType.findType(t.get(0)) : fieldSchemas[0].getType()));
        long ts=System.currentTimeMillis();
        
        for (int i=1;i<t.size();++i){
            put.add(columnList_.get(i-1)[0], columnList_.get(i-1)[1], ts, objToBytes(t.get(i),
                    (fieldSchemas == null) ? DataType.findType(t.get(i)) : fieldSchemas[i].getType()));
        }
        try {
            writer.write(null, put);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    private byte[] objToBytes(Object o, byte type) throws IOException {
        LoadStoreCaster caster = (LoadStoreCaster) caster_;
        switch (type) {
        case DataType.BYTEARRAY: return ((DataByteArray) o).get();
        case DataType.BAG: return caster.toBytes((DataBag) o);
        case DataType.CHARARRAY: return caster.toBytes((String) o);
        case DataType.DOUBLE: return caster.toBytes((Double) o);
        case DataType.FLOAT: return caster.toBytes((Float) o);
        case DataType.INTEGER: return caster.toBytes((Integer) o);
        case DataType.LONG: return caster.toBytes((Long) o);
        
        // The type conversion here is unchecked. 
        // Relying on DataType.findType to do the right thing.
        case DataType.MAP: return caster.toBytes((Map<String, Object>) o);
        
        case DataType.NULL: return null;
        case DataType.TUPLE: return caster.toBytes((Tuple) o);
        case DataType.ERROR: throw new IOException("Unable to determine type of " + o.getClass());
        default: throw new IOException("Unable to find a converter for tuple field " + o);
        }
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
    throws IOException {
        return location;
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) { }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        if (location.startsWith("hbase://")){
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, location.substring(8));
        }else{
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, location);
        }
        m_conf = HBaseConfiguration.create(job.getConfiguration());
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
    }

    /*
     * LoadPushDown Methods.
     */
    
    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    @Override
    public RequiredFieldResponse pushProjection(
            RequiredFieldList requiredFieldList) throws FrontendException {
        List<RequiredField>  requiredFields = requiredFieldList.getFields();
        List<byte[][]> newColumns = Lists.newArrayListWithExpectedSize(requiredFields.size());
        
        // HBase Row Key is the first column in the schema when it's loaded, 
        // and is not included in the columnList (since it's not a proper column).
        int offset = loadRowKey_ ? 1 : 0;
        
        if (loadRowKey_) {
            if (requiredFields.size() < 1 || requiredFields.get(0).getIndex() != 0) {
                loadRowKey_ = false;
            } else {
                // We just processed the fact that the row key needs to be loaded.
                requiredFields.remove(0);
            }
        }
        
        for (RequiredField field : requiredFields) {
            int fieldIndex = field.getIndex();
            newColumns.add(columnList_.get(fieldIndex - offset));
        }
        LOG.debug("pushProjection After Projection: loadRowKey is " + loadRowKey_) ;
        for (byte[][] col : newColumns) {
            LOG.debug("pushProjection -- col: " + Bytes.toStringBinary(col[0]) + ":" + Bytes.toStringBinary(col[1]));
        }
        columnList_ = newColumns;
        return new RequiredFieldResponse(true);
    }

    @Override
    public WritableComparable<InputSplit> getSplitComparable(InputSplit split)
            throws IOException {
        return new WritableComparable<InputSplit>() {
            TableSplit tsplit = new TableSplit();

            @Override
            public void readFields(DataInput in) throws IOException {
                tsplit.readFields(in);
}

            @Override
            public void write(DataOutput out) throws IOException {
                tsplit.write(out);
            }

            @Override
            public int compareTo(InputSplit split) {
                return tsplit.compareTo((TableSplit) split);
            }
        };
    }

}
