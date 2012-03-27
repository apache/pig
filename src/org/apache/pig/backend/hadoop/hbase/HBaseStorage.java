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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
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
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.collect.Lists;

/**
 * A HBase implementation of LoadFunc and StoreFunc.
 * <P>
 * Below is an example showing how to load data from HBase:
 * <pre>{@code
 * raw = LOAD 'hbase://SampleTable'
 *       USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
 *       'info:first_name info:last_name friends:* info:*', '-loadKey true -limit 5')
 *       AS (id:bytearray, first_name:chararray, last_name:chararray, friends_map:map[], info_map:map[]);
 * }</pre>
 * This example loads data redundantly from the info column family just to
 * illustrate usage. Note that the row key is inserted first in the result schema.
 * To load only column names that start with a given prefix, specify the column
 * name with a trailing '*'. For example passing <code>friends:bob_*</code> to
 * the constructor in the above example would cause only columns that start with
 * <i>bob_</i> to be loaded.
 * <P>
 * Below is an example showing how to store data into HBase:
 * <pre>{@code
 * copy = STORE raw INTO 'hbase://SampleTableCopy'
 *       USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
 *       'info:first_name info:last_name friends:* info:*')
 *       AS (info:first_name info:last_name buddies:* info:*);
 * }</pre>
 * Note that STORE will expect the first value in the tuple to be the row key.
 * Scalars values need to map to an explicit column descriptor and maps need to
 * map to a column family name. In the above examples, the <code>friends</code>
 * column family data from <code>SampleTable</code> will be written to a
 * <code>buddies</code> column family in the <code>SampleTableCopy</code> table.
 * 
 */
public class HBaseStorage extends LoadFunc implements StoreFuncInterface, LoadPushDown, OrderedLoadFunc {
    
    private static final Log LOG = LogFactory.getLog(HBaseStorage.class);

    private final static String STRING_CASTER = "UTF8StorageConverter";
    private final static String BYTE_CASTER = "HBaseBinaryConverter";
    private final static String CASTER_PROPERTY = "pig.hbase.caster";
    private final static String ASTERISK = "*";
    private final static String COLON = ":";
    
    private List<ColumnInfo> columnInfo_ = Lists.newArrayList();
    private HTable m_table;
    private Configuration m_conf;
    private RecordReader reader;
    private RecordWriter writer;
    private TableOutputFormat outputFormat = null;
    private Scan scan;
    private String contextSignature = null;

    private final CommandLine configuredOptions_;
    private final static Options validOptions_ = new Options();
    private final static CommandLineParser parser_ = new GnuParser();
    private boolean loadRowKey_;
    private String delimiter_;
    private boolean ignoreWhitespace_;
    private final long limit_;
    private final int caching_;
    private final boolean noWAL_;

    protected transient byte[] gt_;
    protected transient byte[] gte_;
    protected transient byte[] lt_;
    protected transient byte[] lte_;

    private LoadCaster caster_;

    private ResourceSchema schema_;
    private RequiredFieldList requiredFieldList;

    private static void populateValidOptions() { 
        validOptions_.addOption("loadKey", false, "Load Key");
        validOptions_.addOption("gt", true, "Records must be greater than this value " +
                "(binary, double-slash-escaped)");
        validOptions_.addOption("lt", true, "Records must be less than this value (binary, double-slash-escaped)");   
        validOptions_.addOption("gte", true, "Records must be greater than or equal to this value");
        validOptions_.addOption("lte", true, "Records must be less than or equal to this value");
        validOptions_.addOption("caching", true, "Number of rows scanners should cache");
        validOptions_.addOption("limit", true, "Per-region limit");
        validOptions_.addOption("delim", true, "Column delimiter");
        validOptions_.addOption("ignoreWhitespace", true, "Ignore spaces when parsing columns");
        validOptions_.addOption("caster", true, "Caster to use for converting values. A class name, " +
                "HBaseBinaryConverter, or Utf8StorageConverter. For storage, casters must implement LoadStoreCaster.");
        validOptions_.addOption("noWAL", false, "Sets the write ahead to false for faster loading. To be used with extreme caution since this could result in data loss (see http://hbase.apache.org/book.html#perf.hbase.client.putwal).");
    }

    /**
     * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or store the cells of the
     * provided columns.
     * 
     * @param columnList
     *        columnlist that is a presented string delimited by space and/or
     *        commas. To retreive all columns in a column family <code>Foo</code>,
     *        specify a column as either <code>Foo:</code> or <code>Foo:*</code>.
     *        To fetch only columns in the CF that start with <I>bar</I>, specify
     *        <code>Foo:bar*</code>. The resulting tuple will always be the size
     *        of the number of tokens in <code>columnList</code>. Items in the
     *        tuple will be scalar values when a full column descriptor is
     *        specified, or a map of column descriptors to values when a column
     *        family is specified.
     *
     * @throws ParseException when unable to parse arguments
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
     * <li>-delim=char delimiter to use when parsing column names (default is space or comma)
     * <li>-ignoreWhitespace=(true|false) ignore spaces when parsing column names (default true)
     * <li>-caching=numRows  number of rows to cache (faster scans, more memory).
     * <li>-noWAL=(true|false) Sets the write ahead to false for faster loading.
     * To be used with extreme caution, since this could result in data loss
     * (see http://hbase.apache.org/book.html#perf.hbase.client.putwal).
     * </ul>
     * @throws ParseException 
     * @throws IOException 
     */
    public HBaseStorage(String columnList, String optString) throws ParseException, IOException {
        populateValidOptions();
        String[] optsArr = optString.split(" ");
        try {
            configuredOptions_ = parser_.parse(validOptions_, optsArr);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "[-loadKey] [-gt] [-gte] [-lt] [-lte] [-columnPrefix] [-caching] [-caster] [-noWAL] [-limit] [-delim] [-ignoreWhitespace]", validOptions_ );
            throw e;
        }

        loadRowKey_ = configuredOptions_.hasOption("loadKey");

        delimiter_ = ",";
        if (configuredOptions_.getOptionValue("delim") != null) {
          delimiter_ = configuredOptions_.getOptionValue("delim");
        }

        ignoreWhitespace_ = true;
        if (configuredOptions_.hasOption("ignoreWhitespace")) {
          String value = configuredOptions_.getOptionValue("ignoreWhitespace");
          if (!"true".equalsIgnoreCase(value)) {
            ignoreWhitespace_ = false;
          }
        }

        columnInfo_ = parseColumnList(columnList, delimiter_, ignoreWhitespace_);

        m_conf = HBaseConfiguration.create();
        String defaultCaster = UDFContext.getUDFContext().getClientSystemProps().getProperty(CASTER_PROPERTY, STRING_CASTER);
        String casterOption = configuredOptions_.getOptionValue("caster", defaultCaster);
        if (STRING_CASTER.equalsIgnoreCase(casterOption)) {
            caster_ = new Utf8StorageConverter();
        } else if (BYTE_CASTER.equalsIgnoreCase(casterOption)) {
            caster_ = new HBaseBinaryConverter();
        } else {
            try {
              caster_ = (LoadCaster) PigContext.instantiateFuncFromSpec(casterOption);
            } catch (ClassCastException e) {
                LOG.error("Configured caster does not implement LoadCaster interface.");
                throw new IOException(e);
            } catch (RuntimeException e) {
                LOG.error("Configured caster class not found.", e);
                throw new IOException(e);
            }
        }
        LOG.debug("Using caster " + caster_.getClass());

        caching_ = Integer.valueOf(configuredOptions_.getOptionValue("caching", "100"));
        limit_ = Long.valueOf(configuredOptions_.getOptionValue("limit", "-1"));
        noWAL_ = configuredOptions_.hasOption("noWAL");
        initScan();	    
    }

    /**
     * Returns UDFProperties based on <code>contextSignature</code>.
     */
    private Properties getUDFProperties() {
        return UDFContext.getUDFContext()
            .getUDFProperties(this.getClass(), new String[] {contextSignature});
    }

    /**
     * @return <code> contextSignature + "_projectedFields" </code>
     */
    private String projectedFieldsName() {
        return contextSignature + "_projectedFields";
    }

    /**
     *
     * @param columnList
     * @param delimiter
     * @param ignoreWhitespace
     * @return
     */
    private List<ColumnInfo> parseColumnList(String columnList,
                                             String delimiter,
                                             boolean ignoreWhitespace) {
        List<ColumnInfo> columnInfo = new ArrayList<ColumnInfo>();

        // Default behavior is to allow combinations of spaces and delimiter
        // which defaults to a comma. Setting to not ignore whitespace will
        // include the whitespace in the columns names
        String[] colNames = columnList.split(delimiter);
        if(ignoreWhitespace) {
            List<String> columns = new ArrayList<String>();

            for (String colName : colNames) {
                String[] subColNames = colName.split(" ");

                for (String subColName : subColNames) {
                    subColName = subColName.trim();
                    if (subColName.length() > 0) columns.add(subColName);
                }
            }

            colNames = columns.toArray(new String[columns.size()]);
        }

        for (String colName : colNames) {
            columnInfo.add(new ColumnInfo(colName));
        }

        return columnInfo;
    }

    private void initScan() {
        scan = new Scan();

        // Map-reduce jobs should not run with cacheBlocks
        scan.setCacheBlocks(false);

        // Set filters, if any.
        if (configuredOptions_.hasOption("gt")) {
            gt_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("gt")));
            addRowFilter(CompareOp.GREATER, gt_);
        }
        if (configuredOptions_.hasOption("lt")) {
            lt_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("lt")));
            addRowFilter(CompareOp.LESS, lt_);
        }
        if (configuredOptions_.hasOption("gte")) {
            gte_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("gte")));
            addRowFilter(CompareOp.GREATER_OR_EQUAL, gte_);
        }
        if (configuredOptions_.hasOption("lte")) {
            lte_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("lte")));
            addRowFilter(CompareOp.LESS_OR_EQUAL, lte_);
        }

        // apply any column filters
        FilterList allColumnFilters = null;
        for (ColumnInfo colInfo : columnInfo_) {
            // all column family filters roll up to one parent OR filter
            if (allColumnFilters == null) {
                allColumnFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            }

            // and each filter contains a column family filter
            FilterList thisColumnFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            thisColumnFilter.addFilter(new FamilyFilter(CompareOp.EQUAL,
                    new BinaryComparator(colInfo.getColumnFamily())));

            if (colInfo.isColumnMap()) {

                if (LOG.isInfoEnabled()) {
                    LOG.info("Adding family:prefix filters with values " +
                        Bytes.toString(colInfo.getColumnFamily()) + COLON +
                        Bytes.toString(colInfo.getColumnPrefix()));
                }

                // each column map filter consists of a FamilyFilter AND
                // optionally a PrefixFilter
                if (colInfo.getColumnPrefix() != null) {
                    thisColumnFilter.addFilter(new ColumnPrefixFilter(
                        colInfo.getColumnPrefix()));
                }
            }
            else {

                if (LOG.isInfoEnabled()) {
                    LOG.info("Adding family:descriptor filters with values " +
                        Bytes.toString(colInfo.getColumnFamily()) + COLON +
                        Bytes.toString(colInfo.getColumnName()));
                }

                // each column value filter consists of a FamilyFilter AND
                // a QualifierFilter
                thisColumnFilter.addFilter(new QualifierFilter(CompareOp.EQUAL,
                        new BinaryComparator(colInfo.getColumnName())));
            }

            allColumnFilters.addFilter(thisColumnFilter);
        }

        if (allColumnFilters != null) {
            addFilter(allColumnFilters);
        }
    }

    private void addRowFilter(CompareOp op, byte[] val) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Adding filter " + op.toString() +
                    " with value " + Bytes.toStringBinary(val));
        }
        addFilter(new RowFilter(op, new BinaryComparator(val)));
    }

    private void addFilter(Filter filter) {
        FilterList scanFilter = (FilterList) scan.getFilter();
        if (scanFilter == null) {
            scanFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        }
        scanFilter.addFilter(filter);
        scan.setFilter(scanFilter);
    }

  /**
   * Returns the ColumnInfo list for so external objects can inspect it. This
   * is available for unit testing. Ideally, the unit tests and the main source
   * would each mirror the same package structure and this method could be package
   * private.
   * @return ColumnInfo
   */
    public List<ColumnInfo> getColumnInfoList() {
      return columnInfo_;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (reader.nextKeyValue()) {
                ImmutableBytesWritable rowKey = (ImmutableBytesWritable) reader
                .getCurrentKey();
                Result result = (Result) reader.getCurrentValue();

                int tupleSize = columnInfo_.size();

                // use a map of families -> qualifiers with the most recent
                // version of the cell. Fetching multiple vesions could be a
                // useful feature.
                NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultsMap =
                        result.getNoVersionMap();

                if (loadRowKey_){
                    tupleSize++;
                }
                Tuple tuple=TupleFactory.getInstance().newTuple(tupleSize);

                int startIndex=0;
                if (loadRowKey_){
                    tuple.set(0, new DataByteArray(rowKey.get()));
                    startIndex++;
                }
                for (int i = 0;i < columnInfo_.size(); ++i){
                    int currentIndex = startIndex + i;

                    ColumnInfo columnInfo = columnInfo_.get(i);
                    if (columnInfo.isColumnMap()) {
                        // It's a column family so we need to iterate and set all
                        // values found
                        NavigableMap<byte[], byte[]> cfResults =
                                resultsMap.get(columnInfo.getColumnFamily());
                        Map<String, DataByteArray> cfMap =
                                new HashMap<String, DataByteArray>();

                        if (cfResults != null) {
                            for (byte[] quantifier : cfResults.keySet()) {
                                // We need to check against the prefix filter to
                                // see if this value should be included. We can't
                                // just rely on the server-side filter, since a
                                // user could specify multiple CF filters for the
                                // same CF.
                                if (columnInfo.getColumnPrefix() == null ||
                                        columnInfo.hasPrefixMatch(quantifier)) {

                                    byte[] cell = cfResults.get(quantifier);
                                    DataByteArray value =
                                            cell == null ? null : new DataByteArray(cell);
                                    cfMap.put(Bytes.toString(quantifier), value);
                                }
                            }
                        }
                        tuple.set(currentIndex, cfMap);
                    } else {
                        // It's a column so set the value
                        byte[] cell=result.getValue(columnInfo.getColumnFamily(),
                                                    columnInfo.getColumnName());
                        DataByteArray value =
                                cell == null ? null : new DataByteArray(cell);
                        tuple.set(currentIndex, value);
                    }
                }

                if (LOG.isDebugEnabled()) {
                    for (int i = 0; i < tuple.size(); i++) {
                        LOG.debug("tuple value:" + tuple.get(i));
                    }
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
    public void setUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        job.getConfiguration().setBoolean("pig.noSplitCombination", true);
        m_conf = initialiseHBaseClassLoaderResources(job);

        String tablename = location;
        if (location.startsWith("hbase://")){
           tablename = location.substring(8);
        }
        if (m_table == null) {
            m_table = new HTable(m_conf, tablename);
        }
        m_table.setScannerCaching(caching_);
        m_conf.set(TableInputFormat.INPUT_TABLE, tablename);

        String projectedFields = getUDFProperties().getProperty( projectedFieldsName() );
        if (projectedFields != null) {
            // update columnInfo_
            pushProjection((RequiredFieldList) ObjectSerializer.deserialize(projectedFields));
        }

        for (ColumnInfo columnInfo : columnInfo_) {
            // do we have a column family, or a column?
            if (columnInfo.isColumnMap()) {
                scan.addFamily(columnInfo.getColumnFamily());
            }
            else {
                scan.addColumn(columnInfo.getColumnFamily(),
                               columnInfo.getColumnName());
            }

        }
        if (requiredFieldList != null) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                    new String[] {contextSignature});
            p.setProperty(contextSignature + "_projectedFields", ObjectSerializer.serialize(requiredFieldList));
        }
        m_conf.set(TableInputFormat.SCAN, convertScanToString(scan));
    }

    private Configuration initialiseHBaseClassLoaderResources(Job job) throws IOException {
        Configuration hbaseConfig = initialiseHBaseConfig(job.getConfiguration());

        // Make sure the HBase, ZooKeeper, and Guava jars get shipped.
        TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
            org.apache.hadoop.hbase.client.HTable.class,
            com.google.common.collect.Lists.class,
            org.apache.zookeeper.ZooKeeper.class);

        return hbaseConfig;
    }

    private Configuration initialiseHBaseConfig(Configuration conf) {
        Configuration hbaseConfig = HBaseConfiguration.create();
        ConfigurationUtil.mergeConf(hbaseConfig, conf);
        return hbaseConfig;
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
        if (outputFormat == null) {
            this.outputFormat = new TableOutputFormat();
            m_conf = initialiseHBaseConfig(m_conf);
            this.outputFormat.setConf(m_conf);            
        }
        return outputFormat;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        if (! (caster_ instanceof LoadStoreCaster)) {
            LOG.error("Caster must implement LoadStoreCaster for writing to HBase.");
            throw new IOException("Bad Caster " + caster_.getClass());
        }
        schema_ = s;
        getUDFProperties().setProperty(contextSignature + "_schema",
                                       ObjectSerializer.serialize(schema_));
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
        byte type = (fieldSchemas == null) ? DataType.findType(t.get(0)) : fieldSchemas[0].getType();
        long ts=System.currentTimeMillis();

        Put put = createPut(t.get(0), type);

        if (LOG.isDebugEnabled()) {
            LOG.debug("putNext -- WAL disabled: " + noWAL_);
            for (ColumnInfo columnInfo : columnInfo_) {
                LOG.debug("putNext -- col: " + columnInfo);
            }
        }

        for (int i=1;i<t.size();++i){
            ColumnInfo columnInfo = columnInfo_.get(i-1);
            if (LOG.isDebugEnabled()) {
                LOG.debug("putNext - tuple: " + i + ", value=" + t.get(i) +
                        ", cf:column=" + columnInfo);
        }

            if (!columnInfo.isColumnMap()) {
                put.add(columnInfo.getColumnFamily(), columnInfo.getColumnName(),
                        ts, objToBytes(t.get(i), (fieldSchemas == null) ?
                        DataType.findType(t.get(i)) : fieldSchemas[i].getType()));
            } else {
                Map<String, Object> cfMap = (Map<String, Object>) t.get(i);
                for (String colName : cfMap.keySet()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("putNext - colName=" + colName +
                                  ", class: " + colName.getClass());
                    }
                    // TODO deal with the fact that maps can have types now. Currently we detect types at
                    // runtime in the case of storing to a cf, which is suboptimal.
                    put.add(columnInfo.getColumnFamily(), Bytes.toBytes(colName.toString()), ts,
                            objToBytes(cfMap.get(colName), DataType.findType(cfMap.get(colName))));
                }
            }
        }

        try {
            writer.write(null, put);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * Public method to initialize a Put. Used to allow assertions of how Puts
     * are initialized by unit tests.
     *
     * @param key
     * @param type
     * @return new put
     * @throws IOException
     */
    public Put createPut(Object key, byte type) throws IOException {
        Put put = new Put(objToBytes(key, type));

        if(noWAL_) {
            put.setWriteToWAL(false);
        }

        return put;
    }
    
    @SuppressWarnings("unchecked")
    private byte[] objToBytes(Object o, byte type) throws IOException {
        LoadStoreCaster caster = (LoadStoreCaster) caster_;
        if (o == null) return null;
        switch (type) {
        case DataType.BYTEARRAY: return ((DataByteArray) o).get();
        case DataType.BAG: return caster.toBytes((DataBag) o);
        case DataType.CHARARRAY: return caster.toBytes((String) o);
        case DataType.DOUBLE: return caster.toBytes((Double) o);
        case DataType.FLOAT: return caster.toBytes((Float) o);
        case DataType.INTEGER: return caster.toBytes((Integer) o);
        case DataType.LONG: return caster.toBytes((Long) o);
        case DataType.BOOLEAN: return caster.toBytes((Boolean) o);
        
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
    public void setStoreFuncUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        if (location.startsWith("hbase://")){
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, location.substring(8));
        }else{
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, location);
        }

        String serializedSchema = getUDFProperties().getProperty(contextSignature + "_schema");
        if (serializedSchema!= null) {
            schema_ = (ResourceSchema) ObjectSerializer.deserialize(serializedSchema);
        }

        m_conf = initialiseHBaseClassLoaderResources(job);
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
        List<ColumnInfo> newColumns = Lists.newArrayListWithExpectedSize(requiredFields.size());

        if (this.requiredFieldList != null) {
            // in addition to PIG, this is also called by this.setLocation().
            LOG.debug("projection is already set. skipping.");
            return new RequiredFieldResponse(true);
        }

        /* How projection is handled :
         *  - pushProjection() is invoked by PIG on the front end
         *  - pushProjection here both stores serialized projection in the
         *    context and adjusts columnInfo_.
         *  - setLocation() is invoked on the backend and it reads the
         *    projection from context. setLocation invokes this method again
         *    so that columnInfo_ is adjected.
         */

        // colOffset is the offset in our columnList that we need to apply to indexes we get from requiredFields
        // (row key is not a real column)
        int colOffset = loadRowKey_ ? 1 : 0;
        // projOffset is the offset to the requiredFieldList we need to apply when figuring out which columns to prune.
        // (if key is pruned, we should skip row key's element in this list when trimming colList)
        int projOffset = colOffset;
        this.requiredFieldList = requiredFieldList;

        if (requiredFieldList != null && requiredFields.size() > (columnInfo_.size() + colOffset)) {
            throw new FrontendException("The list of columns to project from HBase is larger than HBaseStorage is configured to load.");
        }

        // remember the projection
        try {
            getUDFProperties().setProperty( projectedFieldsName(),
                    ObjectSerializer.serialize(requiredFieldList) );
        } catch (IOException e) {
            throw new FrontendException(e);
        }

       if (loadRowKey_ &&
                ( requiredFields.size() < 1 || requiredFields.get(0).getIndex() != 0)) {
                loadRowKey_ = false;
            projOffset = 0;
            }
        
        for (int i = projOffset; i < requiredFields.size(); i++) {
            int fieldIndex = requiredFields.get(i).getIndex();
            newColumns.add(columnInfo_.get(fieldIndex - colOffset));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("pushProjection After Projection: loadRowKey is " + loadRowKey_) ;
            for (ColumnInfo colInfo : newColumns) {
                LOG.debug("pushProjection -- col: " + colInfo);
        }
        }
        columnInfo_ = newColumns;
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

    /**
     * Class to encapsulate logic around which column names were specified in each
     * position of the column list. Users can specify columns names in one of 4
     * ways: 'Foo:', 'Foo:*', 'Foo:bar*' or 'Foo:bar'. The first 3 result in a
     * Map being added to the tuple, while the last results in a scalar. The 3rd
     * form results in a prefix-filtered Map.
     */
    public class ColumnInfo {

        final String originalColumnName;  // always set
        final byte[] columnFamily; // always set
        final byte[] columnName; // set if it exists and doesn't contain '*'
        final byte[] columnPrefix; // set if contains a prefix followed by '*'

        public ColumnInfo(String colName) {
            originalColumnName = colName;
            String[] cfAndColumn = colName.split(COLON, 2);

            //CFs are byte[1] and columns are byte[2]
            columnFamily = Bytes.toBytes(cfAndColumn[0]);
            if (cfAndColumn.length > 1 &&
                    cfAndColumn[1].length() > 0 && !ASTERISK.equals(cfAndColumn[1])) {
                if (cfAndColumn[1].endsWith(ASTERISK)) {
                    columnPrefix = Bytes.toBytes(cfAndColumn[1].substring(0,
                            cfAndColumn[1].length() - 1));
                    columnName = null;
                }
                else {
                    columnName = Bytes.toBytes(cfAndColumn[1]);
                    columnPrefix = null;
                }
            } else {
              columnPrefix = null;
              columnName = null;
            }
        }

        public byte[] getColumnFamily() { return columnFamily; }
        public byte[] getColumnName() { return columnName; }
        public byte[] getColumnPrefix() { return columnPrefix; }
        public boolean isColumnMap() { return columnName == null; }

        public boolean hasPrefixMatch(byte[] qualifier) {
            return Bytes.startsWith(qualifier, columnPrefix);
        }

        @Override
        public String toString() { return originalColumnName; }
    }

}
