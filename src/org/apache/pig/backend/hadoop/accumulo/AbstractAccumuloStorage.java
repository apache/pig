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
package org.apache.pig.backend.hadoop.accumulo;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.joda.time.DateTime;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Accumulo
 * 
 * A Key/Val pair will be returned as tuples: (key, colfam, colqual, colvis,
 * timestamp, value). All fields except timestamp are DataByteArray, timestamp
 * is a long.
 * 
 * Tuples can be written in 2 forms: (key, colfam, colqual, colvis, value) OR
 * (key, colfam, colqual, value)
 * 
 */
public abstract class AbstractAccumuloStorage extends LoadFunc implements
        StoreFuncInterface {
    private static final Log log = LogFactory
            .getLog(AbstractAccumuloStorage.class);

    protected static final char COLON = ':', COMMA = ',';
    protected static final String ASTERISK = "*";

    private static final String INPUT_PREFIX = AccumuloInputFormat.class
            .getSimpleName();
    private static final String OUTPUT_PREFIX = AccumuloOutputFormat.class
            .getSimpleName();

    private final static String STRING_CASTER = "UTF8StorageConverter";
    private final static String BYTE_CASTER = "AccumuloBinaryConverter";
    private final static String CASTER_PROPERTY = "pig.accumulo.caster";

    protected final AccumuloStorageOptions storageOptions;
    protected final CommandLine commandLine;

    private RecordReader<Key, Value> reader;
    private RecordWriter<Text, Mutation> writer;

    protected String inst;
    protected String zookeepers;
    protected String user;
    protected String password;
    protected String table;
    protected Text tableName;
    protected Authorizations authorizations;

    protected List<Column> columns;

    protected String start = null;
    protected String end = null;

    // Defaults from BatchWriterConfig
    protected int maxWriteThreads = 3;
    protected long maxMutationBufferSize = 50 * 1024 * 1024l;
    protected long maxLatency = Long.MAX_VALUE;

    protected String columnSeparator = ",";
    protected boolean ignoreWhitespace = true;

    protected LoadStoreCaster caster;
    protected ResourceSchema schema;
    protected String contextSignature = null;

    public AbstractAccumuloStorage(String columns, String args)
            throws ParseException, IOException {
        storageOptions = new AccumuloStorageOptions();
        commandLine = storageOptions.getCommandLine(args);

        // Extract any command line args
        extractArgs(commandLine, storageOptions);

        // Split out the user provided columns
        parseColumns(columns);
    }

    /**
     * Initializes {@link #columnDefs} and splits columns on {@link #COMMA}
     * 
     * @param columns
     *            CSV of columns
     */
    private void parseColumns(String columnStr) {
        columns = new LinkedList<Column>();
        if (ignoreWhitespace) {
            columnStr = StringUtils.strip(columnStr);
        }

        if (!columnStr.isEmpty()) {
            for (String column : StringUtils.split(columnStr, columnSeparator)) {
                columns.add(new Column(ignoreWhitespace ? StringUtils
                        .strip(column) : column));
            }
        } else {
            // Preserve original functionality for empty columns to fetch all
            // data in a map
            columns.add(new Column("*"));
        }
    }

    /**
     * Extract arguments passed into the constructor to avoid the URI
     * 
     * @param cli
     * @param opts
     */
    protected void extractArgs(CommandLine cli, AccumuloStorageOptions opts)
            throws IOException {
        if (opts.hasAuthorizations(cli)) {
            authorizations = opts.getAuthorizations(cli);
        }

        this.start = cli.getOptionValue(
                AccumuloStorageOptions.START_ROW_OPTION.getOpt(), null);
        this.end = cli.getOptionValue(
                AccumuloStorageOptions.END_ROW_OPTION.getOpt(), null);

        if (cli.hasOption(AccumuloStorageOptions.MAX_LATENCY_OPTION.getOpt())) {
            this.maxLatency = opts.getInt(cli,
                    AccumuloStorageOptions.MAX_LATENCY_OPTION);
        }

        if (cli.hasOption(AccumuloStorageOptions.WRITE_THREADS_OPTION.getOpt())) {
            this.maxWriteThreads = opts.getInt(cli,
                    AccumuloStorageOptions.WRITE_THREADS_OPTION);
        }

        if (cli.hasOption(AccumuloStorageOptions.MUTATION_BUFFER_SIZE_OPTION
                .getOpt())) {
            this.maxMutationBufferSize = opts.getLong(cli,
                    AccumuloStorageOptions.MUTATION_BUFFER_SIZE_OPTION);
        }

        Properties clientSystemProps = UDFContext.getUDFContext()
                .getClientSystemProps();
        String defaultCaster = STRING_CASTER;
        if (null != clientSystemProps) {
            defaultCaster = clientSystemProps.getProperty(CASTER_PROPERTY,
                    defaultCaster);
        }

        String casterOption = cli.getOptionValue("caster", defaultCaster);
        if (STRING_CASTER.equalsIgnoreCase(casterOption)) {
            caster = new Utf8StorageConverter();
        } else if (BYTE_CASTER.equalsIgnoreCase(casterOption)) {
            caster = new HBaseBinaryConverter();
        } else {
            try {
                caster = (LoadStoreCaster) PigContext
                        .instantiateFuncFromSpec(casterOption);
            } catch (ClassCastException e) {
                log.error("Configured caster does not implement LoadCaster interface.");
                throw new IOException(e);
            } catch (RuntimeException e) {
                log.error("Configured caster class not found.", e);
                throw new IOException(e);
            }
        }
        log.debug("Using caster " + caster.getClass());

        if (cli.hasOption(AccumuloStorageOptions.COLUMN_SEPARATOR_OPTION
                .getOpt())) {
            columnSeparator = cli
                    .getOptionValue(AccumuloStorageOptions.COLUMN_SEPARATOR_OPTION
                            .getOpt());
        }

        if (cli.hasOption(AccumuloStorageOptions.COLUMN_IGNORE_WHITESPACE_OPTION
                .getOpt())) {
            String value = cli
                    .getOptionValue(AccumuloStorageOptions.COLUMN_IGNORE_WHITESPACE_OPTION
                            .getOpt());
            if ("false".equalsIgnoreCase(value)) {
                ignoreWhitespace = false;
            } else if ("true".equalsIgnoreCase(value)) {
                ignoreWhitespace = true;
            } else {
                log.warn("Ignoring unknown value for "
                        + AccumuloStorageOptions.COLUMN_IGNORE_WHITESPACE_OPTION
                                .getOpt() + ": " + value);
            }
        }
    }

    protected CommandLine getCommandLine() {
        return commandLine;
    }

    protected Map<String, String> getInputFormatEntries(Configuration conf) {
        return getEntries(conf, INPUT_PREFIX);
    }

    protected Map<String, String> getOutputFormatEntries(Configuration conf) {
        return getEntries(conf, OUTPUT_PREFIX);
    }

    /**
     * Removes the given values from the configuration, accounting for changes
     * in the Configuration API given the version of Hadoop being used.
     * 
     * @param conf
     * @param entriesToUnset
     */
    protected void unsetEntriesFromConfiguration(Configuration conf,
            Map<String, String> entriesToUnset) {
        boolean configurationHasUnset = true;
        try {
            conf.getClass().getMethod("unset", String.class);
        } catch (NoSuchMethodException e) {
            configurationHasUnset = false;
        } catch (SecurityException e) {
            configurationHasUnset = false;
        }

        // Only Hadoop >=1.2.0 and >=0.23 actually contains the method
        // Configuration#unset
        if (configurationHasUnset) {
            simpleUnset(conf, entriesToUnset);
        } else {
            // If we're running on something else, we have to remove everything
            // and re-add it
            clearUnset(conf, entriesToUnset);
        }
    }

    /**
     * Unsets elements in the Configuration using the unset method
     * 
     * @param conf
     * @param entriesToUnset
     */
    protected void simpleUnset(Configuration conf,
            Map<String, String> entriesToUnset) {
        try {
            Method unset = conf.getClass().getMethod("unset", String.class);

            for (String key : entriesToUnset.keySet()) {
                unset.invoke(conf, key);
            }
        } catch (NoSuchMethodException e) {
            log.error("Could not invoke Configuration.unset method", e);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            log.error("Could not invoke Configuration.unset method", e);
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            log.error("Could not invoke Configuration.unset method", e);
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            log.error("Could not invoke Configuration.unset method", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Replaces the given entries in the configuration by clearing the
     * Configuration and re-adding the elements that aren't in the Map of
     * entries to unset
     * 
     * @param conf
     * @param entriesToUnset
     */
    protected void clearUnset(Configuration conf,
            Map<String, String> entriesToUnset) {
        // Gets a copy of the entries
        Iterator<Entry<String, String>> originalEntries = conf.iterator();
        conf.clear();

        while (originalEntries.hasNext()) {
            Entry<String, String> originalEntry = originalEntries.next();

            // Only re-set() the pairs that aren't in our collection of keys to
            // unset
            if (!entriesToUnset.containsKey(originalEntry.getKey())) {
                conf.set(originalEntry.getKey(), originalEntry.getValue());
            }
        }
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            // load the next pair
            if (!reader.nextKeyValue())
                return null;

            Key key = (Key) reader.getCurrentKey();
            Value value = (Value) reader.getCurrentValue();
            assert key != null && value != null;
            return getTuple(key, value);
        } catch (InterruptedException e) {
            throw new IOException(e.getMessage());
        }
    }

    protected abstract Tuple getTuple(Key key, Value value) throws IOException;

    @Override
    @SuppressWarnings("rawtypes")
    public InputFormat getInputFormat() {
        return new AccumuloInputFormat();
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void prepareToRead(RecordReader reader, PigSplit split) {
        this.reader = reader;
    }

    private void setLocationFromUri(String location) throws IOException {
        // ex:
        // accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC&fetch_columns=col1:cq1,col2:cq2&start=abc&end=z
        String columns = "", auths = "";
        try {
            if (!location.startsWith("accumulo://"))
                throw new Exception("Bad scheme.");
            String[] urlParts = location.split("\\?");
            if (urlParts.length > 1) {
                for (String param : urlParts[1].split("&")) {
                    String[] pair = param.split("=");
                    if (pair[0].equals("instance"))
                        inst = pair[1];
                    else if (pair[0].equals("user"))
                        user = pair[1];
                    else if (pair[0].equals("password"))
                        password = pair[1];
                    else if (pair[0].equals("zookeepers"))
                        zookeepers = pair[1];
                    else if (pair[0].equals("auths"))
                        auths = pair[1];
                    else if (pair[0].equals("fetch_columns"))
                        columns = pair[1];
                    else if (pair[0].equals("start"))
                        start = pair[1];
                    else if (pair[0].equals("end"))
                        end = pair[1];
                    else if (pair[0].equals("write_buffer_size_bytes"))
                        maxMutationBufferSize = Long.parseLong(pair[1]);
                    else if (pair[0].equals("write_threads"))
                        maxWriteThreads = Integer.parseInt(pair[1]);
                    else if (pair[0].equals("write_latency_ms"))
                        maxLatency = Long.parseLong(pair[1]);
                }
            }
            String[] parts = urlParts[0].split("/+");
            table = parts[1];
            tableName = new Text(table);

            if (null == authorizations && auths == null) {
                authorizations = new Authorizations();
            } else {
                authorizations = new Authorizations(StringUtils.split(auths,
                        COMMA));
            }

            if (!StringUtils.isEmpty(columns)) {
                parseColumns(columns);
            }

        } catch (Exception e) {
            throw new IOException(
                    "Expected 'accumulo://<table>[?instance=<instanceName>&user=<user>&password=<password>&zookeepers=<zookeepers>&auths=<authorizations>&"
                            + "[start=startRow,end=endRow,fetch_columns=[cf1:cq1,cf2:cq2,...],write_buffer_size_bytes=10000000,write_threads=10,write_latency_ms=30000]]': "
                            + e.getMessage());
        }
    }

    protected RecordWriter<Text, Mutation> getWriter() {
        return writer;
    }

    /**
     * Extract elements from the Configuration whose keys match the given prefix
     * 
     * @param conf
     * @param prefix
     * @return
     */
    protected Map<String, String> getEntries(Configuration conf, String prefix) {
        Map<String, String> entries = new HashMap<String, String>();

        for (Entry<String, String> entry : conf) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                entries.put(key, entry.getValue());
            }
        }

        return entries;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        setLocationFromUri(location);

        loadDependentJars(job.getConfiguration());

        Map<String, String> entries = getInputFormatEntries(job
                .getConfiguration());
        unsetEntriesFromConfiguration(job.getConfiguration(), entries);

        try {
            AccumuloInputFormat.setConnectorInfo(job, user, new PasswordToken(
                    password));
        } catch (AccumuloSecurityException e) {
            throw new IOException(e);
        }

        AccumuloInputFormat.setInputTableName(job, table);
        AccumuloInputFormat.setScanAuthorizations(job, authorizations);
        AccumuloInputFormat.setZooKeeperInstance(job, inst, zookeepers);

        List<Pair<Text, Text>> inputFormatColumns = new LinkedList<Pair<Text, Text>>();
        int colfamPrefix = 0;

        for (Column c : columns) {
            switch (c.getType()) {
            case LITERAL:
                // Pull the colf[:colq] individually
                inputFormatColumns.add(makePair(c.getColumnFamily(),
                        c.getColumnQualifier()));
                break;
            case COLFAM_PREFIX:
                // Some colfams
                colfamPrefix++;
                break;
            case COLQUAL_PREFIX:
                // Some colquals in a given colfam
                inputFormatColumns.add(makePair(c.getColumnFamily(), null));
                break;
            default:
                log.info("Ignoring unhandled column type");
                break;
            }
        }

        // If we have colfam prefixes, we have to pull all columns and filter on
        // client-side
        // TODO Create an iterator that lets us push-down *all* of the filter
        // logic
        if (0 == colfamPrefix && !inputFormatColumns.isEmpty()) {
            AccumuloInputFormat.fetchColumns(job, inputFormatColumns);
        }

        Collection<Range> ranges = Collections.singleton(new Range(start, end));

        log.info("Scanning Accumulo for " + ranges + " for table " + table);

        AccumuloInputFormat.setRanges(job, ranges);

        configureInputFormat(job);
    }

    protected Pair<Text, Text> makePair(String first, String second) {
        return new Pair<Text, Text>((null == first) ? null : new Text(first),
                (null == second) ? null : new Text(second));
    }

    /**
     * Ensure that Accumulo's dependent jars are added to the Configuration to
     * alleviate the need for clients to REGISTER dependency jars.
     * 
     * @param job
     *            The Mapreduce Job object
     * @throws IOException
     */
    protected void loadDependentJars(Configuration conf) throws IOException {
        // Thank you, HBase.
        Utils.addDependencyJars(conf,
                org.apache.accumulo.trace.instrument.Tracer.class,
                org.apache.accumulo.core.client.Instance.class,
                org.apache.accumulo.fate.Fate.class,
                org.apache.accumulo.server.tabletserver.TabletServer.class,
                org.apache.zookeeper.ZooKeeper.class,
                org.apache.thrift.TServiceClient.class);
    }

    /**
     * Method to allow specific implementations to add more elements to the Job
     * for reading data from Accumulo.
     * 
     * @param job
     */
    protected void configureInputFormat(Job job) {
    }

    /**
     * Method to allow specific implementations to add more elements to the Job
     * for writing data to Accumulo.
     * 
     * @param job
     */
    protected void configureOutputFormat(Job job) {
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir)
            throws IOException {
        return location;
    }

    @Override
    public void setUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    /* StoreFunc methods */
    public void setStoreFuncUDFContextSignature(String signature) {
        this.contextSignature = signature;

    }

    /**
     * Returns UDFProperties based on <code>contextSignature</code>.
     */
    protected Properties getUDFProperties() {
        return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                new String[] { contextSignature });
    }

    public String relToAbsPathForStoreLocation(String location, Path curDir)
            throws IOException {
        return relativeToAbsolutePath(location, curDir);
    }

    public void setStoreLocation(String location, Job job) throws IOException {
        setLocationFromUri(location);

        loadDependentJars(job.getConfiguration());

        Map<String, String> entries = getOutputFormatEntries(job
                .getConfiguration());
        unsetEntriesFromConfiguration(job.getConfiguration(), entries);

        try {
            AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(
                    password));
        } catch (AccumuloSecurityException e) {
            throw new IOException(e);
        }

        AccumuloOutputFormat.setCreateTables(job, true);
        AccumuloOutputFormat.setZooKeeperInstance(job, inst, zookeepers);

        BatchWriterConfig bwConfig = new BatchWriterConfig();
        bwConfig.setMaxLatency(maxLatency, TimeUnit.MILLISECONDS);
        bwConfig.setMaxMemory(maxMutationBufferSize);
        bwConfig.setMaxWriteThreads(maxWriteThreads);
        AccumuloOutputFormat.setBatchWriterOptions(job, bwConfig);

        log.info("Writing data to " + table);

        configureOutputFormat(job);
    }

    @SuppressWarnings("rawtypes")
    public OutputFormat getOutputFormat() {
        return new AccumuloOutputFormat();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void prepareToWrite(RecordWriter writer) {
        this.writer = writer;
    }

    protected abstract Collection<Mutation> getMutations(Tuple tuple)
            throws ExecException, IOException;

    public void putNext(Tuple tuple) throws ExecException, IOException {
        Collection<Mutation> muts = getMutations(tuple);
        for (Mutation mut : muts) {
            try {
                getWriter().write(tableName, mut);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    public void cleanupOnFailure(String failure, Job job) {
    }

    public void cleanupOnSuccess(String location, Job job) {
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        if (!(caster instanceof LoadStoreCaster)) {
            log.error("Caster must implement LoadStoreCaster for writing to Accumulo.");
            throw new IOException("Bad Caster " + caster.getClass());
        }
        schema = s;
        getUDFProperties().setProperty(contextSignature + "_schema",
                ObjectSerializer.serialize(schema));
    }

    protected Text tupleToText(Tuple tuple, int i,
            ResourceFieldSchema[] fieldSchemas) throws IOException {
        Object o = tuple.get(i);
        byte type = schemaToType(o, i, fieldSchemas);

        return objToText(o, type);
    }

    protected Text objectToText(Object o, ResourceFieldSchema fieldSchema)
            throws IOException {
        byte type = schemaToType(o, fieldSchema);

        return objToText(o, type);
    }

    protected byte schemaToType(Object o, ResourceFieldSchema fieldSchema) {
        return (fieldSchema == null) ? DataType.findType(o) : fieldSchema
                .getType();
    }

    protected byte schemaToType(Object o, int i,
            ResourceFieldSchema[] fieldSchemas) {
        return (fieldSchemas == null) ? DataType.findType(o) : fieldSchemas[i]
                .getType();
    }

    protected byte[] tupleToBytes(Tuple tuple, int i,
            ResourceFieldSchema[] fieldSchemas) throws IOException {
        Object o = tuple.get(i);
        byte type = schemaToType(o, i, fieldSchemas);

        return objToBytes(o, type);

    }

    protected Text objToText(Object o, byte type) throws IOException {
        byte[] bytes = objToBytes(o, type);

        if (null == bytes) {
            log.warn("Creating empty text from null value");
            return new Text();
        }

        return new Text(bytes);
    }

    @SuppressWarnings("unchecked")
    protected byte[] objToBytes(Object o, byte type) throws IOException {
        if (o == null)
            return null;
        switch (type) {
        case DataType.BYTEARRAY:
            return ((DataByteArray) o).get();
        case DataType.BAG:
            return caster.toBytes((DataBag) o);
        case DataType.CHARARRAY:
            return caster.toBytes((String) o);
        case DataType.DOUBLE:
            return caster.toBytes((Double) o);
        case DataType.FLOAT:
            return caster.toBytes((Float) o);
        case DataType.INTEGER:
            return caster.toBytes((Integer) o);
        case DataType.LONG:
            return caster.toBytes((Long) o);
        case DataType.BIGINTEGER:
            return caster.toBytes((BigInteger) o);
        case DataType.BIGDECIMAL:
            return caster.toBytes((BigDecimal) o);
        case DataType.BOOLEAN:
            return caster.toBytes((Boolean) o);
        case DataType.DATETIME:
            return caster.toBytes((DateTime) o);

            // The type conversion here is unchecked.
            // Relying on DataType.findType to do the right thing.
        case DataType.MAP:
            return caster.toBytes((Map<String, Object>) o);

        case DataType.NULL:
            return null;
        case DataType.TUPLE:
            return caster.toBytes((Tuple) o);
        case DataType.ERROR:
            throw new IOException("Unable to determine type of " + o.getClass());
        default:
            throw new IOException("Unable to find a converter for tuple field "
                    + o);
        }
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return caster;
    }
}
