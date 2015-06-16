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
package org.apache.pig.builtin;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.Version;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.HadoopShimsSecure;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.Expression.BetweenExpression;
import org.apache.pig.Expression.Column;
import org.apache.pig.Expression.Const;
import org.apache.pig.Expression.InExpression;
import org.apache.pig.Expression.OpType;
import org.apache.pig.Expression.UnaryExpression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPredicatePushdown;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreResources;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.hive.HiveUtils;
import org.joda.time.DateTime;

import com.esotericsoftware.kryo.io.Input;
import com.google.common.annotations.VisibleForTesting;

/**
 * A load function and store function for ORC file.
 * An optional constructor argument is provided that allows one to customize
 * advanced behaviors. A list of available options is below:
 * <ul>
 * <li><code>-s, --stripeSize</code> Set the stripe size for the file
 * <li><code>-r, --rowIndexStride</code> Set the distance between entries in the row index
 * <li><code>-b, --bufferSize</code> The size of the memory buffers used for compressing and storing the
 * stripe in memory
 * <li><code>-p, --blockPadding</code> Sets whether the HDFS blocks are padded to prevent stripes
 * from straddling blocks
 * <li><code>-c, --compress</code> Sets the generic compression that is used to compress the data.
 * Valid codecs are: NONE, ZLIB, SNAPPY, LZO
 * <li><code>-v, --version</code> Sets the version of the file that will be written
 * </ul>
 **/
public class OrcStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata, LoadPushDown, LoadPredicatePushdown, StoreResources {

    //TODO Make OrcInputFormat.SARG_PUSHDOWN visible
    private static final String SARG_PUSHDOWN = "sarg.pushdown";

    protected RecordReader in = null;
    protected RecordWriter writer = null;
    private TypeInfo typeInfo = null;
    private ObjectInspector oi = null;
    private OrcSerde serde = new OrcSerde();
    private String signature;

    private Long stripeSize;
    private Integer rowIndexStride;
    private Integer bufferSize;
    private Boolean blockPadding;
    private CompressionKind compress;
    private Version version;

    private static final Options validOptions;
    private final CommandLineParser parser = new GnuParser();
    protected final static Log log = LogFactory.getLog(OrcStorage.class);
    protected boolean[] mRequiredColumns = null;

    private static final String SchemaSignatureSuffix = "_schema";
    private static final String RequiredColumnsSuffix = "_columns";
    private static final String SearchArgsSuffix = "_sarg";

    static {
        validOptions = new Options();
        validOptions.addOption("s", "stripeSize", true,
                "Set the stripe size for the file");
        validOptions.addOption("r", "rowIndexStride", true,
                "Set the distance between entries in the row index");
        validOptions.addOption("b", "bufferSize", true,
                "The size of the memory buffers used for compressing and storing the " +
                "stripe in memory");
        validOptions.addOption("p", "blockPadding", false, "Sets whether the HDFS blocks " +
                "are padded to prevent stripes from straddling blocks");
        validOptions.addOption("c", "compress", true,
                "Sets the generic compression that is used to compress the data");
        validOptions.addOption("v", "version", true,
                "Sets the version of the file that will be written");
    }

    public OrcStorage() {
    }

    public OrcStorage(String options) {
        String[] optsArr = options.split(" ");
        try {
            CommandLine configuredOptions = parser.parse(validOptions, optsArr);
            if (configuredOptions.hasOption('s')) {
                stripeSize = Long.parseLong(configuredOptions.getOptionValue('s'));
            }
            if (configuredOptions.hasOption('r')) {
                rowIndexStride = Integer.parseInt(configuredOptions.getOptionValue('r'));
            }
            if (configuredOptions.hasOption('b')) {
                bufferSize = Integer.parseInt(configuredOptions.getOptionValue('b'));
            }
            blockPadding = configuredOptions.hasOption('p');
            if (configuredOptions.hasOption('c')) {
                compress = CompressionKind.valueOf(configuredOptions.getOptionValue('c'));
            }
            if (configuredOptions.hasOption('v')) {
                version = Version.byName(configuredOptions.getOptionValue('v'));
            }
        } catch (ParseException e) {
            log.error("Exception in OrcStorage", e);
            log.error("OrcStorage called with arguments " + options);
            warn("ParseException in OrcStorage", PigWarning.UDF_WARNING_1);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("OrcStorage(',', '[options]')", validOptions);
            throw new RuntimeException(e);
        }
    }
    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
            throws IOException {
        return LoadFunc.getAbsolutePath(location, curDir);
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return new OrcNewOutputFormat();
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        if (!UDFContext.getUDFContext().isFrontend()) {
            if (stripeSize!=null) {
                job.getConfiguration().setLong(HiveConf.ConfVars.HIVE_ORC_DEFAULT_STRIPE_SIZE.varname,
                        stripeSize);
            }
            if (rowIndexStride!=null) {
                job.getConfiguration().setInt(HiveConf.ConfVars.HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE.varname,
                        rowIndexStride);
            }
            if (bufferSize!=null) {
                job.getConfiguration().setInt(HiveConf.ConfVars.HIVE_ORC_DEFAULT_BUFFER_SIZE.varname,
                        bufferSize);
            }
            if (blockPadding!=null) {
                job.getConfiguration().setBoolean(HiveConf.ConfVars.HIVE_ORC_DEFAULT_BLOCK_PADDING.varname,
                        blockPadding);
            }
            if (compress!=null) {
                job.getConfiguration().set(HiveConf.ConfVars.HIVE_ORC_DEFAULT_COMPRESS.varname,
                        compress.toString());
            }
            if (version!=null) {
                job.getConfiguration().set(HiveConf.ConfVars.HIVE_ORC_WRITE_FORMAT.varname,
                        version.getName());
            }
        }
        FileOutputFormat.setOutputPath(job, new Path(location));
        if (typeInfo==null) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            typeInfo = (TypeInfo)ObjectSerializer.deserialize(p.getProperty(signature + SchemaSignatureSuffix));
        }
        if (oi==null) {
            oi = HiveUtils.createObjectInspector(typeInfo);
        }
    }

    @Override
    public void checkSchema(ResourceSchema rs) throws IOException {
        ResourceFieldSchema fs = new ResourceFieldSchema();
        fs.setType(DataType.TUPLE);
        fs.setSchema(rs);
        typeInfo = HiveUtils.getTypeInfo(fs);
        Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
        p.setProperty(signature + SchemaSignatureSuffix, ObjectSerializer.serialize(typeInfo));
    }

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @Override
    public void putNext(Tuple t) throws IOException {
        try {
            writer.write(null, serde.serialize(t, oi));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        this.signature = signature;
    }

    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature;
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        StoreFunc.cleanupOnFailureImpl(location, job);
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
        if (!UDFContext.getUDFContext().isFrontend()) {
            typeInfo = (TypeInfo)ObjectSerializer.deserialize(p.getProperty(signature + SchemaSignatureSuffix));
        } else if (typeInfo == null) {
            typeInfo = getTypeInfo(location, job);
        }
        if (typeInfo != null && oi == null) {
            oi = OrcStruct.createObjectInspector(typeInfo);
        }
        if (!UDFContext.getUDFContext().isFrontend()) {
            if (p.getProperty(signature + RequiredColumnsSuffix) != null) {
                mRequiredColumns = (boolean[]) ObjectSerializer.deserialize(p
                        .getProperty(signature + RequiredColumnsSuffix));
                job.getConfiguration().setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
                job.getConfiguration().set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR,
                        getReqiredColumnIdString(mRequiredColumns));
                if (p.getProperty(signature + SearchArgsSuffix) != null) {
                    // Bug in setSearchArgument which always expects READ_COLUMN_NAMES_CONF_STR to be set
                    job.getConfiguration().set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR,
                            getReqiredColumnNamesString(getSchema(location, job), mRequiredColumns));
                }
            } else if (p.getProperty(signature + SearchArgsSuffix) != null) {
                // Bug in setSearchArgument which always expects READ_COLUMN_NAMES_CONF_STR to be set
                job.getConfiguration().set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR,
                        getReqiredColumnNamesString(getSchema(location, job)));
            }
            if (p.getProperty(signature + SearchArgsSuffix) != null) {
                job.getConfiguration().set(SARG_PUSHDOWN, p.getProperty(signature + SearchArgsSuffix));
            }

        }
        FileInputFormat.setInputPaths(job, location);
    }

    private String getReqiredColumnIdString(boolean[] requiredColumns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < requiredColumns.length; i++) {
            if (requiredColumns[i]) {
                sb.append(i).append(",");
            }
        }
        if (sb.charAt(sb.length() - 1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    private String getReqiredColumnNamesString(ResourceSchema schema) {
        StringBuilder sb = new StringBuilder();
        for (ResourceFieldSchema field : schema.getFields()) {
            sb.append(field.getName()).append(",");
        }
        if(sb.charAt(sb.length() -1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    private String getReqiredColumnNamesString(ResourceSchema schema, boolean[] requiredColumns) {
        StringBuilder sb = new StringBuilder();
        ResourceFieldSchema[] fields = schema.getFields();
        for (int i = 0; i < requiredColumns.length; i++) {
            if (requiredColumns[i]) {
                sb.append(fields[i]).append(",");
            }
        }
        if(sb.charAt(sb.length() - 1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new OrcNewInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        in = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }
            Object value = in.getCurrentValue();

            Tuple t = (Tuple)HiveUtils.convertHiveToPig(value, oi, mRequiredColumns);
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    @Override
    public List<String> getShipFiles() {
        List<String> cacheFiles = new ArrayList<String>();
        String hadoopVersion = "20S";
        if (Utils.isHadoop23() || Utils.isHadoop2()) {
            hadoopVersion = "23";
        }
        Class hadoopVersionShimsClass;
        try {
            hadoopVersionShimsClass = Class.forName("org.apache.hadoop.hive.shims.Hadoop" +
                    hadoopVersion + "Shims");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Cannot find Hadoop" + hadoopVersion + "ShimsClass in classpath");
        }
        Class[] classList = new Class[] {OrcFile.class, HiveConf.class, AbstractSerDe.class,
                org.apache.hadoop.hive.shims.HadoopShims.class, HadoopShimsSecure.class, hadoopVersionShimsClass,
                Input.class};
        return FuncUtils.getShipFiles(classList);
    }

    private static Path getFirstFile(String location, FileSystem fs) throws IOException {
        String[] locations = getPathStrings(location);
        Path[] paths = new Path[locations.length];
        for (int i = 0; i < paths.length; ++i) {
            paths[i] = new Path(locations[i]);
        }
        List<FileStatus> statusList = new ArrayList<FileStatus>();
        for (int i = 0; i < paths.length; ++i) {
            FileStatus[] files = fs.globStatus(paths[i]);
            if (files != null) {
                for (FileStatus tempf : files) {
                    statusList.add(tempf);
                }
            }
        }
        FileStatus[] statusArray = (FileStatus[]) statusList
                .toArray(new FileStatus[statusList.size()]);
        Path p = Utils.depthFirstSearchForFile(statusArray, fs);
        return p;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        if (typeInfo == null) {
            typeInfo = getTypeInfo(location, job);
            // still null means case of multiple load store
            if (typeInfo == null) {
                return null;
            }
        }

        ResourceFieldSchema fs = HiveUtils.getResourceFieldSchema(typeInfo);
        return fs.getSchema();
    }

    private TypeInfo getTypeInfo(String location, Job job) throws IOException {
        Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
        TypeInfo typeInfo = (TypeInfo) ObjectSerializer.deserialize(p.getProperty(signature + SchemaSignatureSuffix));
        if (typeInfo == null) {
            typeInfo = getTypeInfoFromLocation(location, job);
        }
        if (typeInfo != null) {
            p.setProperty(signature + SchemaSignatureSuffix, ObjectSerializer.serialize(typeInfo));
        }
        return typeInfo;
    }

    private TypeInfo getTypeInfoFromLocation(String location, Job job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path path = getFirstFile(location, fs);
        if (path == null) {
            log.info("Cannot find any ORC files from " + location +
                    ". Probably multiple load store in script.");
            return null;
        }
        Reader reader = OrcFile.createReader(fs, path);
        ObjectInspector oip = (ObjectInspector)reader.getObjectInspector();
        return TypeInfoUtils.getTypeInfoFromObjectInspector(oip);
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
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
        if (requiredFieldList == null)
            return null;
        if (requiredFieldList.getFields() != null)
        {
            int schemaSize = ((StructTypeInfo)typeInfo).getAllStructFieldTypeInfos().size();
            mRequiredColumns = new boolean[schemaSize];
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()!=-1)
                    mRequiredColumns[rf.getIndex()] = true;
            }
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            try {
                p.setProperty(signature + RequiredColumnsSuffix, ObjectSerializer.serialize(mRequiredColumns));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize mRequiredColumns");
            }
        }
        return new RequiredFieldResponse(true);
    }

    @Override
    public List<String> getPredicateFields(String location, Job job) throws IOException {
        ResourceSchema schema = getSchema(location, job);
        List<String> predicateFields = new ArrayList<String>();
        for (ResourceFieldSchema field : schema.getFields()) {
            switch(field.getType()) {
            case DataType.BOOLEAN:
            case DataType.INTEGER:
            case DataType.LONG:
            case DataType.FLOAT:
            case DataType.DOUBLE:
            case DataType.DATETIME:
            case DataType.CHARARRAY:
            case DataType.BIGINTEGER:
            case DataType.BIGDECIMAL:
                predicateFields.add(field.getName());
                break;
            default:
                // Skip DataType.BYTEARRAY, DataType.TUPLE, DataType.MAP and DataType.BAG
                break;
            }
        }
        return predicateFields;
    }

    @Override
    public List<OpType> getSupportedExpressionTypes() {
        List<OpType> types = new ArrayList<OpType>();
        types.add(OpType.OP_EQ);
        types.add(OpType.OP_NE);
        types.add(OpType.OP_GT);
        types.add(OpType.OP_GE);
        types.add(OpType.OP_LT);
        types.add(OpType.OP_LE);
        types.add(OpType.OP_IN);
        types.add(OpType.OP_BETWEEN);
        types.add(OpType.OP_NULL);
        types.add(OpType.OP_NOT);
        types.add(OpType.OP_AND);
        types.add(OpType.OP_OR);
        return types;
    }

    @Override
    public void setPushdownPredicate(Expression expr) throws IOException {
        SearchArgument sArg = getSearchArgument(expr);
        if (sArg != null) {
            log.info("Pushdown predicate expression is " + expr);
            log.info("Pushdown predicate SearchArgument is:\n" + sArg);
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            try {
                p.setProperty(signature + SearchArgsSuffix, sArg.toKryo());
            } catch (Exception e) {
                throw new IOException("Cannot serialize SearchArgument: " + sArg);
            }
        }
    }

    @VisibleForTesting
    SearchArgument getSearchArgument(Expression expr) {
        if (expr == null) {
            return null;
        }
        Builder builder = SearchArgumentFactory.newBuilder();
        boolean beginWithAnd = !(expr.getOpType().equals(OpType.OP_AND) || expr.getOpType().equals(OpType.OP_OR) || expr.getOpType().equals(OpType.OP_NOT));
        if (beginWithAnd) {
            builder.startAnd();
        }
        buildSearchArgument(expr, builder);
        if (beginWithAnd) {
            builder.end();
        }
        SearchArgument sArg = builder.build();
        return sArg;
    }

    private void buildSearchArgument(Expression expr, Builder builder) {
        if (expr instanceof BinaryExpression) {
            Expression lhs = ((BinaryExpression) expr).getLhs();
            Expression rhs = ((BinaryExpression) expr).getRhs();
            switch (expr.getOpType()) {
            case OP_AND:
                builder.startAnd();
                buildSearchArgument(lhs, builder);
                buildSearchArgument(rhs, builder);
                builder.end();
                break;
            case OP_OR:
                builder.startOr();
                buildSearchArgument(lhs, builder);
                buildSearchArgument(rhs, builder);
                builder.end();
                break;
            case OP_EQ:
                builder.equals(getColumnName(lhs), getExpressionValue(rhs));
                break;
            case OP_NE:
                builder.startNot();
                builder.equals(getColumnName(lhs), getExpressionValue(rhs));
                builder.end();
                break;
            case OP_LT:
                builder.lessThan(getColumnName(lhs), getExpressionValue(rhs));
                break;
            case OP_LE:
                builder.lessThanEquals(getColumnName(lhs), getExpressionValue(rhs));
                break;
            case OP_GT:
                builder.startNot();
                builder.lessThanEquals(getColumnName(lhs), getExpressionValue(rhs));
                builder.end();
                break;
            case OP_GE:
                builder.startNot();
                builder.lessThan(getColumnName(lhs), getExpressionValue(rhs));
                builder.end();
                break;
            case OP_BETWEEN:
                BetweenExpression between = (BetweenExpression) rhs;
                builder.between(getColumnName(lhs), getSearchArgObjValue(between.getLower()),  getSearchArgObjValue(between.getUpper()));
            case OP_IN:
                InExpression in = (InExpression) rhs;
                builder.in(getColumnName(lhs), getSearchArgObjValues(in.getValues()).toArray());
            default:
                throw new RuntimeException("Unsupported binary expression type: " + expr.getOpType() + " in " + expr);
            }
        } else if (expr instanceof UnaryExpression) {
            Expression unaryExpr = ((UnaryExpression) expr).getExpression();
            switch (expr.getOpType()) {
            case OP_NULL:
                builder.isNull(getColumnName(unaryExpr));
                break;
            case OP_NOT:
                builder.startNot();
                buildSearchArgument(unaryExpr, builder);
                builder.end();
                break;
            default:
                throw new RuntimeException("Unsupported unary expression type: " +
                        expr.getOpType() + " in " + expr);
            }
        } else {
            throw new RuntimeException("Unsupported expression type: " + expr.getOpType() + " in " + expr);
        }
    }

    private String getColumnName(Expression expr) {
        try {
            return ((Column) expr).getName();
        } catch (ClassCastException e) {
            throw new RuntimeException("Expected a Column but found " + expr.getClass().getName() +
                    " in expression " + expr, e);
        }
    }

    private Object getExpressionValue(Expression expr) {
        switch(expr.getOpType()) {
        case TERM_COL:
            return ((Column) expr).getName();
        case TERM_CONST:
            return getSearchArgObjValue(((Const) expr).getValue());
        default:
            throw new RuntimeException("Unsupported expression type: " + expr.getOpType() + " in " + expr);
        }
    }

    private List<Object> getSearchArgObjValues(List<Object> values) {
        if (!(values.get(0) instanceof BigInteger || values.get(0) instanceof BigDecimal || values.get(0) instanceof DateTime)) {
            return values;
        }
        List<Object> newValues = new ArrayList<Object>(values.size());
        for (Object value : values) {
            newValues.add(getSearchArgObjValue(value));
        }
        return values;
    }

    private Object getSearchArgObjValue(Object value) {
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger)value);
        } else if (value instanceof BigDecimal) {
            return value;
        } else if (value instanceof DateTime) {
            return new Timestamp(((DateTime)value).getMillis());
        } else {
            return value;
        }
    }

}
