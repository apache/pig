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
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.orc.OrcUtils;

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
public class OrcStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata, LoadPushDown {
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
    private org.apache.hadoop.hive.ql.io.orc.OrcFile.Version version;
    
    private static final Options validOptions;
    private final CommandLineParser parser = new GnuParser();
    protected final Log log = LogFactory.getLog(getClass());
    protected boolean[] mRequiredColumns = null;
    
    private static final String SchemaSignatureSuffix = "_schema";
    private static final String RequiredColumnsSuffix = "_columns";
    
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
                        version.toString());
            }
        }
        FileOutputFormat.setOutputPath(job, new Path(location));
        if (typeInfo==null) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            typeInfo = (TypeInfo)ObjectSerializer.deserialize(p.getProperty(signature + SchemaSignatureSuffix));
        }
        if (oi==null) {
            oi = OrcUtils.createObjectInspector(typeInfo);
        }
    }

    @Override
    public void checkSchema(ResourceSchema rs) throws IOException {
        ResourceFieldSchema fs = new ResourceFieldSchema();
        fs.setType(DataType.TUPLE);
        fs.setSchema(rs);
        typeInfo = OrcUtils.getTypeInfo(fs);
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
        }
        if (typeInfo!=null && oi==null) {
            oi = OrcStruct.createObjectInspector(typeInfo);
        }
        if (!UDFContext.getUDFContext().isFrontend() &&
                p.getProperty(signature+RequiredColumnsSuffix)!=null) {
            mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature+RequiredColumnsSuffix));
            job.getConfiguration().setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, false);
            job.getConfiguration().set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, 
                    getReqiredColumnIdString(mRequiredColumns));
        }
        FileInputFormat.setInputPaths(job, location);
    }
    
    private static String getReqiredColumnIdString(boolean[] requiredColumns) {
        String result = "";
        for (int i=0;i<requiredColumns.length;i++) {
            if (requiredColumns[i]) {
                result += i;
                result += ",";
            }
        }
        if(result.endsWith(",")) {
            result = result.substring(0, result.length()-1);
        }
        return result;
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
            
            Tuple t = (Tuple)OrcUtils.convertOrcToPig(value, oi, mRequiredColumns);
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }
    
    private static Path getFirstFile(String location, FileSystem fs) throws IOException {
        String[] locations = getPathStrings(location);
        Path[] paths = new Path[locations.length];
        for (int i = 0; i < paths.length; ++i) {
            paths[i] = new Path(locations[i]);
        }
        List<FileStatus> statusList = new ArrayList<FileStatus>();
        for (int i = 0; i < paths.length; ++i) {
            for (FileStatus tempf : fs.globStatus(paths[i])) {
                statusList.add(tempf);
            }
        }
        FileStatus[] statusArray = (FileStatus[]) statusList
                .toArray(new FileStatus[statusList.size()]);
        Path p = Utils.depthFirstSearchForFile(statusArray, fs);
        return p;
    }

    private static TypeInfo getTypeInfoFromLocation(String location, Job job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path path = getFirstFile(location, fs);
        if (path==null) {
            throw new IOException("Cannot find any ORC files from " + location);
        }
        Reader reader = OrcFile.createReader(fs, path);
        ObjectInspector oip = (ObjectInspector)reader.getObjectInspector();
        return TypeInfoUtils.getTypeInfoFromObjectInspector(oip);
    }
    
    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        if (typeInfo==null) {
            typeInfo = getTypeInfoFromLocation(location, job);
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            p.setProperty(signature + SchemaSignatureSuffix, ObjectSerializer.serialize(typeInfo));
        }
        ResourceFieldSchema fs = OrcUtils.getResourceFieldSchema(typeInfo);
        return fs.getSchema();
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

}
