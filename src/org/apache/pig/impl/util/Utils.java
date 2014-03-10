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
package org.apache.pig.impl.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.io.InterStorage;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.io.SequenceFileInterStorage;
import org.apache.pig.impl.io.TFileStorage;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.QueryParserDriver;

import com.google.common.collect.Lists;

/**
 * Class with utility static methods
 */
public class Utils {
    private static final Log log = LogFactory.getLog(Utils.class);
    /**
     * This method is a helper for classes to implement {@link java.lang.Object#equals(java.lang.Object)}
     * checks if two objects are equals - two levels of checks are
     * made - first if both are null or not null. If either is null,
     * check is made whether both are null.
     * If both are non null, equality also is checked if so indicated
     * @param obj1 first object to be compared
     * @param obj2 second object to be compared
     * @param checkEquality flag to indicate whether object equality should
     * be checked if obj1 and obj2 are non-null
     * @return true if the two objects are equal
     * false otherwise
     */
    public static boolean checkNullEquals(Object obj1, Object obj2, boolean checkEquality) {
        if(obj1 == null || obj2 == null) {
            return obj1 == obj2;
        }
        if(checkEquality) {
            if(!obj1.equals(obj2)) {
                return false;
            }
        }
        return true;
    }


    /**
     * This method is a helper for classes to implement {@link java.lang.Object#equals(java.lang.Object)}
     * The method checks whether the two arguments are both null or both not null and
     * whether they are of the same class
     * @param obj1 first object to compare
     * @param obj2 second object to compare
     * @return true if both objects are null or both are not null
     * and if both are of the same class if not null
     * false otherwise
     */
    public static boolean checkNullAndClass(Object obj1, Object obj2) {
        if(checkNullEquals(obj1, obj2, false)) {
            if(obj1 != null) {
                return obj1.getClass() == obj2.getClass();
            } else {
                return true; // both obj1 and obj2 should be null
            }
        } else {
            return false;
        }
    }

    /**
     * A helper function for retrieving the script schema set by the LOLoad
     * function.
     *
     * @param loadFuncSignature
     * @param conf
     * @return Schema
     * @throws IOException
     */
    public static Schema getScriptSchema(
            String loadFuncSignature,
            Configuration conf) throws IOException {
        Schema scriptSchema = null;
        String scriptField = conf.get(getScriptSchemaKey(loadFuncSignature));

        if (scriptField != null) {
            scriptSchema = (Schema) ObjectSerializer.deserialize(scriptField);
        }

        return scriptSchema;
    }

    public static String getScriptSchemaKey(String loadFuncSignature) {
        return loadFuncSignature + ".scriptSchema";
    }

    public static ResourceSchema getSchema(LoadFunc wrappedLoadFunc, String location, boolean checkExistence, Job job)
            throws IOException {
        Configuration conf = job.getConfiguration();
        if (checkExistence) {
            Path path = new Path(location);
            if (!FileSystem.get(conf).exists(path)) {
                // At compile time in batch mode, the file may not exist
                // (such as intermediate file). Just return null - the
                // same way as we would if we did not get a valid record
                return null;
            }
        }
        ReadToEndLoader loader = new ReadToEndLoader(wrappedLoadFunc, conf, location, 0);
        // get the first record from the input file
        // and figure out the schema from the data in
        // the first record
        Tuple t = loader.getNext();
        if (t == null) {
            // we couldn't get a valid record from the input
            return null;
        }
        int numFields = t.size();
        Schema s = new Schema();
        for (int i = 0; i < numFields; i++) {
            try {
                s.add(DataType.determineFieldSchema(t.get(i)));
            }
            catch (Exception e) {
                int errCode = 2104;
                String msg = "Error while determining schema of SequenceFileStorage data.";
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }
        return new ResourceSchema(s);
    }

    /**
     * @param schemaString a String representation of the Schema <b>without</b>
     *                     any enclosing curly-braces.<b>Not</b> for use with
     *                     <code>Schema#toString</code>
     * @return Schema instance
     * @throws ParserException
     */
    public static Schema getSchemaFromString(String schemaString) throws ParserException {
        LogicalSchema schema = parseSchema(schemaString);
        Schema result = org.apache.pig.newplan.logical.Util.translateSchema(schema);
        Schema.setSchemaDefaultType(result, DataType.BYTEARRAY);
        return result;
    }

    /**
     * getSchemaFromBagSchemaString
     * <b>NOTE: use this call</b> when you need to generate a Schema object
     * from the representation generated by <code>Schema#toString</code>.
     * This call strips the enclosing outer curly braces from the <code>toString</code>
     * representation, which are placed there because the actual representation of
     * the schema data is as a Bag-type relation.
     * @param schemaString a String representation of the Schema to instantiate,
     *                     in the form generated by <code>Schema.toString()</code>
     * @return Schema instance
     * @throws ParserException
     */
    public static Schema getSchemaFromBagSchemaString(String schemaString) throws ParserException {
        String unwrappedSchemaString = schemaString.substring(1, schemaString.length() - 1);
        return getSchemaFromString(unwrappedSchemaString);
    }

    public static LogicalSchema parseSchema(String schemaString) throws ParserException {
        QueryParserDriver queryParser = new QueryParserDriver( new PigContext(), 
                "util", new HashMap<String, String>() ) ;
        LogicalSchema schema = queryParser.parseSchema(schemaString);
        return schema;
    }

    /**
     * This method adds FieldSchema of 'input source tag/path' as the first
     * field. This will be called only when PigStorage is invoked with
     * '-tagFile' or '-tagPath' option and the schema file is present to be
     * loaded.
     * 
     * @param schema
     * @param fieldName
     * @return ResourceSchema
     */
    public static ResourceSchema getSchemaWithInputSourceTag(ResourceSchema schema, String fieldName) {
        ResourceFieldSchema[] fieldSchemas = schema.getFields();
        ResourceFieldSchema sourceTagSchema = new ResourceFieldSchema(new FieldSchema(fieldName, DataType.CHARARRAY));
        ResourceFieldSchema[] fieldSchemasWithSourceTag = new ResourceFieldSchema[fieldSchemas.length + 1];
        fieldSchemasWithSourceTag[0] = sourceTagSchema;
        for(int j = 0; j < fieldSchemas.length; j++) {
            fieldSchemasWithSourceTag[j + 1] = fieldSchemas[j];
        }
        return schema.setFields(fieldSchemasWithSourceTag);
    }

    private static enum TEMPFILE_CODEC {
        GZ (GzipCodec.class.getName()),
        GZIP (GzipCodec.class.getName()),
        LZO ("com.hadoop.compression.lzo.LzoCodec"),
        SNAPPY ("org.xerial.snappy.SnappyCodec"),
        BZIP2 (BZip2Codec.class.getName());

        private String hadoopCodecClassName;

        TEMPFILE_CODEC(String codecClassName) {
            this.hadoopCodecClassName = codecClassName;
        }

        public String lowerName() {
            return this.name().toLowerCase();
        }

        public String getHadoopCodecClassName() {
            return this.hadoopCodecClassName;
        }
    }

    private static enum TEMPFILE_STORAGE {
        INTER(InterStorage.class,
                null),
        TFILE(TFileStorage.class,
                Arrays.asList(TEMPFILE_CODEC.GZ,
                        TEMPFILE_CODEC.GZIP,
                        TEMPFILE_CODEC.LZO)),
        SEQFILE(SequenceFileInterStorage.class,
                Arrays.asList(TEMPFILE_CODEC.GZ,
                        TEMPFILE_CODEC.GZIP,
                        TEMPFILE_CODEC.LZO,
                        TEMPFILE_CODEC.SNAPPY,
                        TEMPFILE_CODEC.BZIP2));

        private Class<? extends FileInputLoadFunc> storageClass;
        private List<TEMPFILE_CODEC> supportedCodecs;

        TEMPFILE_STORAGE(
                Class<? extends FileInputLoadFunc> storageClass,
                List<TEMPFILE_CODEC> supportedCodecs) {
            this.storageClass = storageClass;
            this.supportedCodecs = supportedCodecs;
        }

        public String lowerName() {
            return this.name().toLowerCase();
        }

        public Class<? extends FileInputLoadFunc> getStorageClass() {
            return storageClass;
        }

        public boolean ensureCodecSupported(String codec) {
            try {
                return this.supportedCodecs.contains(TEMPFILE_CODEC.valueOf(codec.toUpperCase()));
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
        
        public String supportedCodecsToString() {
            StringBuffer sb = new StringBuffer();
            boolean first = true;
            for (TEMPFILE_CODEC codec : supportedCodecs) {
                if(first) {
                    first = false;
                } else {
                    sb.append(",");    
                }
                sb.append(codec.name());
            }
            return sb.toString();
        }
    }

    public static String getTmpFileCompressorName(PigContext pigContext) {
        if (pigContext == null)
            return InterStorage.class.getName();

        String codec = pigContext.getProperties().getProperty(PigConfiguration.PIG_TEMP_FILE_COMPRESSION_CODEC, "");
        if (codec.equals(TEMPFILE_CODEC.LZO.lowerName())) {
            pigContext.getProperties().setProperty("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
        }

        return getTmpFileStorage(pigContext.getProperties()).getStorageClass().getName();
    }

    public static FileInputLoadFunc getTmpFileStorageObject(Configuration conf) throws IOException {
        Class<? extends FileInputLoadFunc> storageClass = getTmpFileStorageClass(ConfigurationUtil.toProperties(conf));
        try {
            return storageClass.newInstance();
        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
    }

    public static Class<? extends FileInputLoadFunc> getTmpFileStorageClass(Properties properties) {
       return getTmpFileStorage(properties).getStorageClass();
    }

    private static TEMPFILE_STORAGE getTmpFileStorage(Properties properties) {
        boolean tmpFileCompression = properties.getProperty(
                PigConfiguration.PIG_ENABLE_TEMP_FILE_COMPRESSION, "false").equals("true");
        String tmpFileCompressionStorage =
                properties.getProperty(PigConfiguration.PIG_TEMP_FILE_COMPRESSION_STORAGE,
                        TEMPFILE_STORAGE.TFILE.lowerName());

        if (!tmpFileCompression) {
            return TEMPFILE_STORAGE.INTER;
        } else if (TEMPFILE_STORAGE.SEQFILE.lowerName().equals(tmpFileCompressionStorage)) {
            return TEMPFILE_STORAGE.SEQFILE;
        } else if (TEMPFILE_STORAGE.TFILE.lowerName().equals(tmpFileCompressionStorage)) {
            return TEMPFILE_STORAGE.TFILE;
        } else {
            throw new IllegalArgumentException("Unsupported storage format " + tmpFileCompressionStorage + 
                    ". Should be one of " + Arrays.toString(TEMPFILE_STORAGE.values()));
        }
    }

    public static void setMapredCompressionCodecProps(Configuration conf) {
        String codec = conf.get(
                PigConfiguration.PIG_TEMP_FILE_COMPRESSION_CODEC, "");
        if ("".equals(codec) && conf.get("mapred.output.compression.codec") != null) {
            conf.setBoolean("mapred.output.compress", true);
        } else if(TEMPFILE_STORAGE.SEQFILE.ensureCodecSupported(codec)) {
            conf.setBoolean("mapred.output.compress", true);
            conf.set("mapred.output.compression.codec", TEMPFILE_CODEC.valueOf(codec.toUpperCase()).getHadoopCodecClassName());
        }
        // no codec specified
    }

    public static void setTmpFileCompressionOnConf(PigContext pigContext, Configuration conf) throws IOException{
        // PIG-3741 This is also called for non-intermediate jobs, do not set any mapred properties here
        if (pigContext == null) {
            return;
        }
        TEMPFILE_STORAGE storage = getTmpFileStorage(pigContext.getProperties());
        String codec = pigContext.getProperties().getProperty(
                PigConfiguration.PIG_TEMP_FILE_COMPRESSION_CODEC, "");
        switch (storage) {
        case INTER:
            break;
        case SEQFILE:
            conf.set(PigConfiguration.PIG_TEMP_FILE_COMPRESSION_STORAGE, "seqfile");
            if("".equals(codec)) {
                // codec is not specified, ensure  is set
                log.warn("Temporary file compression codec is not specified. Using mapred.output.compression.codec property.");
                if(conf.get("mapred.output.compression.codec") == null) {
                    throw new IOException("mapred.output.compression.codec is not set");
                }
            } else if(storage.ensureCodecSupported(codec)) {
                // do nothing
            } else {
                throw new IOException("Invalid temporary file compression codec [" + codec + "]. " +
                        "Expected compression codecs for " + storage.getStorageClass().getName() + " are " + storage.supportedCodecsToString() + ".");
            }
            break;
        case TFILE:
            if(storage.ensureCodecSupported(codec)) {
                conf.set(PigConfiguration.PIG_TEMP_FILE_COMPRESSION_CODEC, codec.toLowerCase());
            } else {
                throw new IOException("Invalid temporary file compression codec [" + codec + "]. " +
                        "Expected compression codecs for " + storage.getStorageClass().getName() + " are " + storage.supportedCodecsToString() + ".");
            }
            break;
        }
    }

    public static String getStringFromArray(String[] arr) {
        StringBuilder str = new StringBuilder();
        for(String s: arr) {
            str.append(s);
            str.append(" ");
        }
        return str.toString();
    }

    public static FuncSpec buildSimpleFuncSpec(String className, byte...types) {
        List<Schema.FieldSchema> fieldSchemas = Lists.newArrayListWithExpectedSize(types.length);
        for (byte type : types) {
            fieldSchemas.add(new Schema.FieldSchema(null, type));
        }
        return new FuncSpec(className, new Schema(fieldSchemas));
    }

    /**
     * Replace sequences of two slashes ("\\") with one slash ("\")
     * (not escaping a slash in grunt is disallowed, but a double slash doesn't get converted
     * into a regular slash, so we have to do it instead)
     * @param str
     * @return the resulting string
     */
    public static String slashisize(String str) {
        return str.replace("\\\\", "\\");
    }

    @SuppressWarnings("unchecked")
    public static <O> Collection<O> mergeCollection(Collection<O> a, Collection<O> b) {
        if (a==null && b==null)
            return null;
        Collection<O> result = null;
        try {
            if (a!=null)
                result = a.getClass().newInstance();
            else
                result = b.getClass().newInstance();
        } catch (Exception e) {
            // Shall not happen
        }
        if (a==null) {
            result.addAll(b);
        }
        else if (b==null) {
            result.addAll(a);
        }
        else {
            result.addAll(a);
            for (O o : b) {
                if (!result.contains(o)) {
                    result.add(o);
                }
            }
        }

        return result;
    }

    public static InputStream getCompositeStream(InputStream in, Properties properties) {
        //Load default ~/.pigbootup if not specified by user
        final String bootupFile = properties.getProperty("pig.load.default.statements", System.getProperty("user.home") + "/.pigbootup");
        try {
            final InputStream inputSteam = new FileInputStream(new File(bootupFile));
            return new SequenceInputStream(inputSteam, in);
        } catch(FileNotFoundException fe) {
            log.info("Default bootup file " +bootupFile+ " not found");
            return in;
        }
    }

    /**
     * Method to apply pig properties to JobConf (replaces properties with
     * resulting jobConf values).
     *
     * @param conf JobConf with appropriate hadoop resource files
     * @param properties Pig properties that will override hadoop properties;
     * properties might be modified
     */
    public static void recomputeProperties(JobConf jobConf, Properties properties) {
        // We need to load the properties from the hadoop configuration
        // We want to override these with any existing properties we have.
        if (jobConf != null && properties != null) {
            // set user properties on the jobConf to ensure that defaults
            // and deprecation is applied correctly
            Enumeration<Object> propertiesIter = properties.keys();
            while (propertiesIter.hasMoreElements()) {
                String key = (String) propertiesIter.nextElement();
                String val = properties.getProperty(key);
                // We do not put user.name, See PIG-1419
                if (!key.equals("user.name")) {
                    jobConf.set(key, val);
                }
            }
            // clear user defined properties and re-populate
            properties.clear();
            Iterator<Map.Entry<String, String>> iter = jobConf.iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                properties.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public static String getStackStraceStr(Throwable e) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        e.printStackTrace(ps);
        return baos.toString();
    }

    /**
     * Returns whether the give path has a FileSystem implementation.
     *
     * @param path path
     * @param conf configuration
     * @return true if the give path's scheme has a FileSystem implementation,
     *         false otherwise
     */
    public static boolean hasFileSystemImpl(Path path, Configuration conf) {
        String scheme = path.toUri().getScheme();
        if (scheme != null) {
            String fsImpl = conf.get("fs." + scheme + ".impl");
            if (fsImpl == null) {
                return false;
            }
        }
        return true;
    }
    


    public static boolean isLocal(PigContext pigContext, Configuration conf) {
        return pigContext.getExecType().isLocal() || conf.getBoolean(PigImplConstants.CONVERTED_TO_LOCAL, false);
    }

}
