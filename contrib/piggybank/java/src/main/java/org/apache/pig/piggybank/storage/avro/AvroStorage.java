/*
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

package org.apache.pig.piggybank.storage.avro;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * AvroStorage is used to load/store Avro data <br/>
 * Document can be found <a href='https://cwiki.apache.org/PIG/avrostorage.html'>here</a>
 */
public class AvroStorage extends FileInputLoadFunc implements StoreFuncInterface, LoadMetadata {

    /* storeFunc parameters */
    private static final String NOTNULL = "NOTNULL";
    private static final String AVRO_OUTPUT_SCHEMA_PROPERTY = "avro_output_schema";
    private static final String SCHEMA_DELIM = "#";
    private static final String SCHEMA_KEYVALUE_DELIM = "@";
    private static final String NO_SCHEMA_CHECK = "no_schema_check";
    private static final String IGNORE_BAD_FILES = "ignore_bad_files";
    private static final String MULTIPLE_SCHEMAS = "multiple_schemas";

    /* FIXME: we use this variable to distinguish schemas specified
     * by different AvroStorage calls and will remove this once
     * StoreFunc provides access to Pig output schema in backend.
     * Default value is 0.
     */
    private int storeFuncIndex = 0;
    private PigAvroRecordWriter writer = null;    /* avro record writer */
    private Schema outputAvroSchema = null;  /* output avro schema */
    /* indicate whether data is nullable */
    private boolean nullable = true;

    /* loadFunc parameters */
    private PigAvroRecordReader reader = null;   /* avro record writer */
    private Schema inputAvroSchema = null;    /* input avro schema */
    private Schema userSpecifiedAvroSchema = null;    /* avro schema specified in constructor args */

    /* if multiple avro record schemas are merged, this map associates each input
     * record with a remapping of its fields relative to the merged schema. please
     * see AvroStorageUtils.getSchemaToMergedSchemaMap() for more details.
     */
    private Map<Path, Map<Integer, Integer>> schemaToMergedSchemaMap = null;

    /* whether input avro files have the same schema or not. If input avro files
     * do not have the same schema, we merge these schemas into a single schema
     * and derive the pig schema from it.
     */
    private boolean useMultipleSchemas = false;

    private boolean checkSchema = true; /*whether check schema of input directories*/
    private boolean ignoreBadFiles = false; /* whether ignore corrupted files during load */
    private String contextSignature = null;

    /**
     * Empty constructor. Output schema is derived from pig schema.
     */
    public AvroStorage() {
        outputAvroSchema = null;
        nullable = true;
        AvroStorageLog.setDebugLevel(0);
        checkSchema = true;
    }

    /**
     * Constructor of quoted string list
     *
     * @param parts quoted string list
     * @throws IOException
     * @throws ParseException
     */
    public AvroStorage(String[] parts) throws IOException, ParseException {
        outputAvroSchema = null;
        nullable = true;
        checkSchema = true;

        if (parts.length == 1
                && !parts[0].equalsIgnoreCase(NO_SCHEMA_CHECK)
                && !parts[0].equalsIgnoreCase(IGNORE_BAD_FILES)
                && !parts[0].equalsIgnoreCase(MULTIPLE_SCHEMAS)) {
            /* If one parameter is given, and that is not 'no_schema_check',
             * 'ignore_bad_files', or 'multiple_schemas', then it must be a
             * json string.
             */
            init(parseJsonString(parts[0]));
        } else {
            /* parse parameters */
            init(parseStringList(parts));
        }
    }

    /**
     * Set input location and obtain input schema.
     */
    @Override
    public void setLocation(String location, Job job) throws IOException {
        if (inputAvroSchema != null) {
            return;
        }

        Configuration conf = job.getConfiguration();
        Set<Path> paths = AvroStorageUtils.getPaths(location, conf, true);
        if (!paths.isEmpty()) {
            // Set top level directories in input format. Adding all files will
            // bloat configuration size
            FileInputFormat.setInputPaths(job, paths.toArray(new Path[paths.size()]));
            // Scan all directories including sub directories for schema
            setInputAvroSchema(paths, conf);
        } else {
            throw new IOException("Input path \'" + location + "\' is not found");
        }
    }

    /**
     * Set input avro schema. If the 'multiple_schemas' option is enabled, we merge multiple
     * schemas and use that merged schema; otherwise, we simply use the schema from the first
     * file in the paths set.
     *
     * @param paths  set of input files
     * @param conf  configuration
     * @return avro schema
     * @throws IOException
     */
    protected void setInputAvroSchema(Set<Path> paths, Configuration conf) throws IOException {
        if(userSpecifiedAvroSchema != null) {
            inputAvroSchema = userSpecifiedAvroSchema;
        }
        else {
            inputAvroSchema = useMultipleSchemas ? getMergedSchema(paths, conf)
                                                 : getAvroSchema(paths, conf);
        }
    }

    /**
     * Get avro schema of first input file that matches the location pattern.
     *
     * @param paths  set of input files
     * @param conf  configuration
     * @return avro schema
     * @throws IOException
     */
    protected Schema getAvroSchema(Set<Path> paths, Configuration conf) throws IOException {
        if (paths == null || paths.isEmpty()) {
            return null;
        }
        Iterator<Path> iterator = paths.iterator();
        Schema schema = null;
        while (iterator.hasNext()) {
            Path path = iterator.next();
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            schema = getAvroSchema(path, fs);
            if (schema != null) {
                break;
            }
        }
        return schema;
    }

    /**
     * Get avro schema of input path. There are three cases:
     * 1. if path is a file, then return its avro schema;
     * 2. if path is a first-level directory (no sub-directories), then
     * return the avro schema of one underlying file;
     * 3. if path contains sub-directories, then recursively check
     * whether all of them share the same schema and return it
     * if so or throw an exception if not.
     *
     * @param path input path
     * @param fs file system
     * @return avro schema of data
     * @throws IOException if underlying sub-directories do not share the same schema; or if input path is empty or does not exist
     */
    @SuppressWarnings("deprecation")
    protected Schema getAvroSchema(Path path, FileSystem fs) throws IOException {
        if (!fs.exists(path) || !AvroStorageUtils.PATH_FILTER.accept(path))
            return null;

        /* if path is first level directory or is a file */
        if (!fs.isDirectory(path)) {
            return getSchema(path, fs);
        }

        FileStatus[] ss = fs.listStatus(path, AvroStorageUtils.PATH_FILTER);
        Schema schema = null;
        if (ss.length > 0) {
            if (AvroStorageUtils.noDir(ss))
                return getSchema(path, fs);

            /*otherwise, check whether schemas of underlying directories are the same */
            for (FileStatus s : ss) {
                Schema newSchema = getAvroSchema(s.getPath(), fs);
                if (schema == null) {
                    schema = newSchema;
                    if(!checkSchema) {
                        System.out.println("Do not check schema; use schema of " + s.getPath());
                        return schema;
                    }
                } else if (newSchema != null && !schema.equals(newSchema)) {
                    throw new IOException( "Input path is " + path + ". Sub-direcotry " + s.getPath()
                                         + " contains different schema " + newSchema + " than " + schema);
                }
            }
        }

        if (schema == null)
            System.err.println("Cannot get avro schema! Input path " + path + " might be empty.");

        return schema;
    }

    /**
     * Merge multiple input avro schemas into one. Note that we can't merge arbitrary schemas.
     * Please see AvroStorageUtils.mergeSchema() for what's allowed and what's not allowed.
     *
     * @param basePaths  set of input dir or files
     * @param conf  configuration
     * @return avro schema
     * @throws IOException
     */
    protected Schema getMergedSchema(Set<Path> basePaths, Configuration conf) throws IOException {
        Schema result = null;
        Map<Path, Schema> mergedFiles = new HashMap<Path, Schema>();

        Set<Path> paths = AvroStorageUtils.getAllFilesRecursively(basePaths, conf);
        for (Path path : paths) {
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            Schema schema = getSchema(path, fs);
            if (schema != null) {
                result = AvroStorageUtils.mergeSchema(result, schema);
                mergedFiles.put(path, schema);
            }
        }
        // schemaToMergedSchemaMap is only needed when merging multiple records.
        if (mergedFiles.size() > 1 && result.getType().equals(Schema.Type.RECORD)) {
            schemaToMergedSchemaMap = AvroStorageUtils.getSchemaToMergedSchemaMap(result, mergedFiles);
        }
        return result;
    }

    /**
     * This method is called by {@link #getAvroSchema}. The default implementation
     * returns the schema of an avro file; or the schema of the last file in a first-level
     * directory (it does not contain sub-directories).
     *
     * @param path  path of a file or first level directory
     * @param fs  file system
     * @return avro schema
     * @throws IOException
     */
    protected Schema getSchema(Path path, FileSystem fs) throws IOException {
        return AvroStorageUtils.getSchema(path, fs);
    }

    /**
     * This method is called to return the schema of an avro schema file. This
     * method is different than {@link #getSchema}, which returns the schema
     * from a data file.
     *
     * @param path  path of a file or first level directory
     * @param fs  file system
     * @return avro schema
     * @throws IOException
     */
    protected Schema getSchemaFromFile(Path path, FileSystem fs) throws IOException {
        /* get path of the last file */
        Path lastFile = AvroStorageUtils.getLast(path, fs);
        if (lastFile == null) {
            return null;
        }

        /* read in file and obtain schema */
        GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
        InputStream hdfsInputStream = fs.open(lastFile);
        Schema ret = Schema.parse(hdfsInputStream);
        hdfsInputStream.close();

        return ret;
    }


    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() throws IOException {
        AvroStorageLog.funcCall("getInputFormat");
        InputFormat result = null;
        if(inputAvroSchema != null) {
            result = new PigAvroInputFormat(
            inputAvroSchema, ignoreBadFiles, schemaToMergedSchemaMap, useMultipleSchemas);
        } else {
            result = new TextInputFormat();
        }
        return result;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
    throws IOException {
        AvroStorageLog.funcCall("prepareToRead");
        this.reader = (PigAvroRecordReader) reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }
            return (Tuple) this.reader.getCurrentValue();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


    /* implement LoadMetadata */

    /**
     * Get avro schema from "location" and return the converted
     * PigSchema.
     */
    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {

        /* get avro schema */
        AvroStorageLog.funcCall("getSchema");
        if (inputAvroSchema == null) {
            Configuration conf = job.getConfiguration();
            // If within a script, you store to one location and read from same
            // location using AvroStorage getPaths will be empty. Since
            // getSchema is called during script parsing we don't want to fail
            // here if path not found

            Set<Path> paths = AvroStorageUtils.getPaths(location, conf, false);
            if (!paths.isEmpty()) {
                setInputAvroSchema(paths, conf);
            }
        }
        if(inputAvroSchema != null) {
            AvroStorageLog.details( "avro input schema:"  + inputAvroSchema);

            /* convert to pig schema */
            ResourceSchema pigSchema = AvroSchema2Pig.convert(inputAvroSchema);
            AvroStorageLog.details("pig input schema:" + pigSchema);
            if (pigSchema.getFields().length == 1){
                pigSchema = pigSchema.getFields()[0].getSchema();
            }
            return pigSchema;
        } else {
            return null;
        }
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        return null;  // Nothing to do
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        return null; // Nothing to do
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException {
        // Nothing to do
    }

    /**
     * build a property map from a json object
     *
     * @param jsonString  json object in string format
     * @return a property map
     * @throws ParseException
     */
    @SuppressWarnings("unchecked")
    protected Map<String, Object> parseJsonString(String jsonString) throws ParseException {

        /*parse the json object */
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(jsonString);

        Set<Entry<String, Object>> entries = obj.entrySet();
        for (Entry<String, Object> entry : entries) {

            String key = entry.getKey();
            Object value = entry.getValue();

            if (key.equalsIgnoreCase("debug") || key.equalsIgnoreCase("index")) {
                /* convert long values to integer */
                int v = ((Long) value).intValue();
                obj.put(key, v);
            }
            else if (key.equalsIgnoreCase("schema") || key.matches("field\\d+")) {
               /* convert avro schema (as json object) to string */
                obj.put(key, value.toString().trim());
            }

        }
        return obj;
    }

    /**
     * build a property map from a string list
     *
     * @param parts  input string list
     * @return a property map
     * @throws IOException
     * @throws ParseException
     */
    protected Map<String, Object> parseStringList(String[] parts) throws IOException {

        Map<String, Object> map = new HashMap<String, Object>();

        for (int i = 0; i < parts.length; ) {
            String name = parts[i].trim();
            if (name.equalsIgnoreCase(NO_SCHEMA_CHECK)) {
                checkSchema = false;
                /* parameter only, so increase iteration counter by 1 */
                i += 1;
            } else if (name.equalsIgnoreCase(IGNORE_BAD_FILES)) {
                ignoreBadFiles = true;
                /* parameter only, so increase iteration counter by 1 */
                i += 1;
            } else if (name.equalsIgnoreCase(MULTIPLE_SCHEMAS)) {
                useMultipleSchemas = true;
                /* parameter only, so increase iteration counter by 1 */
                i += 1;
            } else {
                String value = parts[i+1].trim();
                if (name.equalsIgnoreCase("debug")
                             || name.equalsIgnoreCase("index")) {
                    /* store value as integer */
                    map.put(name, Integer.parseInt(value));
                } else if (name.equalsIgnoreCase("data")
                             || name.equalsIgnoreCase("same")
                             || name.equalsIgnoreCase("schema")
                             || name.equalsIgnoreCase("schema_file")
                             || name.equalsIgnoreCase("schema_uri")
                             || name.matches("field\\d+")) {
                    /* store value as string */
                    map.put(name, value);
                } else if (name.equalsIgnoreCase("nullable")) {
                    /* store value as boolean */
                    map.put(name, Boolean.getBoolean(value));
                } else {
                    throw new IOException("Invalid parameter:" + name);
                }
                /* parameter/value pair, so increase iteration counter by 2 */
                i += 2;
            }
        }
        return map;
    }

    /**
     * Initialize output avro schema using input property map
     */
    protected void init(Map<String, Object> inputs) throws IOException {

        /*used to store field schemas */
        List<Field> fields = null;

        /* set debug level */
        if (inputs.containsKey("debug")) {
            AvroStorageLog.setDebugLevel((Integer) inputs.get("debug"));
        }

        /* initialize schema manager, if any */
        AvroSchemaManager schemaManager = null;
        if (inputs.containsKey("data")) {
            Path path = new Path((String) inputs.get("data"));
            AvroStorageLog.details("data path=" + path.toUri().toString());
            FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
            Schema schema = getAvroSchema(path, fs);
            schemaManager = new AvroSchemaManager(schema);
        }
        else if (inputs.containsKey("schema_file")) {
            Path path = new Path((String) inputs.get("schema_file"));
            AvroStorageLog.details("schemaFile path=" + path.toUri().toString());
            FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
            Schema schema = getSchemaFromFile(path, fs);
            schemaManager = new AvroSchemaManager(schema);
        }

        /* iterate input property map */
        for (Entry<String, Object> entry : inputs.entrySet()) {
            String name = entry.getKey().trim();
            Object value = entry.getValue();

            if (name.equalsIgnoreCase("index")) {
                /* set index of store function */
                storeFuncIndex = (Integer) value;
            } else if (name.equalsIgnoreCase("same")) {
                /* use schema in the specified path as output schema */
                Path path = new Path( ((String) value).trim());
                AvroStorageLog.details("data path=" + path.toUri().toString());
                FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
                outputAvroSchema = getAvroSchema(path, fs);
            } else if (name.equalsIgnoreCase("nullable")) {
                nullable = (Boolean) value;
            } else if (name.equalsIgnoreCase("schema")) {
                outputAvroSchema = Schema.parse((String) value);
                userSpecifiedAvroSchema = outputAvroSchema;
            } else if (name.equalsIgnoreCase("schema_uri")) {
                /* use the contents of the specified path as output schema */
                Path path = new Path( ((String) value).trim());
                AvroStorageLog.details("schema_uri path=" + path.toUri().toString());
                FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
                outputAvroSchema = getSchemaFromFile(path, fs);
                userSpecifiedAvroSchema = outputAvroSchema;
            } else if (name.matches("field\\d+")) {
                /*set schema of dth field */
                if (fields == null)
                    fields = new ArrayList<Field>();

                int index = Integer.parseInt(name.substring("field".length()));
                String content = ((String) value).trim();
                Field field = null;
                if (content.equalsIgnoreCase(NOTNULL)) {
                    /* null means deriving avro schema from pig schema but not null*/
                    field = AvroStorageUtils.createUDField(index, null);
                } else if (content.startsWith("def:")) {
                    if (schemaManager == null)
                        throw new IOException("Please specify data parameter (using \"data\") before this one.");

                    String alias = content.substring("def:".length());
                    Schema s = schemaManager.getSchema(alias);
                    if (s == null)
                        throw new IOException("Cannot find matching schema for alias:" + alias);
                    /* use pre-defined schema*/
                    field = AvroStorageUtils.createUDField(index, s);

                    AvroStorageLog.details("Use pre-defined schema(" + alias + "): " + s + " for field " + index);
                } else {
                    Schema schema = null;
                    try {
                        schema = Schema.parse(content);
                    } catch (RuntimeException e) {
                        /* might be primary schema like int or long */
                        schema = Schema.parse("\"" + content + "\"");
                    }

                    field = AvroStorageUtils.createUDField(index, schema);
                }

                fields.add(field);
            } else if (!name.equalsIgnoreCase("data")
                                  && !name.equalsIgnoreCase("schema_file")
                                  && !name.equalsIgnoreCase("debug")) {
                throw new IOException("Invalid parameter:" + name);
            }
        }

        /* if schemas of some fields are set */
        if (fields != null && outputAvroSchema == null) {
            outputAvroSchema = AvroStorageUtils.createUDPartialRecordSchema();
            outputAvroSchema.setFields(fields);
        }

        /* print warning if both output and nullable are specified;
         * and nullable will be ignored.*/
        if (outputAvroSchema != null) {
            if (!nullable) {
                AvroStorageLog.warn("Invalid parameter--nullable cannot be false while "
                        + "output schema is not null. Will ignore nullable.\n\n");
                nullable = true;
            }
        }

    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
        return LoadFunc.getAbsolutePath(location, curDir);
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        AvroStorageLog.details("output location=" + location);
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    /**
     * Append newly specified schema
     */
    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        AvroStorageLog.funcCall("Check schema");
        Properties property = getUDFProperties();
        String prevSchemaStr = property.getProperty(AVRO_OUTPUT_SCHEMA_PROPERTY);
        AvroStorageLog.details("Previously defined schemas=" + prevSchemaStr);

        String key = getSchemaKey();
        Map<String, String> schemaMap = (prevSchemaStr != null)
                                                                ? parseSchemaMap(prevSchemaStr)
                                                                : null;

        if (schemaMap != null && schemaMap.containsKey(key)) {
            AvroStorageLog.warn("Duplicate value for key-" + key + ". Will ignore the new schema.");
            return;
        }

        /* validate and convert output schema */
        Schema schema = outputAvroSchema != null
                                  ? (checkSchema
                                          ? PigSchema2Avro.validateAndConvert(outputAvroSchema, s)
                                          : outputAvroSchema)
                                  : PigSchema2Avro.convert(s, nullable);

        AvroStorageLog.info("key=" + key + " outputSchema=" + schema);

        String schemaStr = schema.toString();
        String append = key + SCHEMA_KEYVALUE_DELIM + schemaStr;

        String newSchemaStr = (schemaMap != null)
                                                ? prevSchemaStr + SCHEMA_DELIM + append
                                                : append;
        property.setProperty(AVRO_OUTPUT_SCHEMA_PROPERTY, newSchemaStr);
        AvroStorageLog.details("New schemas=" + newSchemaStr);
    }

    /**
     * Returns UDFProperties based on <code>contextSignature</code>.
     */
    private Properties getUDFProperties() {
        return UDFContext.getUDFContext()
            .getUDFProperties(this.getClass(), new String[] {contextSignature});
    }

    private String getSchemaKey() {
        return Integer.toString(storeFuncIndex);
    }

    private Map<String, String> parseSchemaMap(String input) throws IOException {
        AvroStorageLog.details("Parse schema map from " + input);
        String[] entries = input.split(SCHEMA_DELIM);
        Map<String, String> map = new HashMap<String, String>();
        for (String entry : entries) {

            AvroStorageLog.details("Entry = " + entry);
            if (entry.length() == 0)
                continue;

            String[] parts = entry.split(SCHEMA_KEYVALUE_DELIM);
            if (parts.length != 2)
                throw new IOException("Expect 2 fields in " + entry);
            map.put(parts[0], parts[1]);
        }
        return map;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        AvroStorageLog.funcCall("getOutputFormat");

        Properties property = getUDFProperties();
        String allSchemaStr = property.getProperty(AVRO_OUTPUT_SCHEMA_PROPERTY);
        Map<String, String> map = (allSchemaStr != null)  ? parseSchemaMap(allSchemaStr) : null;

        String key = getSchemaKey();
        Schema schema = (map == null || !map.containsKey(key))  ? outputAvroSchema  : Schema.parse(map.get(key));

        if (schema == null)
            throw new IOException("Output schema is null!");
        AvroStorageLog.details("Output schema=" + schema);

        return new PigAvroOutputFormat(schema);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public  void  prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = (PigAvroRecordWriter) writer;
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        StoreFunc.cleanupOnFailureImpl(location, job);
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        // Nothing to do
    }

    @Override
    public void putNext(Tuple t) throws IOException {
        try {
            this.writer.write(NullWritable.get(), t.getAll().size() == 1 ? t.get(0) : t);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
