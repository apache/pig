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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.file.DataFileStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.codehaus.jackson.JsonNode;
/**
 * This is utility class for this package
 */
public class AvroStorageUtils {

    public static Schema BooleanSchema = Schema.create(Schema.Type.BOOLEAN);
    public static Schema LongSchema = Schema.create(Schema.Type.LONG);
    public static Schema FloatSchema = Schema.create(Schema.Type.FLOAT);
    public static Schema DoubleSchema = Schema.create(Schema.Type.DOUBLE);
    public static Schema IntSchema = Schema.create(Schema.Type.INT);
    public static Schema StringSchema = Schema.create(Schema.Type.STRING);
    public static Schema BytesSchema = Schema.create(Schema.Type.BYTES);
    public static Schema NullSchema = Schema.create(Schema.Type.NULL);

    private static final String NONAME = "NONAME";
    private static final String PIG_TUPLE_WRAPPER = "PIG_WRAPPER";

    /** ignore hdfs files with prefix "_" and "." */
    public static PathFilter PATH_FILTER = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            return !path.getName().startsWith("_")
                        && !path.getName().startsWith(".");
        }
    };

    static String getDummyFieldName(int index) {
        return NONAME + "_" + index;
    }

    /** create  an avro field using the given schema */
    public static Field createUDField(int index, Schema s) {
        return new Field(getDummyFieldName(index), s, null, null);
    }

    /** create an avro field with null schema (it is a space holder) */
    public static Schema createUDPartialRecordSchema() {
        return Schema.createRecord(NONAME, null, null, false);
    }

    /** check whether a schema is a space holder (using field name) */
    public static boolean isUDPartialRecordSchema(Schema s) {
        return s.getName().equals(NONAME);
    }

    /** get field schema given index number */
    public static Field getUDField(Schema s, int index) {
        return s.getField(getDummyFieldName(index));
    }

    /**
     * Gets the list of paths from the pathString specified which may contain
     * comma-separated paths and glob style path
     *
     * @throws IOException
     */
    public static Set<Path> getPaths(String pathString, Configuration conf, boolean failIfNotFound)
            throws IOException {
        Set<Path> paths = new HashSet<Path>();
        String[] pathStrs = LoadFunc.getPathStrings(pathString);
        for (String pathStr : pathStrs) {
            FileSystem fs = FileSystem.get(new Path(pathStr).toUri(), conf);
            FileStatus[] matchedFiles = fs.globStatus(new Path(pathStr), PATH_FILTER);
            if (matchedFiles == null || matchedFiles.length == 0) {
                if (failIfNotFound) {
                    throw new IOException("Input Pattern " + pathStr + " matches 0 files");
                } else {
                    continue;
                }
            }
            for (FileStatus file : matchedFiles) {
                paths.add(file.getPath());
            }
        }
        return paths;
    }

    /**
     * Returns all non-hidden files recursively inside the base paths given
     *
     * @throws IOException
     */
    public static Set<Path> getAllFilesRecursively(Set<Path> basePaths, Configuration conf) throws IOException {
        Set<Path> paths = new HashSet<Path>();
        for (Path path : basePaths) {
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            FileStatus f = fs.getFileStatus(path);
            if (f.isDir()) {
                getAllFilesInternal(f, conf, paths, fs);
            } else {
                paths.add(path);
            }
        }
        return paths;
    }

    private static void getAllFilesInternal(FileStatus file, Configuration conf,
            Set<Path> paths, FileSystem fs) throws IOException {
        for (FileStatus f : fs.listStatus(file.getPath(), PATH_FILTER)) {
            if (f.isDir()) {
                getAllFilesInternal(f, conf, paths, fs);
            } else {
                paths.add(f.getPath());
            }
        }
    }

    /** check whether there is NO directory in the input file (status) list*/
    public static boolean noDir(FileStatus [] ss) {
        for (FileStatus s : ss) {
            if (s.isDir())
                return false;
        }
        return true;
    }

    /** get last file of a hdfs path if it is  a directory;
     *   or return the file itself if path is a file
     */
    public static Path getLast(Path path, FileSystem fs) throws IOException {

        FileStatus status = fs.getFileStatus(path);
        if (!status.isDir()) {
            return path;
        }
        FileStatus[] statuses = fs.listStatus(path, PATH_FILTER);

        if (statuses.length == 0) {
            return null;
        } else {
            Arrays.sort(statuses);
            for (int i = statuses.length - 1; i >= 0; i--) {
                if (!statuses[i].isDir()) {
                    return statuses[i].getPath();
                }
            }
            return null;
        }
    }

    /**
     * This method merges two primitive avro types into one. This method must
     * be used only to merge two primitive types. For complex types, null will
     * be returned unless they are both the same type. Also note that not every
     * primitive type can be merged. For types that cannot be merged, null is
     * returned.
     *
     * @param x first avro type to merge
     * @param y second avro type to merge
     * @return merged avro type
     */
    private static Schema.Type mergeType(Schema.Type x, Schema.Type y) {
        if (x.equals(y)) {
            return x;
        }

        switch(x) {
            case INT:
                switch (y) {
                    case LONG:
                        return Schema.Type.LONG;
                    case FLOAT:
                        return Schema.Type.FLOAT;
                    case DOUBLE:
                        return Schema.Type.DOUBLE;
                    case ENUM:
                    case STRING:
                        return Schema.Type.STRING;
                }

            case LONG:
                switch (y) {
                    case INT:
                        return Schema.Type.LONG;
                    case FLOAT:
                        return Schema.Type.FLOAT;
                    case DOUBLE:
                        return Schema.Type.DOUBLE;
                    case ENUM:
                    case STRING:
                        return Schema.Type.STRING;
                }

            case FLOAT:
                switch (y) {
                    case INT:
                    case LONG:
                        return Schema.Type.FLOAT;
                    case DOUBLE:
                        return Schema.Type.DOUBLE;
                    case ENUM:
                    case STRING:
                        return Schema.Type.STRING;
                }

            case DOUBLE:
                switch (y) {
                    case INT:
                    case LONG:
                    case FLOAT:
                        return Schema.Type.DOUBLE;
                    case ENUM:
                    case STRING:
                        return Schema.Type.STRING;
                }

            case ENUM:
                switch (y) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case STRING:
                        return Schema.Type.STRING;
                }

            case STRING:
                switch (y) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case ENUM:
                        return Schema.Type.STRING;
                }
        }

        // else return just null in particular, bytes and boolean
        return null;
    }

    /**
     * This method merges two avro schemas into one. Note that not every avro schema
     * can be merged. For complex types to be merged, they must be the same type.
     * For primitive types to be merged, they must meet certain conditions. For
     * schemas that cannot be merged, an exception is thrown.
     *
     * @param x first avro schema to merge
     * @param y second avro schema to merge
     * @return merged avro schema
     * @throws IOException
     */
    public static Schema mergeSchema(Schema x, Schema y) throws IOException {
        if (x == null) {
            return y;
        }
        if (y == null) {
            return x;
        }
        if (x.equals(y)) {
            return x;
        }

        Schema.Type xType = x.getType();
        Schema.Type yType = y.getType();

        switch (xType) {
            case RECORD:
                if (!yType.equals(Schema.Type.RECORD)) {
                   throw new IOException("Cannot merge " + xType + " with " + yType);
                }

                List<Schema.Field> xFields = x.getFields();
                List<Schema.Field> yFields = y.getFields();

                // LinkedHashMap is used to keep fields in insertion order.
                // It's convenient for testing to have deterministic behaviors.
                Map<String, Schema> fieldName2Schema =
                        new LinkedHashMap<String, Schema>(xFields.size() + yFields.size());
                Map<String, JsonNode> fieldName2Default =
                        new LinkedHashMap<String, JsonNode>(xFields.size() + yFields.size());

                for (Schema.Field xField : xFields) {
                    fieldName2Schema.put(xField.name(), xField.schema());
                    fieldName2Default.put(xField.name(),xField.defaultValue());
                }
                for (Schema.Field yField : yFields) {
                    String name = yField.name();
                    Schema currSchema = yField.schema();
                    Schema prevSchema = fieldName2Schema.get(name);
                    if (prevSchema == null) {
                        fieldName2Schema.put(name, currSchema);
                        fieldName2Default.put(name, yField.defaultValue());
                    } else {
                        fieldName2Schema.put(name, mergeSchema(prevSchema, currSchema));
                        //during merging of schemas for records it to okay to have one field with a default
                        // and another null so the one with the default will be considered
                        JsonNode xDefaultValue = fieldName2Default.get(name);
                        JsonNode yDefaultValue = yField.defaultValue();
                        if (xDefaultValue != null) {
                            // need to check if the default values in the schemas are the same
                            if (yDefaultValue != null && !xDefaultValue.equals(yDefaultValue)) {
                                throw new IOException(
                                     "Cannot merge schema's which have different default values - " + xDefaultValue +
                                     " and " + yDefaultValue);
                            }
                        } else {
                            fieldName2Default.put(name, yDefaultValue);
                        }
                    }
                }
                List<Schema.Field> mergedFields = new ArrayList<Schema.Field>(fieldName2Schema.size());
                for (Entry<String, Schema> entry : fieldName2Schema.entrySet()) {
                    mergedFields.add(new Schema.Field(entry.getKey(), entry.getValue(), "auto-gen", fieldName2Default.get(entry.getKey())));
                }
                Schema result = Schema.createRecord(
                        "merged", null, "merged schema (generated by AvroStorage)", false);
                result.setFields(mergedFields);
                return result;

            case ARRAY:
                if (!yType.equals(Schema.Type.ARRAY)) {
                    throw new IOException("Cannot merge " + xType + " with " + yType);
                }
                return Schema.createArray(mergeSchema(x.getElementType(), y.getElementType()));

            case MAP:
                if (!yType.equals(Schema.Type.MAP)) {
                    throw new IOException("Cannot merge " + xType + " with " + yType);
                }
                return Schema.createMap(mergeSchema(x.getValueType(), y.getValueType()));

            case UNION:
                if (!yType.equals(Schema.Type.UNION)) {
                    throw new IOException("Cannot merge " + xType + " with " + yType);
                }
                List<Schema> xTypes = x.getTypes();
                List<Schema> yTypes = y.getTypes();

                List<Schema> unionTypes = new ArrayList<Schema>();
                for (Schema xSchema : xTypes) {
                    unionTypes.add(xSchema);
                }
                for (Schema ySchema : yTypes) {
                    if (!unionTypes.contains(ySchema)) {
                        unionTypes.add(ySchema);
                    }
                }
                return Schema.createUnion(unionTypes);

            case FIXED:
                if (!yType.equals(Schema.Type.FIXED)) {
                    throw new IOException("Cannot merge " + xType + " with " + yType);
                }
                int xSize = x.getFixedSize();
                int ySize = y.getFixedSize();
                if (xSize != ySize) {
                    throw new IOException("Cannot merge FIXED types with different sizes: " + xSize + " and " + ySize);
                }
                return Schema.createFixed("merged", null, "merged schema (generated by AvroStorage)", xSize);

            default: // primitive types
                Schema.Type mergedType = mergeType(xType ,yType);
                if (mergedType == null) {
                    throw new IOException("Cannot merge " + xType + " with " + yType);
                }
                return Schema.create(mergedType);
        }
    }

    /**
     * When merging multiple avro record schemas, we build a map (schemaToMergedSchemaMap)
     * to associate each input record with a remapping of its fields relative to the merged
     * schema. Take the following two schemas for example:
     *
     * // path1
     * { "type": "record",
     *   "name": "x",
     *   "fields": [ { "name": "xField", "type": "string" } ]
     * }
     *
     * // path2
     * { "type": "record",
     *   "name": "y",
     *   "fields": [ { "name": "yField", "type": "string" } ]
     * }
     *
     * The merged schema will be something like this:
     *
     * // merged
     * { "type": "record",
     *   "name": "merged",
     *   "fields": [ { "name": "xField", "type": "string" },
     *               { "name": "yField", "type": "string" } ]
     * }
     *
     * The schemaToMergedSchemaMap will look like this:
     *
     * // schemaToMergedSchemaMap
     * { path1 : { 0 : 0 },
     *   path2 : { 0 : 1 }
     * }
     *
     * The meaning of the map is:
     * - The field at index '0' of 'path1' is moved to index '0' in merged schema.
     * - The field at index '0' of 'path2' is moved to index '1' in merged schema.
     *
     * With this map, we can now remap the field position of the original schema to
     * that of the merged schema. This is necessary because in the backend, we don't
     * use the merged avro schema but embedded avro schemas of input files to load
     * them. Therefore, we must relocate each field from old positions in the original
     * schema to new positions in the merged schema.
     *
     * @param mergedSchema new schema generated from multiple input schemas
     * @param mergedFiles input avro files that are merged
     * @return schemaToMergedSchemaMap that maps old position of each field in the
     * original schema to new position in the new schema
     * @throws IOException
     */
    public static Map<Path, Map<Integer, Integer>> getSchemaToMergedSchemaMap(
            Schema mergedSchema, Map<Path, Schema> mergedFiles) throws IOException {

        if (!mergedSchema.getType().equals(Schema.Type.RECORD)) {
            throw new IOException("Remapping of non-record schemas is not supported");
        }

        Map<Path, Map<Integer, Integer>> result =
                new HashMap<Path, Map<Integer, Integer>>(mergedFiles.size());

        // map from field position in old schema to field position in new schema
        for (Map.Entry<Path, Schema> entry : mergedFiles.entrySet()) {
            Path path = entry.getKey();
            Schema schema = entry.getValue();
            if (!schema.getType().equals(Schema.Type.RECORD)) {
                throw new IOException("Remapping of non-record schemas is not supported");
            }
            List<Field> fields = schema.getFields();
            Map<Integer, Integer> oldPos2NewPos = result.get(path);
            if (oldPos2NewPos == null) {
                oldPos2NewPos = new HashMap<Integer, Integer>(fields.size());
                result.put(path, oldPos2NewPos);
            }
            for (Field field : fields) {
                String fieldName = field.name();
                int oldPos = schema.getField(fieldName).pos();
                int newPos = mergedSchema.getField(fieldName).pos();
                oldPos2NewPos.put(oldPos, newPos);
            }
        }
        return result;
    }

    /**
     * Wrap an avro schema as a nullable union if needed.
     * For instance, wrap schema "int" as ["null", "int"]
     */
    public static Schema wrapAsUnion(Schema schema, boolean nullable) {
        if (nullable) {
            /* if schema is an acceptable union, then return itself */
            if (schema.getType().equals(Schema.Type.UNION)
                    && isAcceptableUnion(schema))
                return schema;
            else
                return Schema.createUnion(Arrays.asList(NullSchema, schema));
        } else
            /*do not wrap it if not */
            return schema;
    }

    /** determine whether the input schema contains recursive records */
    public static boolean containsRecursiveRecord(Schema s) {
        /*initialize empty set of defined record names*/
        Set<String> set = new HashSet<String> ();
        return containsRecursiveRecord(s, set);
    }

    /**
     * Called by {@link #containsRecursiveRecord(Schema)} and it recursively checks
     * whether the input schema contains recursive records.
     */
    protected static boolean containsRecursiveRecord(Schema s, Set<String> definedRecordNames) {

        /* if it is a record, check itself and all fields*/
        if (s.getType().equals(Schema.Type.RECORD)) {
            String name = s.getName();
            if (definedRecordNames.contains(name)) return true;

            /* add its own name into defined record set*/
            definedRecordNames.add(s.getName());

            /* check all fields */
            List<Field> fields = s.getFields();
            for (Field field: fields) {
                Schema fs = field.schema();
                if (containsRecursiveRecord(fs, definedRecordNames))
                    return true;
            }

            /* remove its own name from the name set */
            definedRecordNames.remove(s.getName());

            return false;
        }

        /* if it is an array, check its element type */
        else if (s.getType().equals(Schema.Type.ARRAY)) {
            Schema fs = s.getElementType();
            return containsRecursiveRecord(fs, definedRecordNames);
        }

        /*if it is a map, check its value type */
        else if (s.getType().equals(Schema.Type.MAP)) {
            Schema vs = s.getValueType();
            return containsRecursiveRecord(vs, definedRecordNames);
        }

        /* if it is a union, check all possible types */
        else if (s.getType().equals(Schema.Type.UNION)) {
            List<Schema> types = s.getTypes();
            for (Schema type: types) {
                if (containsRecursiveRecord(type, definedRecordNames))
                    return true;
            }
            return false;
        }

        /* return false for other cases */
        else {
            return false;
        }
    }

    /** determine whether the input schema contains generic unions */
    public static boolean containsGenericUnion(Schema s) {
        /* initialize empty set of visited records */
        Set<Schema> set = new HashSet<Schema> ();
        return containsGenericUnion(s, set);
    }

    /**
     * Called by {@link #containsGenericUnion(Schema)} and it recursively checks
     * whether the input schema contains generic unions.
     */
    protected static boolean containsGenericUnion(Schema s, Set<Schema> visitedRecords) {

        /* if it is a record, check all fields*/
        if (s.getType().equals(Schema.Type.RECORD)) {

            /* add its own name into visited record set*/
            visitedRecords.add(s);

            /* check all fields */
            List<Field> fields = s.getFields();
            for (Field field: fields) {
                Schema fs = field.schema();
                if (!visitedRecords.contains(fs)) {
                    if (containsGenericUnion(fs, visitedRecords)) {
                        return true;
                    }
                }
            }
            return false;
        }

        /* if it is an array, check its element type */
        else if (s.getType().equals(Schema.Type.ARRAY)) {
            Schema fs = s.getElementType();
            if (!visitedRecords.contains(fs)) {
                return containsGenericUnion(fs, visitedRecords);
            }
            return false;
        }

        /*if it is a map, check its value type */
        else if (s.getType().equals(Schema.Type.MAP)) {
            Schema vs = s.getValueType();
            if (!visitedRecords.contains(vs)) {
                return containsGenericUnion(vs, visitedRecords);
            }
            return false;
        }

        /* if it is a union, check all possible types and itself */
        else if (s.getType().equals(Schema.Type.UNION)) {
            List<Schema> types = s.getTypes();
            for (Schema type: types) {
                if (!visitedRecords.contains(type)) {
                    if (containsGenericUnion(type, visitedRecords)) {
                        return true;
                    }
                }
            }
            /* check whether itself is acceptable (null-union) */
            return !isAcceptableUnion(s);
        }

        /* return false for other cases */
        else {
            return false;
        }
    }

    /** determine whether a union is a nullable union;
     * note that this function doesn't check containing
     * types of the input union recursively. */
    public static boolean isAcceptableUnion(Schema in) {
        if (! in.getType().equals(Schema.Type.UNION))
            return false;

        List<Schema> types = in.getTypes();
        if (types.size() <= 1) {
            return true;
        } else if (types.size() > 2) {
            return false; /*contains more than 2 types */
        } else {
            /* one of two types is NULL */
            return types.get(0).getType().equals(Schema.Type.NULL) || types.get(1) .getType().equals(Schema.Type.NULL);
        }
    }

    /** wrap a pig schema as tuple */
    public static ResourceFieldSchema wrapAsTuple(ResourceFieldSchema subFieldSchema) throws IOException {
        ResourceSchema listSchema = new ResourceSchema();
        listSchema.setFields(new ResourceFieldSchema[] { subFieldSchema });

        ResourceFieldSchema tupleWrapper = new ResourceFieldSchema();
        tupleWrapper.setType(DataType.TUPLE);
        tupleWrapper.setName(PIG_TUPLE_WRAPPER);
        tupleWrapper.setSchema(listSchema);

        return tupleWrapper;
    }

    /** check whether it is just a wrapped tuple */
    public static boolean isTupleWrapper(ResourceFieldSchema pigSchema) {
        Boolean status = false;
        if(pigSchema.getType() == DataType.TUPLE)
            if(pigSchema.getName() != null)
                if(pigSchema.getName().equals(AvroStorageUtils.PIG_TUPLE_WRAPPER))
                    status = true;
        return status;
    }

    /** extract schema from a nullable union */
    public static Schema getAcceptedType(Schema in) {
        if (!isAcceptableUnion(in))
            throw new RuntimeException("Cannot call this function on a unacceptable union");

        List<Schema> types = in.getTypes();
        switch (types.size()) {
        case 0:
            return null; /*union with no type*/
        case 1:
            return types.get(0); /*union with one type*/
        case 2:
            return  (types.get(0).getType().equals(Schema.Type.NULL))
                        ? types.get(1)
                        : types.get(0);
        default:
            return null;
        }
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
    public static Schema getSchema(Path path, FileSystem fs) throws IOException {
        /* get path of the last file */
        Path lastFile = AvroStorageUtils.getLast(path, fs);
        if (lastFile == null) {
            return null;
        }

        /* read in file and obtain schema */
        GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
        InputStream hdfsInputStream = fs.open(lastFile);
        DataFileStream<Object> avroDataStream = new DataFileStream<Object>(hdfsInputStream, avroReader);
        Schema ret = avroDataStream.getSchema();
        avroDataStream.close();

        return ret;
    }

}
