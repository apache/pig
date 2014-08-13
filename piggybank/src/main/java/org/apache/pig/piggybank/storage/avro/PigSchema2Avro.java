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
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;

import org.codehaus.jackson.JsonNode;

/**
 * This class contains functions to convert Pig schema to Avro. It consists of
 * two sets of methods:
 * 
 * 1. Convert a Pig schema to Avro schema;
 * 2. Validate whether a Pig schema is compatible with a given Avro schema.
 * Notice that the Avro schema doesn't need to cover all fields in Pig schema,
 * and the missing fields are converted using methods in set 1.
 * 
 */
public class PigSchema2Avro {

    public static final String TUPLE_NAME = "TUPLE";
    public static final String FIELD_NAME = "FIELD";
    public static int tupleIndex = 0;

    // //////////////////////////////////////////////////////////
    // Methods in Set 1: Convert Pig schema to Avro schema
    // //////////////////////////////////////////////////////////

    /**
     * Convert a pig ResourceSchema to avro schema
     * 
     */
    public static Schema convert(ResourceSchema pigSchema, boolean nullable) throws IOException {
        ResourceFieldSchema[] pigFields = pigSchema.getFields();

        /* remove the pig tuple wrapper */
        if (pigFields.length == 1) {

            AvroStorageLog.details("Ignore the pig tuple wrapper.");
            return convert(pigFields[0], nullable);
        } else
            return convertRecord(pigFields, nullable);
    }

    /**
     * Convert a Pig ResourceFieldSchema to avro schema
     * 
     */
    protected static Schema convert(ResourceFieldSchema pigSchema, boolean nullable) throws IOException {

        AvroStorageLog.details("Convert pig field schema:" + pigSchema);

        final byte pigType = pigSchema.getType();

        if (pigType == DataType.TUPLE) {
            AvroStorageLog.details("Convert a pig field tuple: " + pigSchema);

            ResourceFieldSchema[] listSchemas = pigSchema.getSchema()
                                            .getFields();
            Schema outSchema = null;

            if (AvroStorageUtils.isTupleWrapper(pigSchema)) {
                /* remove Pig tuple wrapper */
                AvroStorageLog.details("Ignore the pig tuple wrapper.");
                if (listSchemas.length != 1)
                    throw new IOException("Expect one subfield from "
                                                    + pigSchema);
                outSchema = convert(listSchemas[0], nullable);
            } else {
                outSchema = convertRecord(listSchemas, nullable);
            }

            return AvroStorageUtils.wrapAsUnion(outSchema, nullable);

        } else if (pigType == DataType.BAG) {

            AvroStorageLog.details("Convert a pig field bag:" + pigSchema);

            /* Bag elements have to be Tuples */
            ResourceFieldSchema[] fs = pigSchema.getSchema().getFields();
            if (fs == null || fs.length != 1
                                 || fs[0].getType() != DataType.TUPLE)
                throw new IOException("Expect one tuple field in a bag");

            Schema outSchema = Schema.createArray(convert(fs[0], nullable));
            return AvroStorageUtils.wrapAsUnion(outSchema, nullable);

        } else if (pigType == DataType.MAP) {
            /* Pig doesn't provide schema info of Map value */
            throw new IOException("Please provide schema for Map field!");
        
        } else if (pigType == DataType.UNKNOWN) {
            /* Results of Pig UNION operation is of UNKNOWN type */
            throw new IOException("Must specify a schema for UNKNOWN pig type.");
        
        } else if (pigType == DataType.CHARARRAY
                                        || pigType == DataType.BIGCHARARRAY
                                        || pigType == DataType.BOOLEAN
                                        || pigType == DataType.BYTE
                                        || pigType == DataType.BYTEARRAY
                                        || pigType == DataType.DOUBLE
                                        || pigType == DataType.FLOAT
                                        || pigType == DataType.INTEGER
                                        || pigType == DataType.LONG) {

            AvroStorageLog.details("Convert a pig field primitive:" + pigSchema);
            Schema outSchema = convertPrimitiveType(pigType);
            return AvroStorageUtils.wrapAsUnion(outSchema, nullable);

        } else
            throw new IOException("unsupported pig type:"
                                            + DataType.findTypeName(pigType));
    }

    /**
     * Convert pig data to Avro record
     * 
     */
    protected static Schema convertRecord(ResourceFieldSchema[] pigFields, boolean nullable) throws IOException {

        AvroStorageLog.funcCall("convertRecord");

        // Type name is required for Avro record
        String typeName = getRecordName();
        Schema outSchema = Schema.createRecord(typeName, null, null, false);

        List<Schema.Field> outFields = new ArrayList<Schema.Field>();
        for (int i = 0; i < pigFields.length; i++) {

            /* get schema */
            Schema fieldSchema = convert(pigFields[i], nullable);

            /* get field name of output */
            String outname = pigFields[i].getName();
            if (outname == null)
                outname = FIELD_NAME + "_" + i; // field name cannot be null

            /* get doc of output */
            String desc = pigFields[i].getDescription();

            outFields.add(new Field(outname, fieldSchema, desc, null));
        }

        outSchema.setFields(outFields);
        return outSchema;

    }

  /**
   * This is a painful hack to make unit tests pass. The static counter holds
   * state between unit tests, so it needs to be reset. Otherwise tests will
   * fail if their order is swapped or a new test is added.
   *
   * @param index
   */
    public static void setTupleIndex(int index) { tupleIndex = index; }

    private static String getRecordName() {
        String name = TUPLE_NAME + "_" + tupleIndex;
        tupleIndex++;
        return name;
    }

    /**
     * Convert Pig primitive type to Avro type
     * 
     */
    protected static Schema convertPrimitiveType(byte pigType) throws IOException {

        if (pigType == DataType.BOOLEAN) {
            return AvroStorageUtils.BooleanSchema;
        } else if (pigType == DataType.BYTEARRAY) {
            return AvroStorageUtils.BytesSchema;
        } else if (pigType == DataType.CHARARRAY
                                        || pigType == DataType.BIGCHARARRAY) {
            return AvroStorageUtils.StringSchema;
        } else if (pigType == DataType.DOUBLE) {
            return AvroStorageUtils.DoubleSchema;
        } else if (pigType == DataType.FLOAT) {
            return AvroStorageUtils.FloatSchema;
        } else if (pigType == DataType.INTEGER) {
            return AvroStorageUtils.IntSchema;
        } else if (pigType == DataType.LONG) {
            return AvroStorageUtils.LongSchema;
        } else
            throw new IOException("unsupported pig type:"
                                            + DataType.findTypeName(pigType));

    }

    
    // //////////////////////////////////////////////////////////
    // Methods in Set 2: Validate whether a Pig schema is compatible 
    //         with a given Avro schema.
    // //////////////////////////////////////////////////////////

    /**
     * Validate whether pigSchema is compatible with avroSchema
     */
    public static Schema validateAndConvert(Schema avroSchema, ResourceSchema pigSchema) throws IOException {
        return validateAndConvertRecord(avroSchema, pigSchema.getFields());
    }

    /**
     * Validate whether pigSchema is compatible with avroSchema and convert
     * those Pig fields with to corresponding Avro schemas.
     */
    protected static Schema validateAndConvert(Schema avroSchema, ResourceFieldSchema pigSchema) throws IOException {

        AvroStorageLog.details("Validate pig field schema:" + pigSchema);

        /* compatibility check based on data types */
        if (!isCompatible(avroSchema, pigSchema))
            throw new IOException("Schemas are not compatible.\n Avro=" + avroSchema + "\n" + "Pig=" + pigSchema);

        final byte pigType = pigSchema.getType();
        if (avroSchema.getType().equals(Schema.Type.UNION)) {
            AvroStorageLog.details("Validate Pig schema with Avro union:" + avroSchema);

            List<Schema> unionSchemas = avroSchema.getTypes();
            for (Schema schema : unionSchemas) {
                try {
                    @SuppressWarnings("unused")
                    Schema s = validateAndConvert(schema, pigSchema);
                    return avroSchema;
                } catch (IOException e) {
                    // ignore the unmatched one
                }
            }
            throw new IOException("pig schema " + pigSchema  + " is not compatible with avro " + avroSchema);
        } else if (pigType == DataType.TUPLE) {
            AvroStorageLog.details("Validate a pig tuple: " + pigSchema);
            ResourceFieldSchema[] pigFields = pigSchema.getSchema().getFields();
            Schema outSchema = validateAndConvertRecord(avroSchema, pigFields);
            return outSchema;

        } else if (pigType == DataType.BAG) {
            AvroStorageLog.details("Validate a pig bag:" + pigSchema);

            /* get fields of containing tuples */
            ResourceFieldSchema[] fs = pigSchema.getSchema().getFields();
            if (fs == null || fs.length != 1 || fs[0].getType() != DataType.TUPLE)
                throw new IOException("Expect one tuple field in a bag");

            Schema inElemSchema = avroSchema.getElementType();
            Schema outSchema = Schema.createArray(validateAndConvert(inElemSchema, fs[0]));
            return outSchema;
        } else if (pigType == DataType.MAP) {
            AvroStorageLog.details("Cannot validate a pig map. Will use user defined Avro schema.");
            return avroSchema;

        } else if (pigType == DataType.UNKNOWN  || pigType == DataType.CHARARRAY
                                                || pigType == DataType.BIGCHARARRAY
                                                || pigType == DataType.BOOLEAN
                                                || pigType == DataType.BYTE
                                                || pigType == DataType.BYTEARRAY
                                                || pigType == DataType.DOUBLE
                                                || pigType == DataType.FLOAT
                                                || pigType == DataType.INTEGER
                                                || pigType == DataType.LONG) {

            AvroStorageLog.details("Validate a pig primitive type:" + pigSchema);
            return avroSchema;

        } else
            throw new IOException("Unsupported pig type:" + DataType.findTypeName(pigType));
    }

    /**
     * Validate a Pig tuple is compatible with Avro record. If the Avro schema 
     * is not complete (with uncovered fields), then convert those fields using 
     * methods in set 1. 
     * 
     * Notice that users can get rid of Pig tuple wrappers, e.g. an Avro schema
     * "int" is compatible with a Pig schema "T:(int)"
     * 
     */
    protected static Schema validateAndConvertRecord(Schema avroSchema, ResourceFieldSchema[] pigFields) throws IOException {

        /* Get rid of Pig tuple wrappers. */
        if (!avroSchema.getType().equals(Schema.Type.RECORD)) {
            if (pigFields.length != 1)
                throw new IOException("Expect only one field in Pig tuple schema. Avro schema is " + avroSchema.getType());

            return validateAndConvert(avroSchema, pigFields[0]);
        }

        /* validate and convert a pig tuple with avro record */
        boolean isPartialSchema = AvroStorageUtils.isUDPartialRecordSchema(avroSchema);
        AvroStorageLog.details("isPartialSchema=" + isPartialSchema);

        String typeName = isPartialSchema ? getRecordName() : avroSchema.getName();
        Schema outSchema = Schema.createRecord(typeName, avroSchema.getDoc(), avroSchema.getNamespace(), false);

        List<Schema.Field> inFields = avroSchema.getFields();
        if (!isPartialSchema && inFields.size() != pigFields.length) {
            throw new IOException("Expect " + inFields.size() + " fields in pig schema." + " But there are " + pigFields.length);
        }

        List<Schema.Field> outFields = new ArrayList<Schema.Field>();

        for (int i = 0; i < pigFields.length; i++) {
            /* get user defined avro field schema */
            Field inputField = isPartialSchema ? AvroStorageUtils.getUDField(avroSchema, i) : inFields.get(i);

            /* get schema */
            Schema fieldSchema = null;
            if (inputField == null) { 
                /* convert pig schema (nullable) */
                fieldSchema = convert(pigFields[i], true);
            } else if (inputField.schema() == null) { 
                /* convert pig schema (not-null) */
                fieldSchema = convert(pigFields[i], false);
            } else { 
                /* validate pigFields[i] with given avro schema */
                fieldSchema = validateAndConvert(inputField.schema(),
                                                pigFields[i]);
            }

            /* get field name of output */
            String outname = (isPartialSchema) ? pigFields[i].getName() : inputField.name();
            if (outname == null)
                outname = FIELD_NAME + "_" + i; // field name cannot be null

            /* get doc of output */
            String doc = (isPartialSchema) ? pigFields[i].getDescription() : inputField.doc();

            JsonNode defaultvalue = (inputField != null) ? inputField.defaultValue() : null;

            outFields.add(new Field(outname, fieldSchema, doc, defaultvalue));

        }

        outSchema.setFields(outFields);
        return outSchema;

    }

    /**
     * Check whether Avro type is compatible with Pig type
     * 
     */
    protected static boolean isCompatible(Schema avroSchema, ResourceFieldSchema pigSchema) {

        Schema.Type avroType = avroSchema.getType();
        byte pigType = pigSchema.getType();

        if (avroType.equals(Schema.Type.UNION)) {
            return true;
        } else if (pigType == DataType.TUPLE) {
            /* Tuple is compatible with any type; for users may want to
               get rid of the tuple wrapper */
            return true;
        }
        return  (avroType.equals(Schema.Type.ARRAY) && pigType == DataType.BAG)
                      || (avroType.equals(Schema.Type.MAP) && pigType == DataType.MAP)
                      || (avroType.equals(Schema.Type.STRING) 
                                                      && pigType == DataType.CHARARRAY 
                                                      || pigType == DataType.BIGCHARARRAY)
                      || (avroType.equals(Schema.Type.ENUM) 
                                                      && pigType == DataType.CHARARRAY)
                      || (avroType.equals(Schema.Type.BOOLEAN) 
                                                      && pigType == DataType.BOOLEAN 
                                                      || pigType == DataType.INTEGER)
                      || (avroType.equals(Schema.Type.BYTES) 
                                                      && pigType == DataType.BYTEARRAY)
                      || (avroType.equals(Schema.Type.DOUBLE) 
                                                      && pigType == DataType.DOUBLE
                                                      || pigType == DataType.FLOAT
                                                      || pigType == DataType.INTEGER 
                                                      || pigType == DataType.LONG)
                      || (avroType.equals(Schema.Type.FLOAT)
                                                      && pigType == DataType.FLOAT
                                                      || pigType == DataType.INTEGER 
                                                      || pigType == DataType.LONG)
                      || (avroType.equals(Schema.Type.FIXED) 
                                                      && pigType == DataType.BYTEARRAY)
                      || (avroType.equals(Schema.Type.INT) 
                                                      && pigType == DataType.INTEGER)
                      || (avroType.equals(Schema.Type.LONG)
                                                      && pigType == DataType.LONG 
                                                      || pigType == DataType.INTEGER);

    }

}
