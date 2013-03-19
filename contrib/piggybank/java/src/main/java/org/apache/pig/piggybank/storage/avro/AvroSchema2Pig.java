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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.data.DataType;

/**
 * This class converts Avro schema to Pig schema
 */
public class AvroSchema2Pig {

    public static String RECORD = "RECORD";
    public static String FIELD = "FIELD";
    public static String ARRAY_FIELD = "ARRAY_ELEM";
    public static String MAP_VALUE_FIELD = "m_value";

    /**
     * Wrap a pig type to a field schema
     */
    public static ResourceFieldSchema getPigSchema(byte pigType, String fieldName) {
        return new ResourceFieldSchema( new FieldSchema(fieldName, pigType));
    }

    /**
     * Convert an Avro schema to a Pig schema
     */
    public static ResourceSchema convert(Schema schema) throws IOException {

        if (AvroStorageUtils.containsGenericUnion(schema))
            throw new IOException ("We don't accept schema containing generic unions.");

        Set<Schema> visitedRecords = new HashSet<Schema>();
        ResourceFieldSchema inSchema = inconvert(schema, FIELD, visitedRecords);

        ResourceSchema tupleSchema;
        if (inSchema.getType() == DataType.TUPLE) {
            tupleSchema = inSchema.getSchema();
        } else { // other typs
            ResourceFieldSchema tupleWrapper = AvroStorageUtils.wrapAsTuple(inSchema);

            ResourceSchema topSchema = new ResourceSchema();
            topSchema.setFields(new ResourceFieldSchema[] { tupleWrapper });

            tupleSchema = topSchema;

        }
        return tupleSchema;
    }

    /**
     * Convert a schema with field name to a pig schema
     */
     private static ResourceFieldSchema inconvert(Schema in, String fieldName, Set<Schema> visitedRecords)
             throws IOException {

        AvroStorageLog.details("InConvert avro schema with field name " + fieldName);

        Schema.Type avroType = in.getType();
        ResourceFieldSchema fieldSchema = new ResourceFieldSchema();
        fieldSchema.setName(fieldName);

        if (avroType.equals(Schema.Type.RECORD)) {

            AvroStorageLog.details("convert to a pig tuple");

            if (visitedRecords.contains(in)) {
                fieldSchema.setType(DataType.BYTEARRAY);
            } else {
                visitedRecords.add(in);
                fieldSchema.setType(DataType.TUPLE);
                ResourceSchema tupleSchema = new ResourceSchema();
                List<Schema.Field> fields = in.getFields();
                ResourceFieldSchema[] childFields = new ResourceFieldSchema[fields.size()];
                int index = 0;
                for (Schema.Field field : fields) {
                    childFields[index++] = inconvert(field.schema(), field.name(), visitedRecords);
                }

                tupleSchema.setFields(childFields);
                fieldSchema.setSchema(tupleSchema);
                visitedRecords.remove(in);
            }

        } else if (avroType.equals(Schema.Type.ARRAY)) {

            AvroStorageLog.details("convert array to a pig bag");
            fieldSchema.setType(DataType.BAG);
            Schema elemSchema = in.getElementType();
            ResourceFieldSchema subFieldSchema = inconvert(elemSchema, ARRAY_FIELD, visitedRecords);
            add2BagSchema(fieldSchema, subFieldSchema);

        } else if (avroType.equals(Schema.Type.MAP)) {

            AvroStorageLog.details("convert map to a pig map");
            fieldSchema.setType(DataType.MAP);

        } else if (avroType.equals(Schema.Type.UNION)) {

            if (AvroStorageUtils.isAcceptableUnion(in)) {
                Schema acceptSchema = AvroStorageUtils.getAcceptedType(in);
                ResourceFieldSchema realFieldSchema = inconvert(acceptSchema, null, visitedRecords);
                fieldSchema.setType(realFieldSchema.getType());
                fieldSchema.setSchema(realFieldSchema.getSchema());
            } else
                throw new IOException("Do not support generic union:" + in);

        } else if (avroType.equals(Schema.Type.FIXED)) {
             fieldSchema.setType(DataType.BYTEARRAY);
        } else if (avroType.equals(Schema.Type.BOOLEAN)) {
            fieldSchema.setType(DataType.BOOLEAN);
        } else if (avroType.equals(Schema.Type.BYTES)) {
            fieldSchema.setType(DataType.BYTEARRAY);
        } else if (avroType.equals(Schema.Type.DOUBLE)) {
            fieldSchema.setType(DataType.DOUBLE);
        } else if (avroType.equals(Schema.Type.ENUM)) {
            fieldSchema.setType(DataType.CHARARRAY);
        } else if (avroType.equals(Schema.Type.FLOAT)) {
            fieldSchema.setType(DataType.FLOAT);
        } else if (avroType.equals(Schema.Type.INT)) {
            fieldSchema.setType(DataType.INTEGER);
        } else if (avroType.equals(Schema.Type.LONG)) {
            fieldSchema.setType(DataType.LONG);
        } else if (avroType.equals(Schema.Type.STRING)) {
            fieldSchema.setType(DataType.CHARARRAY);
        } else if (avroType.equals(Schema.Type.NULL)) {
            // value of NULL is always NULL
            fieldSchema.setType(DataType.INTEGER);
        } else {
            throw new IOException("Unsupported avro type:" + avroType);
        }
        return fieldSchema;
    }

     /**
      * Add a field schema to a bag schema
      */
    static protected void add2BagSchema(ResourceFieldSchema fieldSchema,
                                    ResourceFieldSchema subFieldSchema)
                                    throws IOException {

        ResourceFieldSchema wrapped = (subFieldSchema.getType() == DataType.TUPLE)
                                                              ? subFieldSchema
                                                              : AvroStorageUtils.wrapAsTuple(subFieldSchema);

        ResourceSchema listSchema = new ResourceSchema();
        listSchema.setFields(new ResourceFieldSchema[] { wrapped });

        fieldSchema.setSchema(listSchema);

    }

}
