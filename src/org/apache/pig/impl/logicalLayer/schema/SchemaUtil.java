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

package org.apache.pig.impl.logicalLayer.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 *
 * A utility class for simplify the schema creation, especially for bag and
 * tuple schema. Currently, it only support simple schema creation, nested tuple
 * and bag is not supported
 *
 */

public class SchemaUtil {

    private static Set<Byte> SUPPORTED_TYPE_SET;

    static {
        SUPPORTED_TYPE_SET = new HashSet<Byte>();

        SUPPORTED_TYPE_SET.add(DataType.INTEGER);
        SUPPORTED_TYPE_SET.add(DataType.LONG);
        SUPPORTED_TYPE_SET.add(DataType.CHARARRAY);
        SUPPORTED_TYPE_SET.add(DataType.BOOLEAN);
        SUPPORTED_TYPE_SET.add(DataType.BYTE);
        SUPPORTED_TYPE_SET.add(DataType.BYTEARRAY);
        SUPPORTED_TYPE_SET.add(DataType.DOUBLE);
        SUPPORTED_TYPE_SET.add(DataType.FLOAT);
        SUPPORTED_TYPE_SET.add(DataType.DATETIME);
        SUPPORTED_TYPE_SET.add(DataType.MAP);
        SUPPORTED_TYPE_SET.add(DataType.BIGINTEGER);
        SUPPORTED_TYPE_SET.add(DataType.BIGDECIMAL);
    }

    /**
     * Create a new tuple schema according the tuple name and two list: names of
     * fields, types of fields
     *
     * @param tupleName
     * @param fieldNames
     * @param dataTypes
     * @return tuple schema
     * @throws FrontendException
     */
    public static Schema newTupleSchema(String tupleName,
            List<String> fieldNames, List<Byte> dataTypes)
            throws FrontendException {
        checkParameters(fieldNames, dataTypes);

        List<Schema.FieldSchema> tokenSchemas = new ArrayList<Schema.FieldSchema>();
        for (int i = 0; i < fieldNames.size(); ++i) {
            String name = fieldNames.get(i);
            Byte type = dataTypes.get(i);
            tokenSchemas.add(new Schema.FieldSchema(name, type));
        }

        Schema tupleSchema = new Schema(tokenSchemas);
        Schema.FieldSchema tupleField = new Schema.FieldSchema(tupleName,
                tupleSchema);

        return new Schema(tupleField);
    }

    /**
     * Create a new tuple schema according the tuple name and two arrays: names
     * of fields, types of fields
     *
     * @param tupleName
     * @param fieldNames
     * @param dataTypes
     * @return tuple schema
     * @throws FrontendException
     */
    public static Schema newTupleSchema(String tupleName, String[] fieldNames,
            Byte[] dataTypes) throws FrontendException {
        return newTupleSchema(tupleName, Arrays.asList(fieldNames), Arrays
                .asList(dataTypes));
    }

    /**
     * Create a new tuple schema according the two list: names of fields, types
     * of fields, the default tuple name is t.
     *
     * @param fieldNames
     * @param dataTypes
     * @return tuple schema
     * @throws FrontendException
     */
    public static Schema newTupleSchema(List<String> fieldNames,
            List<Byte> dataTypes) throws FrontendException {
        return newTupleSchema("t", fieldNames, dataTypes);
    }

    /**
     * Create a new tuple schema according one list: types of fields, the
     * default names of fields are f0,f1,f2..., and the tuple name is t.
     *
     * @param dataTypes
     * @return tuple schema
     * @throws FrontendException
     */
    public static Schema newTupleSchema(List<Byte> dataTypes)
            throws FrontendException {
        List<String> names = newNames(dataTypes.size());
        return newTupleSchema("t", names, dataTypes);
    }

    /**
     * Create a new tuple schema according the two arrays: names of fields,
     * types of fields, the default tuple name is t.
     *
     * @param names
     * @param dataTypes
     * @return tuple schema
     * @throws FrontendException
     */
    public static Schema newTupleSchema(String[] names, Byte[] dataTypes)
            throws FrontendException {
        return newTupleSchema("t", Arrays.asList(names), Arrays
                .asList(dataTypes));
    }

    /**
     * Create a new tuple schema according one array: types of fields, the
     * default names of fields are f0,f1,f2..., and the tuple name is t.
     *
     * @param dataTypes
     * @return tuple schema
     * @throws FrontendException
     */
    public static Schema newTupleSchema(Byte[] dataTypes)
            throws FrontendException {
        return newTupleSchema(Arrays.asList(dataTypes));
    }

    private static List<String> newNames(int size) {
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < size; ++i) {
            names.add("f" + i);
        }
        return names;
    }

    /**
     * Create a bag schema according the bag name,tuple name and two list: name
     * of fields, type of fields
     *
     * @param bagName
     * @param tupleName
     * @param fieldNames
     * @param dataTypes
     * @return bag schema
     * @throws FrontendException
     */
    public static Schema newBagSchema(String bagName, String tupleName,
            List<String> fieldNames, List<Byte> dataTypes)
            throws FrontendException {
        checkParameters(fieldNames, dataTypes);

        Schema tupleSchema = newTupleSchema(tupleName, fieldNames, dataTypes);
        Schema.FieldSchema bagField = new Schema.FieldSchema(bagName,
                tupleSchema, DataType.BAG);

        return new Schema(bagField);
    }

    public static Schema newBagSchema(String bagName, String tupleName,
            String[] fieldNames, Byte[] dataTypes) throws FrontendException {
        return newBagSchema(bagName, tupleName, Arrays.asList(fieldNames),
                Arrays.asList(dataTypes));
    }

    /**
     * Create a bag schema according two list: name of fields, type of fields,
     * and the default bag name is b, the default tuple name is t.
     *
     * @param names
     * @param dataTypes
     * @return bag schema
     * @throws FrontendException
     */
    public static Schema newBagSchema(List<String> names, List<Byte> dataTypes)
            throws FrontendException {
        checkParameters(names, dataTypes);

        Schema tupleSchema = newTupleSchema(names, dataTypes);
        Schema.FieldSchema bagField = new Schema.FieldSchema("b", tupleSchema,
                DataType.BAG);

        return new Schema(bagField);
    }

    /**
     * Create a new tuple schema according one list: types of fields, the
     * default names of fields are f0,f1,f2..., and the tuple is t, the bag name
     * is b.
     *
     * @param dataTypes
     * @return bag schema
     * @throws FrontendException
     */
    public static Schema newBagSchema(List<Byte> dataTypes)
            throws FrontendException {
        List<String> names = newNames(dataTypes.size());
        return newBagSchema(names, dataTypes);
    }

    /**
     * Create a new tuple schema according two arrays: names of field,types of
     * fields. The default tuple name is t, and the bag is b.
     *
     * @param names
     * @param dataTypes
     * @return bag schema
     * @throws FrontendException
     */
    public static Schema newBagSchema(String[] names, Byte[] dataTypes)
            throws FrontendException {
        return newBagSchema(Arrays.asList(names), Arrays.asList(dataTypes));
    }

    /**
     * Create a new tuple schema according one array: the type of fields, the
     * tuple name is t, and the bag name is b.
     *
     * @param dataTypes
     * @return bag schema
     * @throws FrontendException
     */
    public static Schema newBagSchema(Byte[] dataTypes)
            throws FrontendException {
        return newBagSchema(Arrays.asList(dataTypes));
    }

    private static void checkDataTypes(List<Byte> dataTypes)
            throws FrontendException {
        for (Byte type : dataTypes) {
            if (!SUPPORTED_TYPE_SET.contains(type)) {
                throw new FrontendException(
                        "Currently pig do not support this kind of type using Schema:"
                                + DataType.findTypeName(type)
                                + ". You can write shema by yourself.");
            }
        }

    }

    private static void checkParameters(List<String> names, List<Byte> dataTypes)
            throws FrontendException {
        // TODO Auto-generated method stub
        checkDataTypes(dataTypes);
        if (names.size() != dataTypes.size()) {
            throw new FrontendException(
                    "The number of names is not equal to the number of dataTypes");
        }
    }

}
