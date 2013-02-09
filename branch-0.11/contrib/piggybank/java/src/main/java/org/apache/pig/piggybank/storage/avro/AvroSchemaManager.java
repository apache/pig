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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

/**
 * This class creates two maps out of a given Avro schema. And it supports
 * looking up avro schemas using either type name or field name.
 *
 * 1. map[type name] = > avro schema
 * 2. map[field name] => avro schema
 *
 */
public class AvroSchemaManager {

    /**map[field name] => schema */
    Map<String, Schema> name2Schema = null;
    /**map[type name]=> schema*/
    Map<String, Schema> typeName2Schema = null;

    /**
     * Construct with a given schema
     */
    public AvroSchemaManager(Schema schema) throws IOException {

        name2Schema = new HashMap<String, Schema>();
        typeName2Schema = new HashMap<String, Schema>();

        if (AvroStorageUtils.containsRecursiveRecord(schema)) {
            throw new IOException ("Schema containing recursive records cannot be referred to"
                + " by 'data' and 'schema_file'. Please instead use 'same' with a path that"
                + " points to an avro file encoded by the same schema as what you want to use,"
                + " or use 'schema' with a json string representation." );
        }

        init(null, schema, false);
    }

    private boolean isNamedSchema(Schema schema) {
        Type type = schema.getType();
        return type.equals(Type.RECORD) || type.equals(Type.ENUM) || type.equals(Type.FIXED);
    }

    /**
     * Initialize given a schema
     */
    protected void init(String namespace, Schema schema,
                                    boolean ignoreNameMap) {

        /* put to map[type name]=>schema */
        if (isNamedSchema(schema)) {
            String typeName = schema.getName();
            if (typeName2Schema.containsKey(typeName))
                AvroStorageLog.warn("Duplicate schemas defined for type:"
                        + typeName
                        + ". will ignore the second one:"
                        + schema);
            else {
                AvroStorageLog.details("add " + schema.getName() + "=" + schema
                        + " to type2Schema");
                typeName2Schema.put(schema.getName(), schema);
            }
        }

        /* put field schema to map[field name]=>schema*/
        if (schema.getType().equals(Type.RECORD)) {

            List<Field> fields = schema.getFields();
            for (Field field : fields) {

                Schema fieldSchema = field.schema();
                String name = (namespace == null) ? field.name()  : namespace + "." + field.name();

                if (!ignoreNameMap) {
                    if (name2Schema.containsKey(name))
                        AvroStorageLog.warn("Duplicate schemas defined for alias:" + name
                                          + ". Will ignore the second one:"+ fieldSchema);
                    else {
                        AvroStorageLog.details("add " + name + "=" + fieldSchema + " to name2Schema");
                        name2Schema.put(name, fieldSchema);
                    }
                }

                init(name, fieldSchema, ignoreNameMap);
            }
        } else if (schema.getType().equals(Type.UNION)) {

            if (AvroStorageUtils.isAcceptableUnion(schema)) {
                Schema realSchema = AvroStorageUtils.getAcceptedType(schema);
                init(namespace, realSchema, ignoreNameMap);
            } else {
                List<Schema> list = schema.getTypes();
                for (Schema s : list) {
                    init(namespace, s, true);
                }
            }
        } else if (schema.getType().equals(Type.ARRAY)) {
            Schema elemSchema = schema.getElementType();
            init(namespace, elemSchema, true);
        } else if (schema.getType().equals(Type.MAP)) {
            Schema valueSchema = schema.getValueType();
            init(namespace, valueSchema, true);
        }
    }

    /**
     * Look up schema using type name or field name
     */
    public Schema getSchema(String name) {
        Schema schema = typeName2Schema.get(name);
        schema = (schema == null) ? name2Schema.get(name) : schema;
        return schema;

    }
}
