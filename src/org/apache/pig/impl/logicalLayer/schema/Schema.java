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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.parser.ParseException;

public class Schema {

    public static class FieldSchema {
        /**
         * Alias for this field.
         */
        public String alias;

        /**
         * Datatype, using codes from {@link org.apache.pig.data.DataType}.
         */
        public byte type;

        /**
         * If this is a tuple itself, it can have a schema.  Otherwise this
         * field must be null.
         */
        public Schema schema;

        /**
         * Constructor for any type.
         * @param a Alias, if known.  If unknown leave null.
         * @param t Type, using codes from {@link org.apache.pig.data.DataType}.
         */
        public FieldSchema(String a, byte t) {
            alias = a;
            type = t ;
            schema = null;
        }

        /**
         * Constructor for tuple fields.
         * @param a Alias, if known.  If unknown leave null.
         * @param s Schema of this tuple.
         */
        public FieldSchema(String a, Schema s) {
            alias = a;
            type = DataType.TUPLE;
            schema = s;
        }
    }

    private List<FieldSchema> mFields;
    private Map<String, FieldSchema> mAliases;

    /**
     * @param fields List of field schemas that describes the fields.
     */
    public Schema(List<FieldSchema> fields) {
        mFields = fields;
        mAliases = new HashMap<String, FieldSchema>(fields.size());
        for (FieldSchema fs : fields) {
            if (fs.alias != null) mAliases.put(fs.alias, fs);
        }
    }

    /**
     * Create a schema with only one field.
     * @param fieldSchema field to put in this schema.
     */
    public Schema(FieldSchema fieldSchema) {
        mFields = new ArrayList<FieldSchema>(1);
        mFields.add(fieldSchema);
        if (fieldSchema.alias != null) {
            mAliases.put(fieldSchema.alias, fieldSchema);
        }
    }

    /**
     * Given an alias name, find the associated FieldSchema.
     * @param alias Alias to look up.
     * @return FieldSchema, or null if no such alias is in this tuple.
     */
    public FieldSchema getField(String alias) {
        return mAliases.get(alias);
    }

    /**
     * Given a field number, find the associated FieldSchema.
     * @param fieldNum Field number to look up.
     * @return FieldSchema for this field.
     * @throws ParseException if the field number exceeds the number of
     * fields in the tuple.
     */
    public FieldSchema getField(int fieldNum) throws ParseException {
        if (fieldNum >= mFields.size()) {
            throw new ParseException("Attempt to fetch field " + fieldNum +
                " from tuple of size " + mFields.size());
        }

        return mFields.get(fieldNum);
    }

    /**
     * Find the number of fields in the schema.
     * @return number of fields.
     */
    public int size() {
        return mFields.size();
    }

    /**
     * Reconcile this schema with another schema.  The schema being reconciled
     * with should have the same number of columns.  The use case is where a
     * schema already exists but may not have alias and or type information.  If
     * an alias exists in this schema and a new one is given, then the new one
     * will be used.  Similarly with types, though this needs to be used
     * carefully, as types should not be lightly changed.
     * @param other Schema to reconcile with.
     * @throws ParseException if this cannot be reconciled.
     */
    public void reconcile(Schema other) throws ParseException {
        if (other.size() != size()) {
            throw new ParseException("Cannot reconcile schemas with different "
                + "sizes.  This schema has size " + size() + " other has size " 
                + "of " + other.size());
        }

        Iterator<FieldSchema> i = other.mFields.iterator();
        for (int j = 0; i.hasNext(); j++) {
            FieldSchema otherFs = i.next();
            FieldSchema ourFs = mFields.get(j);
            if (otherFs.alias != null) ourFs.alias = otherFs.alias; 
            if (otherFs.type != DataType.UNKNOWN) ourFs.type = otherFs.type; 
            if (otherFs.schema != null) ourFs.schema = otherFs.schema; 
        }

    }
}



