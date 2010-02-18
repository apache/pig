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

package org.apache.pig.experimental.logical.relational;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema, from a logical perspective.
 */
public class LogicalSchema {

    public static class LogicalFieldSchema {
        public String alias;
        public byte type;
        public long uid;
        public LogicalSchema schema;

        public LogicalFieldSchema(String alias, LogicalSchema schema,  byte type) {
            this(alias, schema, type, -1);
        }
        
        public LogicalFieldSchema(String alias, LogicalSchema schema,  byte type, long uid) {
            this.alias = alias;
            this.type = type;
            this.schema = schema;
            this.uid = uid;
        }
        
        /**
         * Equality is defined as having the same type and either the same schema
         * or both null schema.  Alias and uid are not checked.
         */
        public boolean isEqual(Object other) {
            if (other instanceof LogicalFieldSchema) {
                LogicalFieldSchema ofs = (LogicalFieldSchema)other;
                if (type != ofs.type) return false;
                if (schema == null && ofs.schema == null) return true;
                if (schema == null) return false;
                else return schema.isEqual(ofs.schema);
            } else {
                return false;
            }
        }
    }

    
    
    private List<LogicalFieldSchema> fields;
    private Map<String, Integer> aliases;
    
    public LogicalSchema() {
        fields = new ArrayList<LogicalFieldSchema>();
        aliases = new HashMap<String, Integer>();
    }
    
    /**
     * Add a field to this schema.
     * @param field to be added to the schema
     */
    public void addField(LogicalFieldSchema field) {
        fields.add(field);
        if (field.alias != null && !field.alias.equals("")) {
            aliases.put(field.alias, fields.size() - 1);
            int index = 0;
            while(index != -1) {
                index = field.alias.indexOf("::", index);
                if (index != -1) {
                    String a = field.alias.substring(index+2);
                    if (aliases.containsKey(a)) {
                        aliases.remove(a);
                    }else{
                        aliases.put(a, fields.size()-1);                       
                    }

                    index = index +2;
                }
            }
        }
    }
    
    /**
     * Fetch a field by alias
     * @param alias
     * @return field associated with alias, or null if no such field
     */
    public LogicalFieldSchema getField(String alias) {
        Integer index = aliases.get(alias);
        if (index == null) {
            return null;
        }

        return fields.get(index);
    }

    /**
     * Fetch a field by field number
     * @param fieldNum field number to fetch
     * @return field
     */
    public LogicalFieldSchema getField(int fieldNum) {
        return fields.get(fieldNum);
    }
    
    /**
     * Get all fields
     * @return list of all fields
     */
    public List<LogicalFieldSchema> getFields() {
        return fields;
    }

    /**
     * Get the size of the schema.
     * @return size
     */
    public int size() {
       return fields.size();
    }
    
    /**
     * Two schemas are equal if they are of equal size and their fields
     * schemas considered in order are equal.
     */
    public boolean isEqual(Object other) {
        if (other != null && other instanceof LogicalSchema) {
            LogicalSchema os = (LogicalSchema)other;
            if (size() != os.size()) return false;
            for (int i = 0; i < size(); i++) {
                if (!getField(i).isEqual(os.getField(i))) return false;
            }
            return true;
        } else {
            return false;
        }
        
    }
    
    /**
     * Merge two schemas.
     * @param s1
     * @param s2
     * @return a merged schema, or null if the merge fails
     */
    public static LogicalSchema merge(LogicalSchema s1, LogicalSchema s2) {
        // TODO
        return null;
    }
    
}
