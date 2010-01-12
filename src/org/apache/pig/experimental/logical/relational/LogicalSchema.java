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

import org.apache.pig.data.DataType;

/**
 * Schema, from a logical perspective.
 */
public class LogicalSchema {

    public static class LogicalFieldSchema {
        public String alias;
        public DataType type;
        public LogicalSchema schema;
    }
    
    private List<LogicalFieldSchema> fields;
    private Map<String, Integer> aliases;
    
    public LogicalSchema() {
        fields = new ArrayList<LogicalFieldSchema>();
        aliases = new HashMap<String, Integer>();
    }
    
    public void addField(LogicalFieldSchema field) {
        fields.add(field);
        if (field.alias != null && field.alias.equals("")) {
            aliases.put(field.alias, fields.size() - 1);
        }
    }
    
    public LogicalFieldSchema getField(String alias) {
        return null;
    }

    public LogicalFieldSchema getField(int fieldNum) {
        return fields.get(fieldNum);
    }

    public Integer size() {
       return null;
    }
    
    public static LogicalSchema merge(LogicalSchema s1, LogicalSchema s2) {
        // TODO
        return null;
    }
    
}
