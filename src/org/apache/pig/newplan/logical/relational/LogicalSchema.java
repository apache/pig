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

package org.apache.pig.newplan.logical.relational;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.logical.expression.LogicalExpression;

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
        
        public LogicalFieldSchema(LogicalFieldSchema fs) {
            this(fs.alias, fs.schema, fs.type, fs.uid);
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
               
        public String toString() {
            if( type == DataType.BAG ) {
                if( schema == null ) {
                    return ( alias + "#" + uid + ":bag{}#" );
                }
                return ( alias + "#" + uid + ":bag{" + schema.toString() + "}" );
            } else if( type == DataType.TUPLE ) {
                if( schema == null ) {
                    return ( alias + "#" + uid + ":tuple{}" );
                }
                return ( alias + "#" + uid + ":tuple(" + schema.toString() + ")" );
            }
            return ( alias + "#" + uid + ":" + DataType.findTypeName(type) );
        }
        
        public void stampFieldSchema() {
            if (uid==-1)
                uid = LogicalExpression.getNextUid();
            if (schema!=null) {
                for (LogicalFieldSchema fs : schema.getFields()) {
                    fs.stampFieldSchema();
                }
            }
        }
        
        private boolean compatible(LogicalFieldSchema uidOnlyFieldSchema) {
            if (uidOnlyFieldSchema==null)
                return false;
            if (this.schema==null && uidOnlyFieldSchema.schema!=null ||
                    this.schema!=null && uidOnlyFieldSchema==null)
                return false;
            if (this.schema!=null) {
                if (this.schema.size()!=uidOnlyFieldSchema.schema.size())
                    return false;
                for (int i=0;i<this.schema.size();i++) {
                    boolean comp = schema.getField(i).compatible(uidOnlyFieldSchema.schema.getField(i));
                    if (!comp) return false;
                }
            }
            return true;
        }
        public LogicalSchema.LogicalFieldSchema mergeUid(LogicalFieldSchema uidOnlyFieldSchema) throws FrontendException {
            if (uidOnlyFieldSchema!=null && compatible(uidOnlyFieldSchema)) {
                this.uid = uidOnlyFieldSchema.uid;
                if (this.schema!=null) {
                    for (int i=0;i<this.schema.size();i++) {
                        schema.getField(i).mergeUid(uidOnlyFieldSchema.schema.getField(i));
                    }
                }
                return uidOnlyFieldSchema;
            }
            else {
                if (uidOnlyFieldSchema==null) {
                    stampFieldSchema();
                }
                else {
                    this.uid = uidOnlyFieldSchema.uid;
                    if (this.schema!=null) {
                        for (int i=0;i<this.schema.size();i++) {
                            schema.getField(i).stampFieldSchema();
                        }
                    }
                }
                LogicalFieldSchema clonedUidOnlyCopy = cloneUid();
                return clonedUidOnlyCopy;
            }
        }
        
        public LogicalFieldSchema cloneUid() {
            LogicalFieldSchema resultFs = null;
            if (schema==null) {
                resultFs = new LogicalFieldSchema(null, null, type, uid);
            }
            else {
                LogicalSchema newSchema = new LogicalSchema();
                resultFs = new LogicalFieldSchema(null, newSchema, type, uid);
                for (int i=0;i<schema.size();i++) {
                    LogicalFieldSchema fs = schema.getField(i).cloneUid();
                    newSchema.addField(fs);
                }
            }
            return resultFs;
        }
        
        public LogicalFieldSchema deepCopy() {
            LogicalFieldSchema newFs = new LogicalFieldSchema(alias!=null?alias:null, schema!=null?schema.deepCopy():null, 
                    type, uid);
            return newFs;
        }
    }
    
    private List<LogicalFieldSchema> fields;
    private Map<String, Pair<Integer, Boolean>> aliases;
    
    private boolean twoLevelAccessRequired = false;
    
    public LogicalSchema() {
        fields = new ArrayList<LogicalFieldSchema>();
        aliases = new HashMap<String, Pair<Integer, Boolean>>();
    }
    
    /**
     * Add a field to this schema.
     * @param field to be added to the schema
     */
    public void addField(LogicalFieldSchema field) {
        fields.add(field);
        if (field.alias != null && !field.alias.equals("")) {
            // put the full name of this field into aliases map
            // boolean in the pair indicates if this alias is full name
            aliases.put(field.alias, new Pair<Integer, Boolean>(fields.size()-1, true));
            int index = 0;
            
            // check and put short names into alias map if there is no conflict
            
            while(index != -1) {
                index = field.alias.indexOf("::", index);
                if (index != -1) {
                    String a = field.alias.substring(index+2);
                    if (aliases.containsKey(a)) {
                        // remove conflict if the conflict is not full name
                        // we can never remove full name
                        if (!aliases.get(a).second) {
                            aliases.remove(a);
                        }
                    }else{
                        // put alias into map and indicate it is a short name
                        aliases.put(a, new Pair<Integer, Boolean>(fields.size()-1, false));                       
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
        Pair<Integer, Boolean> p = aliases.get(alias);
        if (p == null) {
            return null;
        }

        return fields.get(p.first);
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
     * Look for the index of the field that contains the specified uid
     * @param uid the uid to look for
     * @return the index of the field, -1 if not found
     */
    public int findField(long uid) {            
        
        for(int i=0; i< size(); i++) {
            LogicalFieldSchema f = getField(i);
            // if this field has the same uid, then return this field
            if (f.uid == uid) {
                return i;
            } 
            
            // if this field has a schema, check its schema
            if (f.schema != null) {
                if (f.schema.findField(uid) != -1) {
                    return i;
                }
            }
        }
        
        return -1;
    }
    
    
    /**
     * Merge two schemas.
     * @param s1
     * @param s2
     * @return a merged schema, or null if the merge fails
     */
    public static LogicalSchema merge(LogicalSchema s1, LogicalSchema s2) throws FrontendException {
        // If any of the schema is null, take the other party
        if (s1==null || s2==null) {
            if (s1!=null) return s1.deepCopy();
            else if (s2!=null) return s2.deepCopy();
            else return null;
        }
        
        if (s1.size()!=s2.size()) return null;
        LogicalSchema mergedSchema = new LogicalSchema();
        for (int i=0;i<s1.size();i++) {
            String mergedAlias;
            byte mergedType;
            LogicalSchema mergedSubSchema = null;
            LogicalFieldSchema fs1 = s1.getField(i);
            LogicalFieldSchema fs2 = s2.getField(i);
            
            if (fs1.alias==null)
                mergedAlias = fs2.alias;
            else {
                mergedAlias = fs1.alias; // If both schema have alias, the first one win
            }
            if (fs1.type==DataType.NULL)
                mergedType = fs2.type;
            else
                mergedType = fs1.type;
            
            if (DataType.isSchemaType(mergedType)) {
                mergedSubSchema = merge(fs1.schema, fs2.schema);
                if (mergedSubSchema==null) {
                    throw new FrontendException("Error merging schema " + fs1 + " and " + fs2, 2246);
                }
            }
            LogicalFieldSchema mergedFS = new LogicalFieldSchema(mergedAlias, mergedSubSchema, mergedType);
            mergedSchema.addField(mergedFS);
            if (s1.isTwoLevelAccessRequired() && s2.isTwoLevelAccessRequired()) {
                mergedSchema.setTwoLevelAccessRequired(true);
            }
        }
        return mergedSchema;
    }
    
    public String toString() {
        StringBuilder str = new StringBuilder();
        
        for( LogicalFieldSchema field : fields ) {
            str.append( field.toString() + "," );
        }
        if( fields.size() != 0 ) {
            str.deleteCharAt( str.length() -1 );
        }
        return str.toString();
    }
    
    public void setTwoLevelAccessRequired(boolean flag) {
        twoLevelAccessRequired = flag;
    }
    
    public boolean isTwoLevelAccessRequired() {
        return twoLevelAccessRequired;
    }

    public LogicalSchema mergeUid(LogicalSchema uidOnlySchema) throws FrontendException {
        if (uidOnlySchema!=null) {
            if (size()!=uidOnlySchema.size()) {
                throw new FrontendException("Structure of schema change. Original: " + uidOnlySchema + " Now: " + this, 2239);
                }
            for (int i=0;i<size();i++) {
                getField(i).mergeUid(uidOnlySchema.getField(i));
                }
            return uidOnlySchema;
        }
        else {
            LogicalSchema clonedUidOnlyCopy = new LogicalSchema();
            for (int i=0;i<size();i++) {
                getField(i).stampFieldSchema();
                clonedUidOnlyCopy.addField(getField(i).cloneUid());                    
                }
            return clonedUidOnlyCopy;
        }
    }
    
    public LogicalSchema deepCopy()  {
        LogicalSchema newSchema = new LogicalSchema();
        newSchema.setTwoLevelAccessRequired(isTwoLevelAccessRequired());
        for (int i=0;i<size();i++)
            newSchema.addField(getField(i).deepCopy());
        return newSchema;
    }
}
