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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
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
               
        public String toString(boolean verbose) {
            String uidString = "";
            if (verbose)
                uidString="#" + uid;
            
            if( type == DataType.BAG ) {
                if( schema == null ) {
                    return ( alias + uidString + ":bag{}" );
                }
                return ( alias + uidString + ":bag{" + schema.toString(verbose) + "}" );
            } else if( type == DataType.TUPLE ) {
                if( schema == null ) {
                    return ( alias + uidString + ":tuple{}" );
                }
                return ( alias + uidString + ":tuple(" + schema.toString(verbose) + ")" );
            }
            return ( alias + uidString + ":" + DataType.findTypeName(type) );
        }
        
        public String toString() {
            return toString(true);
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
        
        /***
         * Compare two field schema for equality

         * @param relaxInner If true, we don't check inner tuple schemas
         * @param relaxAlias If true, we don't check aliases
         * @return true if FieldSchemas are equal, false otherwise
         */
        public static boolean equals(LogicalFieldSchema fschema,
                                     LogicalFieldSchema fother,
                                     boolean relaxInner,
                                     boolean relaxAlias) {
            if( fschema == null || fother == null ) {
                return false ;
            }

            if( fschema.type != fother.type ) {
                return false ;
            }


            if (!relaxAlias) {
                if ( fschema.alias == null && fother.alias == null ) {
                    // good
                } else if ( fschema.alias == null ) {
                    return false ;
                } else if( !fschema.alias.equals( fother.alias ) ) {
                    return false ;
                }
            }

            if ( (!relaxInner) && (DataType.isSchemaType(fschema.type))) {
                // Don't do the comparison if both embedded schemas are
                // null.  That will cause Schema.equals to return false,
                // even though we want to view that as true.
                if (!(fschema.schema == null && fother.schema == null)) {
                    // compare recursively using schema
                    if (!LogicalSchema.equals(fschema.schema, fother.schema, false, relaxAlias)) {
                        return false ;
                    }
                }
            }

            return true ;
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
    
    public int getFieldPosition(String alias) {
        Pair<Integer, Boolean> p = aliases.get( alias );
        if( p == null ) {
            return -1;
        }

        return p.first;
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
            }
            LogicalFieldSchema mergedFS = new LogicalFieldSchema(mergedAlias, mergedSubSchema, mergedType);
            mergedSchema.addField(mergedFS);
            if (s1.isTwoLevelAccessRequired() != s2.isTwoLevelAccessRequired()) {
                return null;
            }
            if (s1.isTwoLevelAccessRequired()) {
                mergedSchema.setTwoLevelAccessRequired(true);
            }
        }
        return mergedSchema;
    }
    
    public String toString(boolean verbose) {
        StringBuilder str = new StringBuilder();
        
        for( LogicalFieldSchema field : fields ) {
            str.append( field.toString(verbose) + "," );
        }
        if( fields.size() != 0 ) {
            str.deleteCharAt( str.length() -1 );
        }
        return str.toString();
    }
    
    public String toString() {
        return toString(true);
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

    /**
     * Merges collection of schemas using their column aliases 
     * (unlike mergeSchema(..) functions which merge using positions)
     * Schema will not be merged if types are incompatible, 
     * as per DataType.mergeType(..)
     * For Tuples and Bags, SubSchemas have to be equal be considered compatible
     * @param schemas - list of schemas to be merged using their column alias
     * @return merged schema
     */
    public static LogicalSchema mergeSchemasByAlias(List<LogicalSchema> schemas)
    throws SchemaMergeException{
        LogicalSchema mergedSchema = null;

        // list of schemas that have currently been merged, used in error message
        ArrayList<LogicalSchema> mergedSchemas = new ArrayList<LogicalSchema>(schemas.size());
        for(LogicalSchema sch : schemas){
            if(mergedSchema == null){
                mergedSchema = sch.deepCopy();
                mergedSchemas.add(sch);
                continue;
            }
            try{
                mergedSchema = mergeSchemaByAlias( mergedSchema, sch );
                mergedSchemas.add(sch);
            }catch(SchemaMergeException e){
                String msg = "Error merging schema: ("  + sch + ") with " 
                    + "merged schema: (" + mergedSchema + ")" + " of schemas : "
                    + mergedSchemas;
                throw new SchemaMergeException(msg, e);
            }
        }
        return mergedSchema;
    }
    
    /**
     * Merges two schemas using their column aliases 
     * (unlike mergeSchema(..) functions which merge using positions)
     * Schema will not be merged if types are incompatible, 
     * as per DataType.mergeType(..)
     * For Tuples and Bags, SubSchemas have to be equal be considered compatible
     */
    public static LogicalSchema mergeSchemaByAlias(LogicalSchema schema1, LogicalSchema schema2)
    throws SchemaMergeException{
        LogicalSchema mergedSchema = new LogicalSchema();
        HashSet<LogicalFieldSchema> schema2colsAdded = new HashSet<LogicalFieldSchema>();
        // add/merge fields present in first schema 
        for(LogicalFieldSchema fs1 : schema1.getFields()){
            checkNullAlias(fs1, schema1);
            LogicalFieldSchema fs2 = schema2.getField( fs1.alias );
            if(fs2 != null){
                if(schema2colsAdded.contains(fs2)){
                    // alias corresponds to multiple fields in schema1,
                    // just do a lookup on
                    // schema1 , that will throw the appropriate error.
                    schema1.getField( fs2.alias );
                }
                schema2colsAdded.add(fs2);
            }
            LogicalFieldSchema mergedFs = mergeFieldSchemaFirstLevelSameAlias(fs1,fs2);
            mergedSchema.addField( mergedFs );
        }

        //add schemas from 2nd schema, that are not already present in
        // merged schema
        for(LogicalFieldSchema fs2 : schema2.getFields()){
            checkNullAlias(fs2, schema2);
            if(! schema2colsAdded.contains(fs2)){
                mergedSchema.addField( new LogicalFieldSchema( fs2 ) );
            }
        }
        return mergedSchema;
    }

    private static void checkNullAlias(LogicalFieldSchema fs, LogicalSchema schema)
    throws SchemaMergeException {
        if(fs.alias == null){
            throw new SchemaMergeException(
                    "Schema having field with null alias cannot be merged " +
                    "using alias. Schema :" + schema
            );
        }
    }

    /**
     * Schema will not be merged if types are incompatible, 
     * as per DataType.mergeType(..)
     * For Tuples and Bags, SubSchemas have to be equal be considered compatible
     * Aliases are assumed to be same for both
     */
    private static LogicalFieldSchema mergeFieldSchemaFirstLevelSameAlias(LogicalFieldSchema fs1,
            LogicalFieldSchema fs2) 
    throws SchemaMergeException {
        if(fs1 == null)
            return fs2;
        if(fs2 == null)
            return fs1;

        LogicalSchema innerSchema = null;
        
        String alias = mergeNameSpacedAlias(fs1.alias, fs2.alias);
        
        byte mergedType = DataType.mergeType(fs1.type, fs2.type) ;

        // If the types cannot be merged
        if (mergedType == DataType.ERROR) {
                int errCode = 1031;
                String msg = "Incompatible types for merging schemas. Field schema: "
                    + fs1 + " Other field schema: " + fs2;
                throw new SchemaMergeException(msg, errCode, PigException.INPUT) ;
        }
        if(DataType.isSchemaType(mergedType)) {
            // if one of them is a bytearray, pick inner schema of other one
            if( fs1.type == DataType.BYTEARRAY ){
                innerSchema = fs2.schema;
            }else if(fs2.type == DataType.BYTEARRAY){
                innerSchema = fs1.schema;
            }
            else {
                //in case of types with inner schema such as bags and tuples
                // the inner schema has to be same
                if(!equals(fs1.schema, fs2.schema, false, false)){
                    int errCode = 1032;
                    String msg = "Incompatible types for merging inner schemas of " +
                    " Field schema type: " + fs1 + " Other field schema type: " + fs2;
                    throw new SchemaMergeException(msg, errCode, PigException.INPUT) ;                
                }
                innerSchema = fs1.schema;
            }
        }
      
        return new LogicalFieldSchema(alias, innerSchema, mergedType) ;
    }

    /**
     * If one of the aliases is of form 'nm::str1', and other is of the form
     * 'str1', this returns str1
     */
    private static String mergeNameSpacedAlias(String alias1, String alias2)
    throws SchemaMergeException {
        if(alias1.equals(alias2)){
            return alias1;
        }
        if(alias1.endsWith("::" + alias2)){
            return alias2;
        }
        if(alias2.endsWith("::" + alias1)){
            return alias1;
        }
        //the aliases are different, alias cannot be merged
        return null;
    }

    /**
     * Recursively compare two schemas for equality
     * @param schema
     * @param other
     * @param relaxInner if true, inner schemas will not be checked
     * @param relaxAlias if true, aliases will not be checked
     * @return true if schemas are equal, false otherwise
     */
    public static boolean equals(LogicalSchema schema,
                                 LogicalSchema other,
                                 boolean relaxInner,
                                 boolean relaxAlias) {
        // If both of them are null, they are equal
        if ((schema == null) && (other == null)) {
            return true ;
        }

        // otherwise
        if (schema == null || other == null ) {
            return false ;
        }

        /*
         * Need to check for bags with schemas and bags with tuples that in turn have schemas.
         * Retrieve the tuple schema of the bag if twoLevelAccessRequired
         * Assuming that only bags exhibit this behavior and twoLevelAccessRequired is used
         * with the right intentions
         */
        if(schema.isTwoLevelAccessRequired() || other.isTwoLevelAccessRequired()) {
            if(schema.isTwoLevelAccessRequired()) {
                schema = schema.getField(0).schema;
            }
            
            if(other.isTwoLevelAccessRequired()) {
                other = other.getField(0).schema;
            }
            
            return LogicalSchema.equals(schema, other, relaxInner, relaxAlias);
        }

        if (schema.size() != other.size()) return false;

        Iterator<LogicalFieldSchema> i = schema.fields.iterator();
        Iterator<LogicalFieldSchema> j = other.fields.iterator();

        while (i.hasNext()) {
            LogicalFieldSchema myFs = i.next() ;
            LogicalFieldSchema otherFs = j.next() ;

            if (!relaxAlias) {
                if( myFs.alias == null && otherFs.alias == null ) {
                    // good
                } else if( myFs.alias == null ) {
                    return false ;
                } else if( !myFs.alias.equals(otherFs.alias) ) {
                    return false ;
                }
            }

            if (myFs.type != otherFs.type) {
                return false ;
            }

            if (!relaxInner && !LogicalFieldSchema.equals( myFs, otherFs, false, relaxAlias ) ) {
                // Compare recursively using field schema
                return false ;
            }
        }
        return true;
    }
}
