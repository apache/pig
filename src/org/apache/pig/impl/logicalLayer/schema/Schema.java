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

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof FieldSchema)) return false;
            FieldSchema fs = (FieldSchema)other;
            // Fields can have different names and still be equal.  But
            // types and schemas (if they're a tuple) must match.
            if (type != fs.type) return false;
            if (schema != fs.schema) return false;

            return true;
        }
        
        /***
         * Compare two field schema for equality
         * @param fschema
         * @param fother
         * @param relaxInner If true, we don't check inner tuple schemas
         * @param relaxAlias If true, we don't check aliases
         * @return
         */
        public static boolean equals(FieldSchema fschema, 
                                     FieldSchema fother, 
                                     boolean relaxInner,
                                     boolean relaxAlias) {
            if (fschema == null) {              
                return false ;
            }
            
            if (fother == null) {
                return false ;
            }
            
            if (fschema.type != fother.type) {
                return false ;
            }
            
            if ( (!relaxAlias) && (fschema.alias != fother.alias) ) {
                return false ;
            }
            
            if ( (!relaxInner) && (fschema.type == DataType.TUPLE) ) {
               // compare recursively using schema
               if (!Schema.equals(fschema.schema, fother.schema, false, relaxAlias)) {
                   return false ;
               }
            }
            
            return true ;
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

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Schema)) return false;

        Schema s = (Schema)other;

        if (s.size() != size()) return false;

        Iterator<FieldSchema> i = mFields.iterator();
        Iterator<FieldSchema> j = s.mFields.iterator();
        while (i.hasNext()) {
            if (!(i.next().equals(j.next()))) return false;
        }
        return true;
    }
    
    /**
     * Recursively compare two schemas for equality
     * @param schema
     * @param other
     * @param relaxInner if true, inner schemas will not be checked
     * @return
     */
    public static boolean equals(Schema schema, 
                                 Schema other, 
                                 boolean relaxInner,
                                 boolean relaxAlias) {
        if (schema == null) {
            return false ;
        }
        
        if (other == null) {
            return false ;
        }
        
        if (schema.size() != other.size()) return false;

        Iterator<FieldSchema> i = schema.mFields.iterator();
        Iterator<FieldSchema> j = other.mFields.iterator();
        
        while (i.hasNext()) {
            
            FieldSchema myFs = i.next() ;
            FieldSchema otherFs = j.next() ;
            
            if ( (!relaxAlias) && (myFs.alias != otherFs.alias) ) {
                return false ;
            }
            
            if (myFs.type != otherFs.type) {
                return false ;
            }
            
            if (!relaxInner) {
                // Compare recursively using field schema
                if (!FieldSchema.equals(myFs, otherFs, false, relaxAlias)) {
                    return false ;
                }            
            }
            
        }
        return true;
    }
    
    
    /***
     * Merge this schema with the other schema
     * @param other the other schema to be merged with
     * @param otherTakesAliasPrecedence true if aliases from the other
     *                                  schema take precedence
     * @return the merged schema, null if they are not compatible
     */
    public Schema merge(Schema other, boolean otherTakesAliasPrecedence) {
        return mergeSchema(this, other, otherTakesAliasPrecedence) ;
    }
    
    /***
     * Recursively merge two schemas 
     * @param schema the initial schema
     * @param other the other schema to be merged with
     * @param otherTakesAliasPrecedence true if aliases from the other
     *                                  schema take precedence
     * @return the merged schema, null if they are not compatible
     */
    private Schema mergeSchema(Schema schema, Schema other, 
                               boolean otherTakesAliasPrecedence) {
        
        if (other == null) {
            return null ;
        }
        
        if (schema.size() != other.size()) {
            return null ;
        }
        
        List<FieldSchema> outputList = new ArrayList<FieldSchema>() ;
        
        Iterator<FieldSchema> mylist = schema.mFields.iterator() ;
        Iterator<FieldSchema> otherlist = other.mFields.iterator() ;
        
        while (mylist.hasNext()) {
            
            FieldSchema myFs = mylist.next() ;
            FieldSchema otherFs = otherlist.next() ;
            
            byte mergedType = mergeType(myFs.type, otherFs.type) ;
            // if the types cannot be merged, the schemas cannot be merged
            if (mergedType == DataType.ERROR) {
                return null ;
            }
            
            String mergedAlias = mergeAlias(myFs.alias, 
                                            otherFs.alias, 
                                            otherTakesAliasPrecedence) ;
            
            FieldSchema mergedFs = null ;
            if (mergedType != DataType.TUPLE) {
                // just normal merge              
                mergedFs = new FieldSchema(mergedAlias, mergedType) ;
            }
            else {
                // merge inner tuple because both sides are tuples
                Schema mergedSubSchema = mergeSchema(myFs.schema, 
                                                     otherFs.schema,
                                                     otherTakesAliasPrecedence) ;
                // return null if they cannot be merged
                if (mergedSubSchema == null) {
                    return null ;
                }
                
                mergedFs = new FieldSchema(mergedAlias, mergedSubSchema) ;
                
            }
            outputList.add(mergedFs) ;
        }
        
        return new Schema(outputList) ;
    }
    
    /***
     * Merge two aliases. If one of aliases is null, return the other.
     * Otherwise check the precedence condition
     * @param alias
     * @param other
     * @param otherTakesPrecedence
     * @return
     */
    private String mergeAlias(String alias, String other
                              ,boolean otherTakesPrecedence) {
        if (alias == null) {
            return other ;
        }
        else if (other == null) {
            return alias ;
        }
        else if (otherTakesPrecedence) {
            return other ;
        }
        else {
            return alias ;
        }
    }
    
    /***
     * Merge types if possible
     * @param type1
     * @param type2
     * @return the merged type, or DataType.ERROR if not successful
     */
    private byte mergeType(byte type1, byte type2) {
        // Only legal types can be merged
        if ( (!DataType.isUsableType(type1)) ||
             (!DataType.isUsableType(type2)) ) {
            return DataType.ERROR ;
        }
        
        // Same type is OK
        if (type1==type2) {
            return type1 ;
        }
        
        // Both are number so we return the bigger type
        if ( (DataType.isNumberType(type1)) &&
             (DataType.isNumberType(type2)) ) {
            return type1>type2 ? type1:type2 ;
        }
        
        // One is bytearray and the other is (number or chararray)
        if ( (type1 == DataType.BYTEARRAY) &&
                ( (type2 == DataType.CHARARRAY) || (DataType.isNumberType(type2)) )
              ) {
            return type2 ;
        }
        
        if ( (type2 == DataType.BYTEARRAY) &&
                ( (type1 == DataType.CHARARRAY) || (DataType.isNumberType(type1)) )
              ) {
            return type1 ;
        }
        
        // else return just ERROR
        return DataType.ERROR ;
    }
    
    
}



