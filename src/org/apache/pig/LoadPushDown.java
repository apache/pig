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
package org.apache.pig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * This interface defines how to communicate to Pig what functionality can
 * be pushed into the loader.  If a given loader does not implement this interface
 * it will be assumed that it is unable to accept any functionality for push down.
 * @since Pig 0.7
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface LoadPushDown {

    /**
     * Set of possible operations that Pig can push down to a loader. 
     */
    enum OperatorSet {PROJECTION};

    /**
     * Determine the operators that can be pushed to the loader.  
     * Note that by indicating a loader can accept a certain operator
     * (such as selection) the loader is not promising that it can handle
     * all selections.  When it is passed the actual operators to 
     * push down it will still have a chance to reject them.
     * @return list of all features that the loader can support
     */
    List<OperatorSet> getFeatures();

    /**
     * Indicate to the loader fields that will be needed.  This can be useful for
     * loaders that access data that is stored in a columnar format where indicating
     * columns to be accessed a head of time will save scans.  This method will
     * not be invoked by the Pig runtime if all fields are required. So implementations
     * should assume that if this method is not invoked, then all fields from 
     * the input are required. If the loader function cannot make use of this 
     * information, it is free to ignore it by returning an appropriate Response
     * @param requiredFieldList RequiredFieldList indicating which columns will be needed.
     * This structure is read only. User cannot make change to it inside pushProjection.
     * @return Indicates which fields will be returned
     * @throws FrontendException
     */
    public RequiredFieldResponse pushProjection(RequiredFieldList 
            requiredFieldList) throws FrontendException;
    
    /**
     * Describes a field that is required to execute a scripts.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Evolving
    public static class RequiredField implements Serializable {
        
        private static final long serialVersionUID = 1L;
        
        // will hold name of the field (would be null if not supplied)
        private String alias; 

        // will hold the index (position) of the required field (would be -1 if not supplied), index is 0 based
        private int index; 

        // A list of sub fields in this field (this could be a list of hash keys for example). 
        // This would be null if the entire field is required and no specific sub fields are required. 
        // In the initial implementation only one level of subfields will be populated.
        private List<RequiredField> subFields;
        
        // Type of this field - the value could be any current PIG DataType (as specified by the constants in DataType class).
        private byte type;

        public RequiredField() {
            // to allow piece-meal construction
        }
        
        /**
         * @param alias
         * @param index
         * @param subFields
         * @param type
         */
        public RequiredField(String alias, int index,
                List<RequiredField> subFields, byte type) {
            this.alias = alias;
            this.index = index;
            this.subFields = subFields;
            this.type = type;
        }

        /**
         * @return the alias
         */
        public String getAlias() {
            return alias;
        }

        /**
         * @return the index
         */
        public int getIndex() {
            return index;
        }

        
        /**
         * @return the required sub fields. The return value is null if all
         *         subfields are required
         */
        public List<RequiredField> getSubFields() {
            return subFields;
        }
        
        public void setSubFields(List<RequiredField> subFields)
        {
            this.subFields = subFields;
        }

        /**
         * @return the type
         */
        public byte getType() {
            return type;
        }

        public void setType(byte t) {
            type = t;
        }
        
        public void setIndex(int i) {
            index = i;
        }
        
        public void setAlias(String alias)
        {
            this.alias = alias;
        }

        @Override
        public String toString() {
            if (index != -1)
                return "" + index;
            else if (alias != null)
                return alias;
            return "";
        }
    }

    /**
     * List of fields that Pig knows to be required to executed a script.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Evolving
    public static class RequiredFieldList implements Serializable {
        
        private static final long serialVersionUID = 1L;
        
        // list of Required fields, this will be null if all fields are required
        private List<RequiredField> fields = new ArrayList<RequiredField>(); 
        
        /**
         * Set the list of required fields.
         * @param fields
         */
        public RequiredFieldList(List<RequiredField> fields) {
            this.fields = fields;
        }

        /**
         * Geta ll required fields as a list.
         * @return the required fields - this will be null if all fields are
         *         required
         */
        public List<RequiredField> getFields() {
            return fields;
        }

        public RequiredFieldList() {
        }
        
        @Override
        public String toString() {
            StringBuffer result = new StringBuffer();
            if (fields == null)
                result.append("*");
            else {
                result.append("[");
                for (int i = 0; i < fields.size(); i++) {
                    result.append(fields.get(i));
                    if (i != fields.size() - 1)
                        result.append(",");
                }
                result.append("]");
            }
            return result.toString();
        }
        
        /**
         * Add a field to the list of required fields.
         * @param rf required field to add to the list.
         */
        public void add(RequiredField rf)
        {
            fields.add(rf);
        }
    }

    /**
     * Indicates whether the loader will return the requested fields or all fields.
     */
    @InterfaceAudience.Public
    @InterfaceStability.Evolving
    public static class RequiredFieldResponse {
        // the loader should pass true if it will return data containing
        // only the List of RequiredFields in that order. false if it
        // will return all fields in the data
        private boolean requiredFieldRequestHonored;

        public RequiredFieldResponse(boolean requiredFieldRequestHonored) {
            this.requiredFieldRequestHonored = requiredFieldRequestHonored;
        }

        /**
         * Indicates whether the loader will return only the requested fields or all fields.
         * @return true if only requested fields will be returned, false if all fields will be
         * returned.
         */
        public boolean getRequiredFieldResponse() {
            return requiredFieldRequestHonored;
        }

        /**
         * Set whether the loader will return only the requesetd fields or all fields.
         * @param honored if true only requested fields will be returned, else all fields will be
         * returned.
         */
        public void setRequiredFieldResponse(boolean honored) {
            requiredFieldRequestHonored = honored;
        }
    }

    
}
