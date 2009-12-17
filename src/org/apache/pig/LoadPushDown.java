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

import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * This interface defines how to communicate to Pig what functionality can
 * be pushed into the loader.  If a given loader does not implement this interface
 * it will be assumed that it is unable to accept any functionality for push down.
 */
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
     * columns to be accessed a head of time will save scans.  If the loader
     * function cannot make use of this information, it is free to ignore it.
     * @param requiredFieldList RequiredFieldList indicating which columns will be needed.
     */
    public RequiredFieldResponse pushProjection(RequiredFieldList 
            requiredFieldList) throws FrontendException;
    
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
        
        // true for atomic types like INTEGER, FLOAT, DOUBLE, CHARARRAY, BYTEARRAY, LONG and when all 
        // subfields from complex types like BAG, TUPLE and MAP are required
        private boolean allSubFieldsRequired;
        
        // Type of this field - the value could be any current PIG DataType (as specified by the constants in DataType class).
        private byte type;

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

        /**
         * @return true if all sub fields are required, false otherwise
         */
        public boolean isAllSubFieldsRequired() {
            return allSubFieldsRequired;
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

    public static class RequiredFieldList implements Serializable {
        // Implementation of the private fields is subject to change but the
        // getter() interface should remain
        
        private static final long serialVersionUID = 1L;
        
        // list of Required fields, this will be null if all fields are required
        private List<RequiredField> fields = new ArrayList<RequiredField>(); 
        
        // flag to indicate if all fields are required. The Loader implementation should check this flag first and look at the fields ONLY if this is true
        private boolean allFieldsRequired;
        
        private String signature;

        /**
         * @return the required fields - this will be null if all fields are
         *         required
         */
        public List<RequiredField> getFields() {
            return fields;
        }

        public RequiredFieldList(String signature) {
            this.signature = signature;
        }
        
        public String getSignature() {
            return signature;
        }
        
        /**
         * @return true if all fields are required, false otherwise
         */
        public boolean isAllFieldsRequired() {
            return allFieldsRequired;
        }

        public void setAllFieldsRequired(boolean allRequired) {
            allFieldsRequired = allRequired;
        }

        @Override
        public String toString() {
            StringBuffer result = new StringBuffer();
            if (allFieldsRequired)
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
        
        public void add(RequiredField rf)
        {
            fields.add(rf);
        }
    }

    public static class RequiredFieldResponse {
        // the loader should pass true if it will return data containing
        // only the List of RequiredFields in that order. false if it
        // will return all fields in the data
        private boolean requiredFieldRequestHonored;

        public RequiredFieldResponse(boolean requiredFieldRequestHonored) {
            this.requiredFieldRequestHonored = requiredFieldRequestHonored;
        }

        // true if the loader will return data containing only the List of
        // RequiredFields in that order. false if the loader will return all
        // fields in the data
        public boolean getRequiredFieldResponse() {
            return requiredFieldRequestHonored;
        }

        // the loader should pass true if the it will return data containing
        // only the List of RequiredFields in that order. false if the it
        // will return all fields in the data
        public void setRequiredFieldResponse(boolean honored) {
            requiredFieldRequestHonored = honored;
        }
    }

    
}