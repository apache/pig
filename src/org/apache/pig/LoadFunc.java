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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * This interface is used to implement functions to parse records
 * from a dataset.  This also includes functions to cast raw byte data into various
 * datatypes.  These are external functions because we want loaders, whenever
 * possible, to delay casting of datatypes until the last possible moment (i.e.
 * don't do it on load).  This means we need to expose the functionality so that
 * other sections of the code can call back to the loader to do the cast.
 */
public interface LoadFunc {
    /**
     * Specifies a portion of an InputStream to read tuples. Because the
     * starting and ending offsets may not be on record boundaries it is up to
     * the implementor to deal with figuring out the actual starting and ending
     * offsets in such a way that an arbitrarily sliced up file will be processed
     * in its entirety.
     * <p>
     * A common way of handling slices in the middle of records is to start at
     * the given offset and, if the offset is not zero, skip to the end of the
     * first record (which may be a partial record) before reading tuples.
     * Reading continues until a tuple has been read that ends at an offset past
     * the ending offset.
     * <p>
     * <b>The load function should not do any buffering on the input stream</b>. Buffering will
     * cause the offsets returned by is.getPos() to be unreliable.
     *  
     * @param fileName the name of the file to be read
     * @param is the stream representing the file to be processed, and which can also provide its position.
     * @param offset the offset to start reading tuples.
     * @param end the ending offset for reading.
     * @throws IOException
     */
    public void bindTo(String fileName,
                       BufferedPositionedInputStream is,
                       long offset,
                       long end) throws IOException;

    /**
     * Retrieves the next tuple to be processed.
     * @return the next tuple to be processed or null if there are no more tuples
     * to be processed.
     * @throws IOException
     */
    public Tuple getNext() throws IOException;
    
    
    /**
     * Cast data from bytes to integer value.  
     * @param b byte array to be cast.
     * @return Integer value.
     * @throws IOException if the value cannot be cast.
     */
    public Integer bytesToInteger(byte[] b) throws IOException;

    /**
     * Cast data from bytes to long value.  
     * @param b byte array to be cast.
     * @return Long value.
     * @throws IOException if the value cannot be cast.
     */
    public Long bytesToLong(byte[] b) throws IOException;

    /**
     * Cast data from bytes to float value.  
     * @param b byte array to be cast.
     * @return Float value.
     * @throws IOException if the value cannot be cast.
     */
    public Float bytesToFloat(byte[] b) throws IOException;

    /**
     * Cast data from bytes to double value.  
     * @param b byte array to be cast.
     * @return Double value.
     * @throws IOException if the value cannot be cast.
     */
    public Double bytesToDouble(byte[] b) throws IOException;

    /**
     * Cast data from bytes to chararray value.  
     * @param b byte array to be cast.
     * @return String value.
     * @throws IOException if the value cannot be cast.
     */
    public String bytesToCharArray(byte[] b) throws IOException;

    /**
     * Cast data from bytes to map value.  
     * @param b byte array to be cast.
     * @return Map value.
     * @throws IOException if the value cannot be cast.
     */
    public Map<String, Object> bytesToMap(byte[] b) throws IOException;

    /**
     * Cast data from bytes to tuple value.  
     * @param b byte array to be cast.
     * @return Tuple value.
     * @throws IOException if the value cannot be cast.
     */
    public Tuple bytesToTuple(byte[] b) throws IOException;

    /**
     * Cast data from bytes to bag value.  
     * @param b byte array to be cast.
     * @return Bag value.
     * @throws IOException if the value cannot be cast.
     */
    public DataBag bytesToBag(byte[] b) throws IOException;

    /**
     * Indicate to the loader fields that will be needed.  This can be useful for
     * loaders that access data that is stored in a columnar format where indicating
     * columns to be accessed a head of time will save scans.  If the loader
     * function cannot make use of this information, it is free to ignore it.
     * @param requiredFieldList RequiredFieldList indicating which columns will be needed.
     */
    public RequiredFieldResponse fieldsToRead(RequiredFieldList requiredFieldList) throws FrontendException;

    /**
     * Find the schema from the loader.  This function will be called at parse time
     * (not run time) to see if the loader can provide a schema for the data.  The
     * loader may be able to do this if the data is self describing (e.g. JSON).  If
     * the loader cannot determine the schema, it can return a null.
     * LoadFunc implementations which need to open the input "fileName", can use 
     * FileLocalizer.open(String fileName, ExecType execType, DataStorage storage) to get
     * an InputStream which they can use to initialize their loader implementation. They
     * can then use this to read the input data to discover the schema. Note: this will
     * work only when the fileName represents a file on Local File System or Hadoop file 
     * system
     * @param fileName Name of the file to be read.(this will be the same as the filename 
     * in the "load statement of the script)
     * @param execType - execution mode of the pig script - one of ExecType.LOCAL or ExecType.MAPREDUCE
     * @param storage - the DataStorage object corresponding to the execType
     * @return a Schema describing the data if possible, or null otherwise.
     * @throws IOException.
     */
    public Schema determineSchema(String fileName, ExecType execType, DataStorage storage) throws IOException;
    
    class RequiredField implements Serializable {
        // Implementation of the private fields is subject to change but the
        // getter() interface should remain
        
        private static final long serialVersionUID = 1L;
        
        // will hold name of the field (would be null if not supplied)
        private String alias; 

        // will hold the index (position) of the required field (would be -1 if not supplied), index is 0 based
        private int index; 

        // A list of sub fields in this field (this could be a list of hash keys for example). 
        // This would be null if the entire field is required and no specific sub fields are required. 
        // In the initial implementation only one level of subfields will be populated except for bags 
        // where two levels will be supported as explained in NOTE 2 below.
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

        public String toString() {
            if (index != -1)
                return "" + index;
            else if (alias != null)
                return alias;
            return "";
        }
    }

    class RequiredFieldList implements Serializable {
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

    class RequiredFieldResponse {
        // Implementation of the private fields is subject ot change but the
        // constructor interface should remain
        
        // the loader should pass true if the it will return data containing
        // only the List of RequiredFields in that order. false if the it
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