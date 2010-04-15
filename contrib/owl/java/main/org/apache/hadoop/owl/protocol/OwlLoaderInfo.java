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
package org.apache.hadoop.owl.protocol;

import java.io.Serializable;

/**
 * Class which stores the loader information required for reading data store in Owl metadata. 
 */
public class OwlLoaderInfo extends OwlObject implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;

    private static final String OWLLOADER_CSV_FORMAT_REGEX = ".*,.*";
    private static final char OWLLOADER_CSV_SEPARATOR_CHAR = ',';

    /** The class name of the OwlInputStorageDriver. */
    private String inputDriverClass;

    private String inputDriverArgs = null;

    /**
     * Instantiates a new owl loader info object.
     */
    public OwlLoaderInfo() {
    }

    /**
     * Copy Constructor for OwlLoaderInfo
     * @param o
     */
    public OwlLoaderInfo(OwlLoaderInfo o){
        this(o.getInputDriverClass(),o.getInputDriverArgs());
    }

    /**
     * Instantiates a new owl loader info object.
     * @param infoString the string representation of the loader info
     */
    public OwlLoaderInfo(String infoString){
        //Currently only storage driver class name, the input would have to be parsed as more info is added 
        if (infoString != null){
            if (! infoString.matches(OWLLOADER_CSV_FORMAT_REGEX)){
                this.setInputDriverClass(infoString);
            } else {
                int commaPosn = infoString.indexOf(OWLLOADER_CSV_SEPARATOR_CHAR);
                this.setInputDriverClass(infoString.substring(0, commaPosn));
                this.setInputDriverArgs(infoString.substring(commaPosn+1,infoString.length()));
            }
        }
    }

    /**
     * Instantiates a new owl loader info object
     * @param inputDriverClass the inputDriver class name
     * @param inputDriverArgs Arguments to pass on to the inputDriver
     */
    public OwlLoaderInfo(String inputDriverClass, String inputDriverArgs) {
        this.setInputDriverClass(inputDriverClass);
        this.setInputDriverArgs(inputDriverArgs);
    }

    /**
     * Gets the value of OwlInputStorageDriver class name.
     * @return the class name
     */
    public String getInputDriverClass() {
        return inputDriverClass;
    }

    /**
     * Sets the value of OwlInputStorageDriver class name.
     * @param inputDriverClass the class name
     */
    public void setInputDriverClass(String inputDriverClass) {
        this.inputDriverClass = inputDriverClass;
    }

    /**
     * Get the aditional arguments specified
     * @return additional arguments to send down to OwlInputStorageDriver
     */
    public String getInputDriverArgs() {
        return inputDriverArgs;
    }


    /**
     * Set additional arguments to send down to OwlInputStorageDriver
     * @param inputDriverArgs additional arguments to send down to OwlInputStorageDriver
     */
    public void setInputDriverArgs(String inputDriverArgs) {
        this.inputDriverArgs = inputDriverArgs;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        if (this.inputDriverArgs == null){
            return inputDriverClass;
        }else{
            StringBuilder reprBuilder = new StringBuilder();
            reprBuilder.append(this.inputDriverClass);
            reprBuilder.append(",");
            reprBuilder.append(inputDriverArgs);
            return reprBuilder.toString();
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + ((inputDriverClass == null) ? 0 : inputDriverClass.hashCode());
        result = (prime * result)  + ((inputDriverArgs == null) ? 0 : inputDriverArgs.hashCode());

        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        OwlLoaderInfo other = (OwlLoaderInfo) obj;

        if (inputDriverClass == null) {
            if (other.getInputDriverClass() != null) {
                return false;
            }
        } else if (!inputDriverClass.equals(other.getInputDriverClass())) {
            return false;
        }

        if (inputDriverArgs == null) {
            if (other.getInputDriverArgs() != null) {
                return false;
            }
        } else if (!inputDriverArgs.equals(other.getInputDriverArgs())) {
            return false;
        }

        return true;
    }
}