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

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;

/** Base class for different key types in owl metadata */
public abstract class OwlKey extends OwlObject {

    /** Enum for different types of data types supported for Owl Keys */
    public enum DataType {
        STRING  (101),
        INT     (102), 
        LONG    (103);

        private int code;

        private DataType(int code){
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static DataType fromCode(int inputCode) throws OwlException {
            for (DataType d : DataType.values()) {
                if (d.getCode() == inputCode) {
                    return d;
                }
            }

            throw new OwlException(ErrorType.ERROR_UNKNOWN_ENUM_TYPE, "Code " + inputCode + " for " + DataType.class.getSimpleName());
        }
    }

    /** Enum for different type of Owl Keys */
    public enum KeyType {
        PARTITION  (201), // partitioning key
        PROPERTY   (202), // property key
        GLOBAL     (203); // owl global system key

        public int code;

        private KeyType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static KeyType fromCode(int inputCode) throws OwlException {
            for (KeyType k : KeyType.values()) {
                if (k.getCode() == inputCode) {
                    return k;
                }
            }

            throw new OwlException(ErrorType.ERROR_UNKNOWN_ENUM_TYPE, "Code " + inputCode + " for " + KeyType.class.getSimpleName());
        }
    }

    /** The name. */
    private String name;

    /** The type. */
    private DataType dataType;

    /**
     * Instantiates a new owl key.
     */
    public OwlKey() {
    }

    /**
     * Instantiates a new owl key.
     * @param name the name
     * @param dataType the data type
     */
    public OwlKey(String name, DataType dataType) {
        this.name = OwlUtil.toLowerCase( name );
        this.dataType = dataType;
    }

    /**
     * Gets the value of name
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the value of dataType
     * @return the dataType
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * Sets the value of name
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = OwlUtil.toLowerCase( name );
    }

    /**
     * Sets the value of dataType
     * @param dataType the dataType to set
     */
    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }


    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result
        + ((dataType == null) ? 0 : dataType.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        OwlKey other = (OwlKey) obj;
        if (dataType == null) {
            if (other.dataType != null) {
                return false;
            }
        } else if (!dataType.equals(other.dataType)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        return true;
    }


}
