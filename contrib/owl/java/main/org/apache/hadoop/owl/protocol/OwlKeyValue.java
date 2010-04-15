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

import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.protocol.OwlKey.KeyType;

/** The class represents a key value in the owl metadata */
public class OwlKeyValue extends OwlObject implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;     

    /** The name for the key */
    private String keyName;

    /** The type of key */
    private OwlKey.KeyType keyType;

    /** The datatype of the key */
    private OwlKey.DataType dataType;

    /** The integer value */
    private Integer intValue;

    /** The string value */
    private String stringValue;

    /** The long value */
    private Long longValue;

    /**
     * Instantiates a new owl key value.
     */
    public OwlKeyValue() {
    }

    /**
     * Instantiates a new owl key value.
     * @param keyName the key name
     * @param intValue the integer value
     */
    public OwlKeyValue(String keyName, Integer intValue) {
        this.keyName = OwlUtil.toLowerCase( keyName );
        this.intValue = intValue;
    }

    /**
     * Instantiates a new owl key value.
     * @param keyName the key name
     * @param stringValue the string value
     */
    public OwlKeyValue(String keyName, String stringValue) {
        this.keyName =  OwlUtil.toLowerCase( keyName );
        this.stringValue = stringValue;
    }

    /**
     * Instantiates a new owl key value.
     * 
     * @param keyName
     *            the key name
     * @param keyType
     *            the key type
     * @param dataType
     *            the data type
     * @param intValue
     *            the integer value
     * @param stringValue
     *            the string value
     */
    public OwlKeyValue(String keyName, KeyType keyType, DataType dataType,
            Integer intValue, String stringValue) {
        this.keyName =  OwlUtil.toLowerCase( keyName );;
        this.keyType = keyType;
        this.dataType = dataType;
        this.intValue = intValue;
        this.stringValue = stringValue;
    }


    /**
     * Instantiates a new owl key value.
     * 
     * @param keyName
     *            the key name
     * @param keyType
     *            the key type
     * @param dataType
     *            the data type
     * @param intValue
     *            the integer value
     * @param stringValue
     *            the string value
     * @param longValue
     *            the long value
     */
    public OwlKeyValue(String keyName, KeyType keyType, DataType dataType,
            Integer intValue, String stringValue, Long longValue) {
        this.keyName =  OwlUtil.toLowerCase( keyName );
        this.keyType = keyType;
        this.dataType = dataType;
        this.intValue = intValue;
        this.stringValue = stringValue;
        this.longValue = longValue;
    }

    /**
     * Gets the value of keyName
     * @return the keyName
     */
    public String getKeyName() {
        return keyName;
    }

    /**
     * Gets the value of keyType
     * @return the keyType
     */
    public OwlKey.KeyType getKeyType() {
        return keyType;
    }

    /**
     * Gets the value of dataType
     * @return the dataType
     */
    public OwlKey.DataType getDataType() {
        return dataType;
    }

    /**
     * Gets the value of intValue
     * @return the intValue
     */
    public Integer getIntValue() {
        return intValue;
    }

    /**
     * Gets the value of stringValue
     * @return the stringValue
     */
    public String getStringValue() {
        return stringValue;
    }

    /**
     * Sets the value of keyName
     * @param keyName the keyName to set
     */
    public void setKeyName(String keyName) {
        this.keyName =  OwlUtil.toLowerCase( keyName );
    }

    /**
     * Sets the value of keyType
     * @param keyType the keyType to set
     */
    public void setKeyType(OwlKey.KeyType keyType) {
        this.keyType = keyType;
    }

    /**
     * Sets the value of dataType
     * @param dataType the dataType to set
     */
    public void setDataType(OwlKey.DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * Sets the value of intValue
     * @param intValue the intValue to set
     */
    public void setIntValue(Integer intValue) {
        this.intValue = intValue;
    }

    /**
     * Sets the value of stringValue
     * @param stringValue the stringValue to set
     */
    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    /**
     * Sets the long value.
     * 
     * @param longValue
     *            the new long value
     */
    public void setLongValue(Long longValue) {
        this.longValue = longValue;
    }

    /**
     * Gets the long value.
     * 
     * @return the long value
     */
    public Long getLongValue() {
        return longValue;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
        + ((dataType == null) ? 0 : dataType.hashCode());
        result = prime * result
        + ((intValue == null) ? 0 : intValue.hashCode());
        result = prime * result + ((keyName == null) ? 0 : keyName.hashCode());
        result = prime * result + ((keyType == null) ? 0 : keyType.hashCode());
        result = prime * result
        + ((longValue == null) ? 0 : longValue.hashCode());
        result = prime * result
        + ((stringValue == null) ? 0 : stringValue.hashCode());
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
        OwlKeyValue other = (OwlKeyValue) obj;
        if (dataType == null) {
            if (other.dataType != null) {
                return false;
            }
        } else if (!dataType.equals(other.dataType)) {
            return false;
        }
        if (intValue == null) {
            if (other.intValue != null) {
                return false;
            }
        } else if (!intValue.equals(other.intValue)) {
            return false;
        }
        if (keyName == null) {
            if (other.keyName != null) {
                return false;
            }
        } else if (!keyName.equals(other.keyName)) {
            return false;
        }
        if (keyType == null) {
            if (other.keyType != null) {
                return false;
            }
        } else if (!keyType.equals(other.keyType)) {
            return false;
        }
        if (longValue == null) {
            if (other.longValue != null) {
                return false;
            }
        } else if (!longValue.equals(other.longValue)) {
            return false;
        }
        if (stringValue == null) {
            if (other.stringValue != null) {
                return false;
            }
        } else if (!stringValue.equals(other.stringValue)) {
            return false;
        }
        return true;
    }

}
