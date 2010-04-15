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

/** Valid values allowed for a Key */
public class OwlKeyListValue extends OwlObject {

    /** Integer value */
    private Integer intValue;

    /** String value */
    private String stringValue;

    /**
     * Instantiates a new owl key list value.
     */
    public OwlKeyListValue() {
    }

    /**
     * Instantiates a new owl key list value.
     * 
     * @param intValue
     *            the integer value
     * @param stringValue
     *            the string value
     */
    public OwlKeyListValue(Integer intValue, String stringValue) {
        this.intValue = intValue;
        this.stringValue = stringValue;
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

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
        + ((intValue == null) ? 0 : intValue.hashCode());
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
        OwlKeyListValue other = (OwlKeyListValue) obj;
        if (intValue == null) {
            if (other.intValue != null) {
                return false;
            }
        } else if (!intValue.equals(other.intValue)) {
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
