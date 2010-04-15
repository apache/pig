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

import java.util.List;

/** The class represents the list of property values added for a partition in the owl metadata */
public class OwlPartitionProperty extends OwlObject {

    /** The property key values added for the partition */
    private List<OwlKeyValue> keyValues;

    /**
     * Instantiates a new owl partition property.
     */
    public OwlPartitionProperty() {
    }

    /**
     * Instantiates a new owl partition property.
     * 
     * @param keyValues
     *            the key values
     */
    public OwlPartitionProperty(List<OwlKeyValue> keyValues) {
        this.keyValues = keyValues;
    }

    /**
     * Gets the value of keyValues
     * @return the keyValues
     */
    public List<OwlKeyValue> getKeyValues() {
        return keyValues;
    }

    /**
     * Sets the value of keyValues
     * @param keyValues the keyValues to set
     */
    public void setKeyValues(List<OwlKeyValue> keyValues) {
        this.keyValues = keyValues;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
        + ((keyValues == null) ? 0 : keyValues.hashCode());
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
        OwlPartitionProperty other = (OwlPartitionProperty) obj;
        if (keyValues == null) {
            if (other.keyValues != null) {
                return false;
            }
        } else if (!keyValues.equals(other.keyValues)) {
            return false;
        }
        return true;
    }

}