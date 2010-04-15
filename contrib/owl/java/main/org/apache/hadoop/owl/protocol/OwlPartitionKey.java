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

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlKey;

/** Partition Key defined in Owl metadata */
public class OwlPartitionKey extends OwlKey {

    public enum PartitioningType {
        LIST     (301),
        INTERVAL (302);

        private int code;

        private PartitioningType(int code){
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static PartitioningType fromCode(int inputCode) throws OwlException {
            for (PartitioningType p : PartitioningType.values()) {
                if (p.getCode() == inputCode) {
                    return p;
                }
            }

            throw new OwlException(ErrorType.ERROR_UNKNOWN_ENUM_TYPE, "Code " + inputCode + " for " + PartitioningType.class.getSimpleName());
        }
    }

    public enum IntervalFrequencyUnit {
        SECONDS     (401),
        MINUTES     (402),
        HOURS       (403);

        private int code;

        private IntervalFrequencyUnit(int code){
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static IntervalFrequencyUnit fromCode(int inputCode) throws OwlException {
            for (IntervalFrequencyUnit p : IntervalFrequencyUnit.values()) {
                if (p.getCode() == inputCode) {
                    return p;
                }
            }

            throw new OwlException(ErrorType.ERROR_UNKNOWN_ENUM_TYPE, "Code " + inputCode + " for " + IntervalFrequencyUnit.class.getSimpleName());
        }
    }

    /** The partition level of this partition key within the OwlTable. First partition key has level one */
    private int level;

    /** The partitioning type. */
    private PartitioningType partitioningType;

    /** The begin time if interval partitioning, convertible to a java.util.Data */
    private Long intervalStart;

    /** The frequency if interval partitioning */
    private Integer intervalFrequency;

    /** The unit for the frequency if interval partitioning */
    private IntervalFrequencyUnit intervalFrequencyUnit;

    /** Valid values for the list */
    private List<OwlKeyListValue> listValues;

    /**
     * Instantiates a new owl partition key.
     */
    public OwlPartitionKey()  {
    }

    /**
     * Instantiates a new owl partition key.
     * 
     * @param keyName
     *            the key name
     * @param keyType
     *            the key type
     * @param level
     *            the key level
     * @param partitioningType
     *            the partitioning type
     * @param listValues
     *            the list values
     */
    public OwlPartitionKey(String keyName, DataType keyType, int level, 
            PartitioningType partitioningType,
            List<OwlKeyListValue> listValues) {
        super(keyName, keyType); 
        this.level = level;
        this.partitioningType = partitioningType;
        this.listValues = listValues;
    }


    /**
     * Instantiates a new owl partition key.
     * 
     * @param keyName
     *            the key name
     * @param keyType
     *            the key type
     * @param level
     *            the key level
     * @param partitioningType
     *            the partitioning type
     * @param intervalStart
     *            the interval begin date
     * @param intervalFrequency
     *            the interval frequency
     * @param intervalFrequencyUnit
     *            the interval frequency unit
     * @param listValues
     *            the list values
     */
    public OwlPartitionKey(String keyName, DataType keyType, int level, 
            PartitioningType partitioningType,
            Long intervalStart, Integer intervalFrequency,
            IntervalFrequencyUnit intervalFrequencyUnit,
            List<OwlKeyListValue> listValues) {
        super(keyName, keyType); 
        this.level = level;
        this.partitioningType = partitioningType;
        this.intervalStart = intervalStart;
        this.intervalFrequency = intervalFrequency;
        this.intervalFrequencyUnit = intervalFrequencyUnit;
        this.listValues = listValues;
    }


    /**
     * Gets the value of partitioningType
     * @return the partitioningType
     */
    public PartitioningType getPartitioningType() {
        return partitioningType;
    }

    /**
     * Gets the value of intervalStart
     * @return the intervalStart
     */
    public Long getIntervalStart() {
        return intervalStart;
    }

    /**
     * Gets the value of intervalFrequency
     * @return the intervalFrequency
     */
    public Integer getIntervalFrequency() {
        return intervalFrequency;
    }

    /**
     * Gets the value of intervalFrequencyUnit
     * @return the intervalFrequencyUnit
     */
    public IntervalFrequencyUnit getIntervalFrequencyUnit() {
        return intervalFrequencyUnit;
    }

    /**
     * Gets the value of listValues
     * @return the listValues
     */
    public List<OwlKeyListValue> getListValues() {
        return listValues;
    }

    /**
     * Sets the value of partitioningType
     * @param partitioningType the partitioningType to set
     */
    public void setPartitioningType(PartitioningType partitioningType) {
        this.partitioningType = partitioningType;
    }

    /**
     * Sets the value of intervalStart
     * @param intervalStart the intervalStart to set
     */
    public void setIntervalStart(Long intervalStart) {
        this.intervalStart = intervalStart;
    }

    /**
     * Sets the value of intervalFrequency
     * @param intervalFrequency the intervalFrequency to set
     */
    public void setIntervalFrequency(Integer intervalFrequency) {
        this.intervalFrequency = intervalFrequency;
    }

    /**
     * Sets the value of intervalFrequencyUnit
     * @param intervalFrequencyUnit the intervalFrequencyUnit to set
     */
    public void setIntervalFrequencyUnit(IntervalFrequencyUnit intervalFrequencyUnit) {
        this.intervalFrequencyUnit = intervalFrequencyUnit;
    }

    /**
     * Sets the value of listValues
     * @param listValues the listValues to set
     */
    public void setListValues(List<OwlKeyListValue> listValues) {
        this.listValues = listValues;
    }

    /**
     * Gets the value of level
     * @return the level
     */
    public int getLevel() {
        return level;
    }

    /**
     * Sets the value of level
     * @param level the level to set
     */
    public void setLevel(int level) {
        this.level = level;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime
        * result
        + ((intervalFrequency == null) ? 0 : intervalFrequency
                .hashCode());
        result = prime
        * result
        + ((intervalFrequencyUnit == null) ? 0 : intervalFrequencyUnit
                .hashCode());
        result = prime * result
        + ((intervalStart == null) ? 0 : intervalStart.hashCode());
        result = prime * result + level;
        result = prime * result
        + ((listValues == null) ? 0 : listValues.hashCode());
        result = prime
        * result
        + ((partitioningType == null) ? 0 : partitioningType.hashCode());
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
        OwlPartitionKey other = (OwlPartitionKey) obj;
        if (intervalFrequency == null) {
            if (other.intervalFrequency != null) {
                return false;
            }
        } else if (!intervalFrequency.equals(other.intervalFrequency)) {
            return false;
        }
        if (intervalFrequencyUnit == null) {
            if (other.intervalFrequencyUnit != null) {
                return false;
            }
        } else if (!intervalFrequencyUnit.equals(other.intervalFrequencyUnit)) {
            return false;
        }
        if (intervalStart == null) {
            if (other.intervalStart != null) {
                return false;
            }
        } else if (!intervalStart.equals(other.intervalStart)) {
            return false;
        }
        if (level != other.level) {
            return false;
        }
        if (listValues == null) {
            if (other.listValues != null) {
                return false;
            }
        } else if (!listValues.equals(other.listValues)) {
            return false;
        }
        if (partitioningType == null) {
            if (other.partitioningType != null) {
                return false;
            }
        } else if (!partitioningType.equals(other.partitioningType)) {
            return false;
        }
        return true;
    }


}
