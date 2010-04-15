/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.owl.protocol;

import java.util.List;

/** This class represent a OwlTable in the metadata */
public class OwlTable extends OwlResource {

    /** The name for the OwlTable */
    private OwlTableName name;

    /** The property keys for this OwlTable */
    private List<OwlPropertyKey> propertyKeys;

    /** The partitioning keys for this OwlTable */
    private List<OwlPartitionKey> partitionKeys = null;

    /** The properties added to this owlTable */
    private List<OwlKeyValue> propertyValues = null;

    /** The owlschema for this owltable */
    private OwlSchema schema;

    /**
     * Instantiates a new owl table.
     */
    public OwlTable() {
    }

    /**
     * Instantiates a new owl table.
     * @param name the name
     */
    public OwlTable(OwlTableName name) {

        super(null, 0, 0);

        this.name = name;
    } 

    /**
     * Instantiates a new owl table.
     * @param name the name of the table
     * @param propertyKeys the property keys
     * @param partitionKeys the partition keys
     * @param propertyValues the property values
     * @param schema the schema
     */
    public OwlTable( 
            OwlTableName name,
            List<OwlPropertyKey> propertyKeys,
            List<OwlPartitionKey> partitionKeys,
            List<OwlKeyValue> propertyValues,
            OwlSchema schema) {

        super(null, 0, 0);

        this.name = name;
        this.propertyKeys = propertyKeys;
        this.partitionKeys = partitionKeys;
        this.propertyValues = propertyValues;
        this.schema = schema;
    } 

    /**
     * Instantiates a new owl table.
     * @param owner the owner
     * @param creationTime the creation time
     * @param modificationTime the modification time
     * @param name the name of the table
     * @param propertyKeys the property keys
     * @param partitionKeys the partition keys
     * @param propertyValues the property values
     * @param schema the schema
     */
    public OwlTable(String owner, long creationTime, long modificationTime, 
            OwlTableName name,
            List<OwlPropertyKey> propertyKeys,
            List<OwlPartitionKey> partitionKeys,
            List<OwlKeyValue> propertyValues,
            OwlSchema schema) {

        super(owner, creationTime, modificationTime);

        this.name = name;
        this.propertyKeys = propertyKeys;
        this.partitionKeys = partitionKeys;
        this.propertyValues = propertyValues;
        this.schema = schema;
    } 

    /**
     * Gets the value of name
     * @return the name
     */
    public OwlTableName getName() {
        return name;
    }

    /**
     * Gets the value of propertyKeys
     * @return the propertyKeys
     */
    public List<OwlPropertyKey> getPropertyKeys() {
        return propertyKeys;
    }

    /**
     * Gets the value of partitionKeys
     * @return the partitionKeys
     */
    public List<OwlPartitionKey> getPartitionKeys() {
        return partitionKeys;
    }

    /**
     * Gets the value of propertyValues
     * @return the propertyValues
     */
    public List<OwlKeyValue> getPropertyValues() {
        return propertyValues;
    }

    /**
     * Gets the value of schema
     * @return the schema
     */
    public OwlSchema getSchema() {
        return this.schema;
    }


    /**
     * Sets the value of name
     * @param name the name to set
     */
    public void setName(OwlTableName name) {
        this.name = name;
    }

    /**
     * Sets the value of propertyKeys
     * @param propertyKeys the propertyKeys to set
     */
    public void setPropertyKeys(List<OwlPropertyKey> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    /**
     * Sets the value of partitionKeys
     * @param partitionKeys the partitionKeys to set
     */
    public void setPartitionKeys(List<OwlPartitionKey> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    /**
     * Sets the value of propertyValues
     * @param propertyValues the propertyValues to set
     */
    public void setPropertyValues(List<OwlKeyValue> propertyValues) {
        this.propertyValues = propertyValues;
    }

    /**
     * Sets the value of schema
     * @param schema the schema to set
     */
    public void setSchema(OwlSchema schema) {
        this.schema = schema;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result
        + ((partitionKeys == null) ? 0 : partitionKeys.hashCode());
        result = prime * result
        + ((propertyKeys == null) ? 0 : propertyKeys.hashCode());
        result = prime * result
        + ((propertyValues == null) ? 0 : propertyValues.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
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
        OwlTable other = (OwlTable) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (partitionKeys == null) {
            if (other.partitionKeys != null) {
                return false;
            }
        } else if (!partitionKeys.equals(other.partitionKeys)) {
            return false;
        }
        if (propertyKeys == null) {
            if (other.propertyKeys != null) {
                return false;
            }
        } else if (!propertyKeys.equals(other.propertyKeys)) {
            return false;
        }
        if (propertyValues == null) {
            if (other.propertyValues != null) {
                return false;
            }
        } else if (!propertyValues.equals(other.propertyValues)) {
            return false;
        }
        if (schema == null) {
            if (other.schema != null) {
                return false;
            }
        } else if (!schema.equals(other.schema)) {
            return false;
        }
        return true;
    }

}
