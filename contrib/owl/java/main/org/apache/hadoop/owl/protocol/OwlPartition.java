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

/**
 * This class represents a Partition in the metadata.
 */
public class OwlPartition extends OwlResource {

    /** Is this a leaf level partition. The storageLocation, LoadInfo and Schema are valid only for leaf level partitions */
    private boolean isLeaf;

    /** The partition level of this partition within the OwlTable. First partition has level one */
    private int partitionLevel;

    /** The storage location for the partition, set only for leaf level partitions */
    private String storageLocation;

    /** The key values defined for this partition */
    private List<OwlKeyValue> keyValues;

    /** The loader information for this partition, set only for leaf level partitions */
    private OwlLoaderInfo loader;

    /** The schema for this partition, set only for leaf level partitions */
    private OwlSchema schema;

    /**
     * Instantiates a new owl data element.
     */
    public OwlPartition() {
    }

    /**
     * Instantiates a new owl partition.
     * @param partitionLevel the partition level
     * @param keyValues the key values
     */
    public OwlPartition(int partitionLevel, List<OwlKeyValue> keyValues) {
        this.partitionLevel = partitionLevel;
        this.keyValues = keyValues;
    }

    /**
     * Instantiates a new owl partition.
     * @param isLeaf is this a leaf level partition
     * @param partitionLevel the partition level
     * @param storageLocation the storage location
     * @param keyValues the key values
     * @param loader the loader
     * @param schema the schema
     */
    public OwlPartition(boolean isLeaf, int partitionLevel,
            String storageLocation, List<OwlKeyValue> keyValues,
            OwlLoaderInfo loader, OwlSchema schema) {
        this.isLeaf = isLeaf;
        this.partitionLevel = partitionLevel;
        this.storageLocation = storageLocation;
        this.keyValues = keyValues;
        this.loader = loader;
        this.schema = schema;
    }

    /**
     * Instantiates a new owl partition.
     * @param owner the owner
     * @param creationTime the creation time
     * @param modificationTime the modification time
     * @param isLeaf is this a leaf level partition
     * @param partitionLevel the partition level
     * @param storageLocation the storage location
     * @param keyValues the key values
     * @param loader the loader
     * @param schema the schema
     */
    public OwlPartition(String owner, long creationTime, long modificationTime,
            boolean isLeaf, int partitionLevel, String storageLocation,
            List<OwlKeyValue> keyValues, OwlLoaderInfo loader, OwlSchema schema) {
        super(owner, creationTime, modificationTime);
        this.isLeaf = isLeaf;
        this.partitionLevel = partitionLevel;
        this.storageLocation = storageLocation;
        this.keyValues = keyValues;
        this.loader = loader;
        this.schema = schema;
    }

    /**
     * Gets the value of isLeaf
     * @return true if this is a leaf level partition
     */
    public boolean isLeaf() {
        return isLeaf;
    }

    /**
     * Gets the value of partitionLevel
     * @return the partitionLevel
     */
    public int getPartitionLevel() {
        return partitionLevel;
    }

    /**
     * Gets the value of storageLocation
     * @return the storageLocation
     */
    public String getStorageLocation() {
        return storageLocation;
    }

    /**
     * Gets the value of keyValues
     * @return the keyValues
     */
    public List<OwlKeyValue> getKeyValues() {
        return keyValues;
    }

    /**
     * Gets the value of loader
     * @return the loader
     */
    public OwlLoaderInfo getLoader() {
        return loader;
    }

    /**
     * Gets the value of schema
     * @return the schema
     */
    public OwlSchema getSchema() {
        return schema;
    }

    /**
     * Sets the value of isLeaf
     * @param isLeaf the isLeaf value to set
     */
    public void setLeaf(boolean isLeaf) {
        this.isLeaf = isLeaf;
    }

    /**
     * Sets the value of partitionLevel
     * @param partitionLevel the partitionLevel to set
     */
    public void setPartitionLevel(int partitionLevel) {
        this.partitionLevel = partitionLevel;
    }

    /**
     * Sets the value of storageLocation
     * @param storageLocation the storageLocation to set
     */
    public void setStorageLocation(String storageLocation) {
        this.storageLocation = storageLocation;
    }

    /**
     * Sets the value of keyValues
     * @param keyValues the keyValues to set
     */
    public void setKeyValues(List<OwlKeyValue> keyValues) {
        this.keyValues = keyValues;
    }

    /**
     * Sets the value of loader
     * @param loader the loader to set
     */
    public void setLoader(OwlLoaderInfo loader) {
        this.loader = loader;
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
        result = prime * result + (isLeaf ? 1231 : 1237);
        result = prime * result
        + ((keyValues == null) ? 0 : keyValues.hashCode());
        result = prime * result + ((loader == null) ? 0 : loader.hashCode());
        result = prime * result + partitionLevel;
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        result = prime * result
        + ((storageLocation == null) ? 0 : storageLocation.hashCode());
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
        OwlPartition other = (OwlPartition) obj;
        if (isLeaf != other.isLeaf) {
            return false;
        }
        if (keyValues == null) {
            if (other.keyValues != null) {
                return false;
            }
        } else if (!keyValues.equals(other.keyValues)) {
            return false;
        }
        if (loader == null) {
            if (other.loader != null) {
                return false;
            }
        } else if (!loader.equals(other.loader)) {
            return false;
        }
        if (partitionLevel != other.partitionLevel) {
            return false;
        }
        if (schema == null) {
            if (other.schema != null) {
                return false;
            }
        } else if (!schema.equals(other.schema)) {
            return false;
        }
        if (storageLocation == null) {
            if (other.storageLocation != null) {
                return false;
            }
        } else if (!storageLocation.equals(other.storageLocation)) {
            return false;
        }
        return true;
    }

}