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
 * This class represents a data element in the metadata.
 */
public class OwlDataElement extends OwlResource {

    /** The storage location for the data element */
    private String storageLocation;

    /** The key values defined for this data element */
    private List<OwlKeyValue> keyValues;

    /** The loader information of this data element */
    private OwlLoaderInfo loader;

    /** The OwlSchemaId of this data element */
    private OwlSchema owlSchema;

    /**
     * Instantiates a new owl data element.
     */
    public OwlDataElement() {
    }

    /**
     * Instantiates a new data element.
     * 
     * @param owner
     *            the owner
     * @param creationTime
     *            the creation time
     * @param modificationTime
     *            the modification time
     * @param storageLocation
     *            the storageLocation
     * @param keyValues
     *            the key values
     * @param loader
     *            the loader information
     */
    public OwlDataElement(String owner, long creationTime, long modificationTime,
            String storageLocation, List<OwlKeyValue> keyValues,
            OwlLoaderInfo loader, OwlSchema owlSchema) {
        super(owner, creationTime, modificationTime);
        this.storageLocation = storageLocation;
        this.keyValues = keyValues;
        this.loader = loader;
        this.owlSchema = owlSchema;
    }

    /**
     * Gets the value of storageLocation
     * @return the storageLocation
     */
    public String getStorageLocation() {
        return storageLocation;
    }

    /**
     * Gets the key values.
     * @return the key values
     */
    public List<OwlKeyValue> getKeyValues() {
        return keyValues;
    }

    /**
     * Gets the loader.
     * @return the loader information
     */
    public OwlLoaderInfo getLoader() {
        return loader;
    }

    /**
     * Gets the owl schema.
     * 
     * @return the owl schema
     */
    public OwlSchema getOwlSchema() {
        return owlSchema;
    }

    /**
     * Sets the value of storageLocation
     * @param storageLocation the storageLocation to set
     */
    public void setStorageLocation(String storageLocation) {
        this.storageLocation = storageLocation;
    }

    /**
     * Sets the key values.
     * @param keyValues the new key values
     */
    public void setKeyValues(List<OwlKeyValue> keyValues) {
        this.keyValues = keyValues;
    }

    /**
     * Sets the loader value
     * @param loader the new loader value
     */
    public void setLoader(OwlLoaderInfo loader){
        this.loader = loader;
    }

    /**
     * Sets the owl schema.
     * 
     * @param owlSchema the new owl schema
     */
    public void setOwlSchema(OwlSchema owlSchema) {
        this.owlSchema = owlSchema;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result
        + ((keyValues == null) ? 0 : keyValues.hashCode());
        result = prime * result + ((loader == null) ? 0 : loader.hashCode());
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
        OwlDataElement other = (OwlDataElement) obj;
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