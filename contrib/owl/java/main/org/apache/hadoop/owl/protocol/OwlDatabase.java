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

import org.apache.hadoop.owl.common.OwlUtil;

/** The class represent a Database in the Owl metadata */
public class OwlDatabase extends OwlResource {

    /** The name for this database */
    private String name;

    /** The storage location for the data */
    private String storageLocation;

    /**
     * Instantiates a new owl database.
     */
    public OwlDatabase() {
    }

    /**
     * Instantiates a new owl database.
     * @param name the name
     * @param storageLocation the storage location
     */
    public OwlDatabase(String name, String storageLocation) {
        super(null, 0, 0);
        this.name = OwlUtil.toLowerCase( name );
        this.storageLocation = storageLocation;
    }

    /**
     * Instantiates a new owl database.
     * 
     * @param owner
     *            the owner
     * @param creationTime
     *            the creation time
     * @param modificationTime
     *            the modification time
     * @param name
     *            the name
     * @param storageLocation
     *            the storage location
     */
    public OwlDatabase(String owner, long creationTime, long modificationTime, String name, String storageLocation) {
        super(owner, creationTime, modificationTime);
        this.name = OwlUtil.toLowerCase( name );
        this.storageLocation = storageLocation;
    }

    /**
     * Gets the value of name
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the value of storageLocation
     * @return the storageLocation
     */
    public String getStorageLocation() {
        return storageLocation;
    }

    /**
     * Sets the value of name
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = OwlUtil.toLowerCase( name );
    }

    /**
     * Sets the value of storageLocation
     * @param storageLocation the storageLocation to set
     */
    public void setStorageLocation(String storageLocation) {
        this.storageLocation = storageLocation;
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
        OwlDatabase other = (OwlDatabase) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
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
