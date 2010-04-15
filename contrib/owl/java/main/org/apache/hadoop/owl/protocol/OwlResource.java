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

public abstract class OwlResource extends OwlObject{

    /** The owner. */
    private String owner       = null;

    /** The creation time. */
    private long creationTime;

    /** The last modification time. */
    private long modificationTime;

    /**
     * Instantiates a new owl resource.
     */
    public OwlResource() {
    }

    /**
     * Instantiates a new owl resource.
     * 
     * @param owner
     *            the owner
     * @param creationTime
     *            the creation time
     * @param modificationTime
     *            the modification time
     */
    public OwlResource(String owner, long creationTime, long modificationTime) {
        this.owner = owner;
        this.creationTime = creationTime;
        this.modificationTime = modificationTime;
    }

    /**
     * Gets the value of owner
     * @return the owner
     */
    public String getOwner() {
        return owner;
    }



    /**
     * Gets the value of creationTime
     * @return the creationTime
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Gets the value of modificationTime
     * @return the modificationTime
     */
    public long getModificationTime() {
        return modificationTime;
    }

    /**
     * Sets the value of owner
     * @param owner the owner to set
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * Sets the value of creationTime
     * @param creationTime the creationTime to set
     */
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    /**
     * Sets the value of modificationTime
     * @param modificationTime the modificationTime to set
     */
    public void setModificationTime(long modificationTime) {
        this.modificationTime = modificationTime;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (creationTime ^ (creationTime >>> 32));
        result = prime * result
        + (int) (modificationTime ^ (modificationTime >>> 32));
        result = prime * result + ((owner == null) ? 0 : owner.hashCode());
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
        OwlResource other = (OwlResource) obj;
        if (creationTime != other.creationTime) {
            return false;
        }
        if (modificationTime != other.modificationTime) {
            return false;
        }
        if (owner == null) {
            if (other.owner != null) {
                return false;
            }
        } else if (!owner.equals(other.owner)) {
            return false;
        }
        return true;
    }
}
