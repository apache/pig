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
package org.apache.hadoop.owl.entity;

/**
 * This class is the base class for all the resource level Entities which are managed
 * by the OR Mapping. Fields added here would not be persisted by default unless
 * inheritance is defined and fields are added in persistence.xml. So this
 * class is mainly intended for functions shared across all Resources. 
 */
public abstract class OwlResourceEntity extends OwlEntity {


    /**
     * Gets the owner.
     * 
     * @return the owner
     */
    public abstract String getOwner();


    /**
     * Gets the description.
     * 
     * @return the description
     */
    public abstract String getDescription();

    /**
     * Sets the created at timestamp.
     * 
     * @param createdAt
     *            the new created at timestamp
     */
    public abstract void setCreatedAt(long createdAt);

    /**
     * Sets the last modification timestamp.
     * 
     * @param lastModifiedAt
     *            the new last modification timestamp
     */
    public abstract void setLastModifiedAt(long lastModifiedAt);

    /**
     * Sets the value of version
     * @param version the version to set
     */
    public abstract void setVersion(int version);

    /**
     * Gets the value of version
     * @return the version
     */
    public abstract int getVersion();


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.OwlEntity#parentResource()
     */
    @Override
    public OwlResourceEntity parentResource() {
        //OwlResourceEntity are top level entities, they do not have an parent entity.
        return null;
    }
}
