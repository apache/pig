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

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents the GlobalKey object for Object Relational Mapping.
 */
public class GlobalKeyEntity extends OwlResourceEntity implements KeyBaseEntity {

    /** The id for this key */
    private int                      id;

    /** The description for this Entity */
    private String                   description;

    /** The owner of this key */
    private String                   owner;

    /** The name of the global key */
    private String                   name;

    /** The data type for the global key */
    private int                      dataType;

    /** The creation time for this Database */
    private long                     createdAt;

    /** The last modification time for this Database */
    private long                     lastModifiedAt;

    /** The database object version for this entry */
    private int                      version;

    /** The list of allowed values */
    private List<KeyListValueEntity> listValues;

    /**
     * Instantiates a new global key entity.
     */
    public GlobalKeyEntity() {
    }

    /**
     * Instantiates a new global key entity.
     * 
     * @param name
     *            the name
     * @param dataType
     *            the data type
     */
    public GlobalKeyEntity(String name, int dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    /**
     * Instantiates a new global key entity.
     * 
     * @param id
     *            the id
     * @param description
     *            the description
     * @param owner
     *            the owner
     * @param name
     *            the name
     * @param dataType
     *            the data type
     * @param createdAt
     *            the created at
     * @param lastModifiedAt
     *            the last modified at
     * @param version
     *            the version
     */
    public GlobalKeyEntity(int id, String description, String owner,
            String name, int dataType, long createdAt, long lastModifiedAt,
            int version) {
        this.id = id;
        this.description = description;
        this.owner = owner;
        this.name = name;
        this.dataType = dataType;
        this.createdAt = createdAt;
        this.lastModifiedAt = lastModifiedAt;
        this.version = version;
    }

    /**
     * Gets the value of id
     * @return the id
     */
    @Override
    public int getId() {
        return id;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.KeyBaseEntity#getName()
     */
    @Override
    public String getName() {
        return name;
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.KeyBaseEntity#getDataType()
     */
    @Override
    public int getDataType() {
        return dataType;
    }

    /**
     * Gets the value of createdAt
     * @return the createdAt
     */
    public long getCreatedAt() {
        return createdAt;
    }

    /**
     * Gets the value of lastModifiedAt
     * @return the lastModifiedAt
     */
    public long getLastModifiedAt() {
        return lastModifiedAt;
    }

    /**
     * Gets the value of version
     * @return the version
     */
    @Override
    public int getVersion() {
        return version;
    }

    /**
     * Sets the value of id
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Sets the value of name
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets the value of dataType
     * @param dataType the dataType to set
     */
    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    /**
     * Sets the value of createdAt
     * @param createdAt the createdAt to set
     */
    @Override
    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * Sets the value of lastModifiedAt
     * @param lastModifiedAt the lastModifiedAt to set
     */
    @Override
    public void setLastModifiedAt(long lastModifiedAt) {
        this.lastModifiedAt = lastModifiedAt;
    }

    /**
     * Sets the value of version
     * @param version the version to set
     */
    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * Sets the description.
     * 
     * @param description
     *            the new description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.OwlResourceEntity#getDescription()
     */
    @Override
    public String getDescription() {
        return description;
    }

    /**
     * Sets the owner.
     * 
     * @param owner
     *            the new owner
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.OwlResourceEntity#getOwner()
     */
    @Override
    public String getOwner() {
        return owner;
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.KeyBaseEntity#getListValues()
     */
    @Override
    public List<KeyListValueEntity> getListValues() {
        return listValues;
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.KeyBaseEntity#setListValues(java.util.List)
     */
    @Override
    public void setListValues(List<KeyListValueEntity> listValues) {
        this.listValues = listValues;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.KeyBaseEntity#addListValue(java.lang.Integer, java.lang.String)
     */
    @Override
    public void addListValue(Integer intValue, String stringValue) {
        if( listValues == null ) {
            listValues = new ArrayList<KeyListValueEntity>();
        }

        listValues.add(new KeyListValueEntity(intValue, stringValue));
    }

}
