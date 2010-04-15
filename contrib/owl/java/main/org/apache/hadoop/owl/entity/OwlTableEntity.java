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

import java.util.List;


/**
 * This class represents the OwlTable object for Object Relational Mapping.
 */
public class OwlTableEntity extends OwlResourceEntity {

    /** The id for this database */
    private int                         id;

    /** The database for this owltable */
    int                                 databaseId;

    /** The name for this database */
    private String                      name;

    /** The owner for this database */
    private String                      owner;


    /** The description for this database */
    private String                      description;

    /** The storage location for the owltable */
    private String                      location;

    /** The partitioning keys for this owltable */
    private List<PartitionKeyEntity>    partitionKeys;

    /** The property keys for this owltable */
    private List<PropertyKeyEntity>     propertyKeys;

    /** KeyValue's specified for this owltable */
    private List<OwlTableKeyValueEntity> keyValues;

    /** The creation time for this OwlTable */
    private long                        createdAt;

    /** The last modification time for this OwlTable */
    private long                        lastModifiedAt;

    /** The database object version for this entry */
    private int                         version;

    /** The schema for this owl table */
    private Integer                     schemaId = null;

    /** The loader information of this owl table */
    private String                      loader = null;

    /**
     * Instantiates a new owl table entity.
     */
    public OwlTableEntity() {
    }

    /**
     * Instantiates a new owl table entity.
     * 
     * @param databaseId
     *            the database id
     * @param name
     *            the name
     * @param owner
     *            the owner
     * @param description
     *            the description
     * @param location
     *            the location
     * @param loader
     *            the loader
     */
    public OwlTableEntity(int databaseId, String name, String owner,
            String description, String location, Integer schemaId, String loader) {
        this.databaseId = databaseId;
        this.name = name;
        this.owner = owner;
        this.description = description;
        this.location = location;
        this.schemaId = schemaId;
        this.loader = loader;
    }


    /**
     * Instantiates a new owl table entity.
     * 
     * @param id
     *            the id
     * @param databaseId
     *            the database id
     * @param name
     *            the name
     * @param owner
     *            the owner
     * @param description
     *            the description
     * @param location
     *            the location
     * @param partitionKeys
     *            the partition keys
     * @param propertyKeys
     *            the property keys
     * @param keyValues
     *            the key values
     * @param createdAt
     *            the created at
     * @param lastModifiedAt
     *            the last modified at
     * @param version
     *            the version
     */
    public OwlTableEntity(int id, int databaseId, String name, String owner,
            String description, String location,
            List<PartitionKeyEntity> partitionKeys,
            List<PropertyKeyEntity> propertyKeys,
            List<OwlTableKeyValueEntity> keyValues, long createdAt,
            long lastModifiedAt, int version, Integer schemaId, String loader) {
        this.id = id;
        this.databaseId = databaseId;
        this.name = name;
        this.owner = owner;
        this.description = description;
        this.location = location;
        this.partitionKeys = partitionKeys;
        this.propertyKeys = propertyKeys;
        this.keyValues = keyValues;
        this.createdAt = createdAt;
        this.lastModifiedAt = lastModifiedAt;
        this.version = version;
        this.schemaId = schemaId;
        this.loader = loader;
    }

    /**
     * Gets the value of id
     * @return the id
     */
    @Override
    public int getId() {
        return id;
    }

    /**
     * Gets the value of databaseId
     * @return the databaseId
     */
    public int getDatabaseId() {
        return databaseId;
    }

    /**
     * Gets the value of name
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the value of owner
     * @return the owner
     */
    @Override
    public String getOwner() {
        return owner;
    }

    /**
     * Gets the value of description
     * @return the description
     */
    @Override
    public String getDescription() {
        return description;
    }

    /**
     * Gets the value of location
     * @return the location
     */
    public String getLocation() {
        return location;
    }

    /**
     * Gets the value of partitionKeys
     * @return the partitionKeys
     */
    public List<PartitionKeyEntity> getPartitionKeys() {
        return partitionKeys;
    }

    /**
     * Gets the value of propertyKeys
     * @return the propertyKeys
     */
    public List<PropertyKeyEntity> getPropertyKeys() {
        return propertyKeys;
    }

    /**
     * Gets the value of keyValues
     * @return the keyValues
     */
    public List<OwlTableKeyValueEntity> getKeyValues() {
        return keyValues;
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
     * Gets the value of schemaString
     * @return the schemaString
     */
    public Integer getSchemaId() {
        return schemaId;
    }

    /**
     * Gets the value of loader
     * @return the loader
     */
    public String getLoader() {
        return loader;
    }

    /**
     * Sets the value of id
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Sets the value of databaseId
     * @param databaseId the databaseId to set
     */
    public void setDatabaseId(int databaseId) {
        this.databaseId = databaseId;
    }

    /**
     * Sets the value of name
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets the value of owner
     * @param owner the owner to set
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * Sets the value of description
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Sets the value of location
     * @param location the location to set
     */
    public void setLocation(String location) {
        this.location = location;
    }

    /**
     * Sets the value of partitionKeys
     * @param partitionKeys the partitionKeys to set
     */
    public void setPartitionKeys(List<PartitionKeyEntity> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    /**
     * Sets the value of propertyKeys
     * @param propertyKeys the propertyKeys to set
     */
    public void setPropertyKeys(List<PropertyKeyEntity> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }

    /**
     * Sets the value of keyValues
     * @param keyValues the keyValues to set
     */
    public void setKeyValues(List<OwlTableKeyValueEntity> keyValues) {
        this.keyValues = keyValues;
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
     * Sets the value of schemaId
     * @param schemaId the schemaId to set
     */
    public void setSchemaId(Integer schemaId) {
        this.schemaId = schemaId;
    }

    /**
     * Sets the value of loader
     * @param loader the loader information to set
     */
    public void setLoader(String loader) {
        this.loader = loader;
    }

    /**
     * Adds a OwlTableKeyValueEntity to the list of keyValues
     * @param kv The OwlTableKeyValueEntity to add
     */
    public void addKeyValuePair(OwlTableKeyValueEntity kv){
        this.keyValues.add(kv);
    }

    /**
     * Removes a OwlTableKeyValueEntity from the list of keyValues
     * @param kv The OwlTableKeyValueEntity to remove
     */
    public void removeKeyValuePair(OwlTableKeyValueEntity kv){
        this.keyValues.remove(kv);
    }
}
