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
 * This class represents the DataElement object for Object Relational Mapping.
 */
public class DataElementEntity extends OwlResourceEntity {

    /** The id for this data element */
    private int    id;

    /** The owner for this data element */
    private String owner;

    /** The description for this data element */
    private String description;

    /** The storage location for the data */
    private String location;

    /** The owltable to which this data element belongs */
    private int owlTableId;

    /** The partition to which this data element is added, null if added to owltable */
    private Integer partitionId;

    /** The creation time for this data element */
    private long   createdAt;

    /** The last modification time for this data element */
    private long   lastModifiedAt;

    /** The database object version for this entry */
    private int    version;

    /** The loader information for data element */
    private String    loader;

    /** The schema id for data element */
    private Integer   schemaId;
    /**
     * Instantiates a new data element entity.
     */
    public DataElementEntity() {
    }


    /**
     * Instantiates a new data element entity.
     * 
     * @param owner
     *            the owner
     * @param description
     *            the description
     * @param location
     *            the location
     * @param owlTableId
     *            the owl table id
     * @param partitionId
     *            the partition id
     * @param loader
     *            the loader information           
     */
    public DataElementEntity(String owner, String description,
            String location, int owlTableId, Integer partitionId, String loader) {
        this.owner = owner;
        this.description = description;
        this.location = location;
        this.owlTableId = owlTableId;
        this.partitionId = partitionId;
        this.loader = loader;
    }

    /**
     * Instantiates a new data element entity.
     * 
     * @param id
     *            the id
     * @param owner
     *            the owner
     * @param description
     *            the description
     * @param location
     *            the location
     * @param owlTableId
     *            the owl table id
     * @param partitionId
     *            the partition id
     * @param createdAt
     *            the created at
     * @param lastModifiedAt
     *            the last modified at
     * @param version
     *            the version
     * @param loader
     *            the loader informaiton
     */
    public DataElementEntity(int id, String owner, String description,
            String location, int owlTableId, Integer partitionId,
            long createdAt, long lastModifiedAt, int version, String loader) {
        this.id = id;
        this.owner = owner;
        this.description = description;
        this.location = location;
        this.owlTableId = owlTableId;
        this.partitionId = partitionId;
        this.createdAt = createdAt;
        this.lastModifiedAt = lastModifiedAt;
        this.version = version;
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
     * Gets the value of owlTableId
     * @return the owlTableId
     */
    public int getOwlTableId() {
        return owlTableId;
    }

    /**
     * Gets the value of partitionId
     * @return the partitionId
     */
    public Integer getPartitionId() {
        return partitionId;
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
     * Gets the information of loader
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
     * Sets the value of owlTableId
     * @param owlTableId the owlTableId to set
     */
    public void setOwlTableId(int owlTableId) {
        this.owlTableId = owlTableId;
    }

    /**
     * Sets the value of partitionId
     * @param partitionId the partitionId to set
     */
    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
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
     * Sets the loader information
     * @param loader the loader information to set
     */
    public void setLoader(String loader){
        this.loader = loader;
    }


    /**
     * Gets the schema id.
     * 
     * @return the schema id
     */
    public Integer getSchemaId() {
        return schemaId;
    }


    /**
     * Sets the schema id.
     * 
     * @param schemaId the new schema id
     */
    public void setSchemaId(Integer schemaId) {
        this.schemaId = schemaId;
    }

}
