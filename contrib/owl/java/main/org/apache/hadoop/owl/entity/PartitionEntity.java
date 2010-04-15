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
 * This class represents the Partition object for Object Relational Mapping.
 */
public class PartitionEntity extends OwlResourceEntity {

    /** The id for this key */
    private int                  id;

    /** The owner for this partition */
    private String               owner;

    /** The description for this partition */
    private String               description;

    /** The owltable to which this partition belongs */
    private int                  owlTableId;

    /** The parent partition for this partition, null for first level partition */
    private Integer              parentPartitionId;

    /** The state of this partition, offline or online */
    private int                  state;

    /** The keys values for this partition */
    private List<KeyValueEntity> keyValues;

    /** The partitionLevel for this partition, starting from 1 */
    private int                  partitionLevel;

    /** Is this a leaf level partition */
    private char                 isLeaf;

    /** The creation time for this Database */
    private long                 createdAt;

    /** The last modification time for this Database */
    private long                 lastModifiedAt;

    /** The database object version for this entry */
    private int                  version;

    /**
     * Instantiates a new partition entity.
     */
    public PartitionEntity() {
    }

    /**
     * Instantiates a new partition entity.
     * 
     * @param owner
     *            the owner
     * @param description
     *            the description
     * @param owlTableId
     *            the owl table id
     * @param parentPartitionId
     *            the parent partition id
     * @param state
     *            the state
     */
    public PartitionEntity(String owner, String description,
            int owlTableId, Integer parentPartitionId, int state) {
        this.owner = owner;
        this.description = description;
        this.owlTableId = owlTableId;
        this.parentPartitionId = parentPartitionId;
        this.state = state;
    }

    /**
     * Instantiates a new partition entity.
     * 
     * @param id
     *            the id
     * @param owner
     *            the owner
     * @param description
     *            the description
     * @param owlTableId
     *            the owl table id
     * @param parentPartitionId
     *            the parent partition id
     * @param state
     *            the state
     * @param keyValues
     *            the key values
     * @param partitionLevel
     *            the partition level
     * @param isLeaf
     *            the is leaf
     * @param createdAt
     *            the created at
     * @param lastModifiedAt
     *            the last modified at
     * @param version
     *            the version
     */
    public PartitionEntity(int id, String owner, String description,
            int owlTableId, Integer parentPartitionId, int state,
            List<KeyValueEntity> keyValues, int partitionLevel, char isLeaf,
            long createdAt, long lastModifiedAt, int version) {
        this.id = id;
        this.owner = owner;
        this.description = description;
        this.owlTableId = owlTableId;
        this.parentPartitionId = parentPartitionId;
        this.state = state;
        this.keyValues = keyValues;
        this.partitionLevel = partitionLevel;
        this.isLeaf = isLeaf;
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
     * Gets the value of parentPartition
     * @return the parentPartition
     */
    public Integer getParentPartitionId() {
        return parentPartitionId;
    }

    /**
     * Gets the value of partitionLevel
     * @return the partitionLevel
     */
    public int getPartitionLevel() {
        return partitionLevel;
    }

    /**
     * Gets the value of isLeaf
     * @return the isLeaf
     */
    public char getIsLeaf() {
        return isLeaf;
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
     * Sets the value of parentPartitionId
     * @param parentPartitionId the parentPartitionId to set
     */
    public void setParentPartition(Integer parentPartitionId) {
        this.parentPartitionId = parentPartitionId;
    }

    /**
     * Sets the value of partitionLevel
     * @param partitionLevel the partitionLevel to set
     */
    public void setPartitionLevel(int partitionLevel) {
        this.partitionLevel = partitionLevel;
    }

    /**
     * Sets the value of isLeaf
     * @param isLeaf the isLeaf to set
     */
    public void setIsLeaf(char isLeaf) {
        this.isLeaf = isLeaf;
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
     * Sets the state.
     * 
     * @param state
     *            the new state
     */
    public void setState(int state) {
        this.state = state;
    }

    /**
     * Gets the state.
     * 
     * @return the state
     */
    public int getState() {
        return state;
    }

    /**
     * Sets the owl table id.
     * 
     * @param owlTableId
     *            the new owl table id
     */
    public void setOwlTableId(int owlTableId) {
        this.owlTableId = owlTableId;
    }

    /**
     * Gets the owl table id.
     * 
     * @return the owl table id
     */
    public int getOwlTableId() {
        return owlTableId;
    }

    /**
     * Sets the key values.
     * 
     * @param keyValues
     *            the new key values
     */
    public void setKeyValues(List<KeyValueEntity> keyValues) {
        this.keyValues = keyValues;
    }

    /**
     * Gets the key values.
     * 
     * @return the key values
     */
    public List<KeyValueEntity> getKeyValues() {
        return keyValues;
    }

}
