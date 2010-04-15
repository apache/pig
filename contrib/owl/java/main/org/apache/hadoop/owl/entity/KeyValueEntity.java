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
 * This class represents a KeyValue object for Object Relational Mapping.
 */
public class KeyValueEntity extends KeyValueBaseEntity {

    /** The id for this key */
    private int             id;

    /** The owltable for which this value is set */
    private int             owlTableId;

    /** The partition for which this value is set, null if set at owltable level */
    private PartitionEntity partition;

    /** The partition key for which this value is set */
    private Integer         partitionKeyId;

    /** The property key for which this value is set */
    private Integer         propertyKeyId;

    /** The global key for which this value is set */
    private Integer         globalKeyId;

    /** The integer value set for the key */
    private Integer         intValue;

    /** The string value set for the key */
    private String          stringValue;

    /**
     * Instantiates a new key value entity.
     */
    public KeyValueEntity() {
    }

    /**
     * Instantiates a new key value entity.
     * 
     * @param owlTableId
     *            the owl table id
     * @param partition
     *            the partition
     * @param partitionKeyId
     *            the partition key id
     * @param propertyKeyId
     *            the property key id
     * @param globalKeyId
     *            the global key id
     * @param intValue
     *            the int value
     * @param stringValue
     *            the string value
     */
    public KeyValueEntity(int owlTableId, PartitionEntity partition,
            Integer partitionKeyId, Integer propertyKeyId, Integer globalKeyId,
            Integer intValue, String stringValue) {
        this.owlTableId = owlTableId;
        this.partition = partition;
        this.partitionKeyId = partitionKeyId;
        this.propertyKeyId = propertyKeyId;
        this.globalKeyId = globalKeyId;
        this.intValue = intValue;
        this.stringValue = stringValue;
    }

    /**
     * Instantiates a new key value entity.
     * 
     * @param id
     *            the id
     * @param owlTableId
     *            the owl table id
     * @param partition
     *            the partition
     * @param partitionKeyId
     *            the partition key id
     * @param propertyKeyId
     *            the property key id
     * @param globalKeyId
     *            the global key id
     * @param intValue
     *            the int value
     * @param stringValue
     *            the string value
     */
    public KeyValueEntity(int id, int owlTableId, PartitionEntity partition,
            Integer partitionKeyId, Integer propertyKeyId, Integer globalKeyId,
            Integer intValue, String stringValue) {
        this.id = id;
        this.owlTableId = owlTableId;
        this.partition = partition;
        this.partitionKeyId = partitionKeyId;
        this.propertyKeyId = propertyKeyId;
        this.globalKeyId = globalKeyId;
        this.intValue = intValue;
        this.stringValue = stringValue;
    }

    /**
     * Instantiates a new key value entity.
     * 
     * @param kv the key value to duplicate
     */
    public KeyValueEntity(KeyValueEntity kv) {
        this.id = kv.id;
        this.owlTableId = kv.owlTableId;
        this.partition = kv.partition;
        this.partitionKeyId = kv.partitionKeyId;
        this.propertyKeyId = kv.propertyKeyId;
        this.globalKeyId = kv.globalKeyId;
        this.intValue = kv.intValue;
        this.stringValue = kv.stringValue;
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
     * Gets the value of owlTableId
     * @return the owlTableId
     */
    public int getOwlTableId() {
        return owlTableId;
    }

    /**
     * Gets the value of partition
     * @return the partition
     */
    public PartitionEntity getPartition() {
        return partition;
    }

    /**
     * Gets the value of partitionKeyId.
     * 
     * @return the partitionKeyId
     */
    public Integer getPartitionKeyId() {
        return partitionKeyId;
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.KeyValueBaseEntity#getPropertyKeyId()
     */
    @Override
    public Integer getPropertyKeyId() {
        return propertyKeyId;
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.KeyValueBaseEntity#getGlobalKeyId()
     */
    @Override
    public Integer getGlobalKeyId() {
        return globalKeyId;
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.KeyValueBaseEntity#getIntValue()
     */
    @Override
    public Integer getIntValue() {
        return intValue;
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.KeyValueBaseEntity#getStringValue()
     */
    @Override
    public String getStringValue() {
        return stringValue;
    }

    /**
     * Sets the value of id
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Sets the value of owlTableId
     * @param owlTableId the owlTableId to set
     */
    public void setOwlTableId(int owlTableId) {
        this.owlTableId = owlTableId;
    }

    /**
     * Sets the value of partition
     * @param partition the partition to set
     */
    public void setPartition(PartitionEntity partition) {
        this.partition = partition;
    }

    /**
     * Sets the value of partitionKeyId
     * @param partitionKeyId the partitionKeyId to set
     */
    public void setPartitionKeyId(Integer partitionKeyId) {
        this.partitionKeyId = partitionKeyId;
    }

    /**
     * Sets the value of propertyKeyId
     * @param propertyKeyId the propertyKeyId to set
     */
    public void setPropertyKeyId(Integer propertyKeyId) {
        this.propertyKeyId = propertyKeyId;
    }

    /**
     * Sets the value of globalKeyId
     * @param globalKeyId the globalKeyId to set
     */
    public void setGlobalKeyId(Integer globalKeyId) {
        this.globalKeyId = globalKeyId;
    }

    /**
     * Sets the value of intValue
     * @param intValue the intValue to set
     */
    public void setIntValue(Integer intValue) {
        this.intValue = intValue;
    }

    /**
     * Sets the value of stringValue
     * @param stringValue the stringValue to set
     */
    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.OwlEntity#parentResource()
     */
    @Override
    public OwlResourceEntity parentResource() {
        return partition;
    }

}
