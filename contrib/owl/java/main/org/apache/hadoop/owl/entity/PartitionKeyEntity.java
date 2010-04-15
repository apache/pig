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
 * This class represents the PartitionKey object for Object Relational Mapping.
 */
public class PartitionKeyEntity extends OwlEntity implements KeyBaseEntity {

    /** The id for this key */
    private int                      id;

    /** The owltable to which this key is added */
    private OwlTableEntity           owlTable;

    /** The partitioning level of this key within the owltable, starting from 1 */
    private int                      partitionLevel;

    /** The name of the partitioning key */
    private String                   name;

    /** The data type for the partitioning key */
    private int                      dataType;

    /** The partitioning type */
    private int                      partitioningType;

    /** The begin date if interval partitioning */
    private Long                     intervalStart;

    /** The frequency if interval partitioning */
    private Integer                  intervalFrequency;

    /** The unit for the frequency if interval partitioning */
    private Integer                  intervalFrequencyUnit;

    /** The list of allowed values */
    private List<KeyListValueEntity> listValues;

    /**
     * Instantiates a new partition key entity.
     */
    public PartitionKeyEntity() {
    }


    /**
     * Instantiates a new partition key entity.
     * 
     * @param owlTable
     *            the owl table
     * @param partitionLevel
     *            the partition level
     * @param name
     *            the name
     * @param dataType
     *            the data type
     * @param partitioningType
     *            the partitioning type
     */
    public PartitionKeyEntity(OwlTableEntity owlTable,
            int partitionLevel, String name, int dataType, int partitioningType) {
        this.owlTable = owlTable;
        this.partitionLevel = partitionLevel;
        this.name = name;
        this.dataType = dataType;
        this.partitioningType = partitioningType;
    }

    /**
     * Instantiates a new partition key entity.
     * 
     * @param owlTable
     *            the owl table
     * @param partitionLevel
     *            the partition level
     * @param name
     *            the name
     * @param dataType
     *            the data type
     * @param partitioningType
     *            the partitioning type
     * @param intervalStart
     *            the interval start
     * @param intervalFrequency
     *            the interval frequency
     * @param intervalFrequencyUnit
     *            the interval frequency unit
     */
    public PartitionKeyEntity(OwlTableEntity owlTable,
            int partitionLevel, String name, int dataType, int partitioningType,
            Long intervalStart, Integer intervalFrequency, Integer intervalFrequencyUnit) {
        this.owlTable = owlTable;
        this.partitionLevel = partitionLevel;
        this.name = name;
        this.dataType = dataType;
        this.partitioningType = partitioningType;
        this.intervalStart = intervalStart;
        this.intervalFrequency = intervalFrequency;
        this.intervalFrequencyUnit = intervalFrequencyUnit;
    }


    /**
     * Gets the value of id
     * 
     * @return the id
     */
    @Override
    public int getId() {
        return id;
    }

    /**
     * Gets the value of owlTable
     * @return the owlTable
     */
    public OwlTableEntity getOwlTable() {
        return owlTable;
    }

    /**
     * Gets the value of partitionLevel
     * @return the partitionLevel
     */
    public int getPartitionLevel() {
        return partitionLevel;
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
     * Gets the value of partitioningType
     * @return the partitioningType
     */
    public int getPartitioningType() {
        return partitioningType;
    }

    /**
     * Sets the value of id
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Sets the value of owlTable
     * @param owlTable the owlTable to set
     */
    public void setOwlTable(OwlTableEntity owlTable) {
        this.owlTable = owlTable;
    }

    /**
     * Sets the value of partitionLevel
     * @param partitionLevel the partitionLevel to set
     */
    public void setPartitionLevel(int partitionLevel) {
        this.partitionLevel = partitionLevel;
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
     * Sets the value of partitioningType
     * @param partitioningType the partitioningType to set
     */
    public void setPartitioningType(int partitioningType) {
        this.partitioningType = partitioningType;
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.OwlEntity#parentResource()
     */
    @Override
    public OwlResourceEntity parentResource() {
        return owlTable;
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

    /**
     * Gets the value of intervalStart
     * @return the intervalStart
     */
    public Long getIntervalStart() {
        return intervalStart;
    }

    /**
     * Gets the value of intervalFrequency
     * @return the intervalFrequency
     */
    public Integer getIntervalFrequency() {
        return intervalFrequency;
    }

    /**
     * Gets the value of intervalFrequencyUnit
     * @return the intervalFrequencyUnit
     */
    public Integer getIntervalFrequencyUnit() {
        return intervalFrequencyUnit;
    }

    /**
     * Sets the value of intervalStart
     * @param intervalStart the intervalStart to set
     */
    public void setIntervalStart(Long intervalStart) {
        this.intervalStart = intervalStart;
    }

    /**
     * Sets the value of intervalFrequency
     * @param intervalFrequency the intervalFrequency to set
     */
    public void setIntervalFrequency(Integer intervalFrequency) {
        this.intervalFrequency = intervalFrequency;
    }

    /**
     * Sets the value of intervalFrequencyUnit
     * @param intervalFrequencyUnit the intervalFrequencyUnit to set
     */
    public void setIntervalFrequencyUnit(Integer intervalFrequencyUnit) {
        this.intervalFrequencyUnit = intervalFrequencyUnit;
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
