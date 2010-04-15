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
 * This class represents the PropertyKey object for Object Relational Mapping.
 */
public class PropertyKeyEntity extends OwlEntity implements KeyBaseEntity {

    /** The id for this key */
    private int                      id;

    /** The owltable to which this key is added */
    private OwlTableEntity           owlTable;

    /** The name of the property key */
    private String                   name;

    /** The data type for the property key */
    private int                      dataType;

    /** The list of allowed values */
    private List<KeyListValueEntity> listValues;

    /**
     * Instantiates a new property key entity.
     */
    public PropertyKeyEntity() {
    }

    /**
     * Instantiates a new property key entity.
     * 
     * @param owlTable
     *            the owl table
     * @param name
     *            the name
     * @param dataType
     *            the data type
     */
    public PropertyKeyEntity(OwlTableEntity owlTable, String name, int dataType) {
        this.owlTable = owlTable;
        this.name = name;
        this.dataType = dataType;
    }

    /**
     * Instantiates a new property key entity.
     * 
     * @param id
     *            the id
     * @param owlTable
     *            the owl table
     * @param name
     *            the name
     * @param dataType
     *            the data type
     */
    public PropertyKeyEntity(int id, OwlTableEntity owlTable, String name, int dataType) {
        this.id = id;
        this.owlTable = owlTable;
        this.name = name;
        this.dataType = dataType;
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
     * Gets the value of owlTable
     * @return the owlTable
     */
    public OwlTableEntity getOwlTable() {
        return owlTable;
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
