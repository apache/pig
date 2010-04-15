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
 * This class represents a value within a Key List object for Object Relational Mapping.
 */
public class KeyListValueEntity {

    /** The id for this key */
    private int             id;

    /** The integer value set for the key */
    private Integer         intValue;

    /** The string value set for the key */
    private String          stringValue;


    /**
     * Instantiates a new key list value entity.
     */
    public KeyListValueEntity() {
    }


    /**
     * Instantiates a new key list value entity.
     * 
     * @param intValue
     *            the int value
     * @param stringValue
     *            the string value
     */
    public KeyListValueEntity(Integer intValue,
            String stringValue) {
        this.intValue = intValue;
        this.stringValue = stringValue;
    }

    /**
     * Gets the value of id
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * Gets the value of intValue
     * @return the intValue
     */
    public Integer getIntValue() {
        return intValue;
    }

    /**
     * Gets the value of stringValue
     * @return the stringValue
     */
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

}
