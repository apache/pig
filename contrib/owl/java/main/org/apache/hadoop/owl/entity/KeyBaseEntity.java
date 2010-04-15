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

/** Interface implemented by all Key entities */ 
public interface KeyBaseEntity {


    /**
     * Gets the value of name
     * @return the name
     */
    public String getName();

    /**
     * Gets the value of dataType
     * @return the dataType
     */
    public int getDataType();


    /**
     * Gets the list values.
     * 
     * @return the list values
     */
    public List<KeyListValueEntity> getListValues();


    /**
     * Sets the list values.
     * 
     * @param listValues
     *            the new list values
     */
    public void setListValues(List<KeyListValueEntity> listValues);


    /**
     * Adds the given value to the bounded list.
     * 
     * @param intValue
     *            the int value
     * @param stringValue
     *            the string value
     */
    public void addListValue(Integer intValue, String stringValue);

}
