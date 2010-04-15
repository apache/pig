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

/** Abstract base class for KeyValue entities */
public abstract class KeyValueBaseEntity extends OwlEntity {

    /**
     * Gets the value of propertyKeyId
     * @return the propertyKeyId
     */
    public abstract Integer getPropertyKeyId();

    /**
     * Gets the value of globalKeyId
     * @return the globalKeyId
     */
    public abstract Integer getGlobalKeyId();

    /**
     * Gets the value of intValue
     * @return the intValue
     */
    public abstract Integer getIntValue();

    /**
     * Gets the value of stringValue
     * @return the stringValue
     */
    public abstract String getStringValue();

}
