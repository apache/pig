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
 * This class is the base class for all Entities which are managed
 * by the OR Mapping. Fields added here would not be persisted by default unless
 * inheritance is defined and fields are added in persistence.xml. So this
 * class is mainly intended for functions shared across all entities. 
 */
public abstract class OwlEntity {

    /**
     * Check if input is true.
     * 
     * @param c the char to check
     * 
     * @return true, if input is Y or y
     */
    public static boolean isTrue(char c) {
        return (c == 'Y' || c == 'y');
    }

    /**
     * Gets the char value of boolean.
     * 
     * @param b the boolean to check
     * 
     * @return the char 'Y' if true is passed, otherwise 'N'
     */
    public static char isTrue(boolean b) {
        return (b ? 'Y' : 'N');
    }

    /**
     * Gets the id for the resource.
     * 
     * @return the id
     */
    public abstract int getId();

    /** Get the resource to which this entity belongs. For OwlResourceEntity objects,
     *  this will be null since they are top level entities. 
     *  @return the resource entity to this the entity belongs
     */
    public abstract OwlResourceEntity parentResource();

}
