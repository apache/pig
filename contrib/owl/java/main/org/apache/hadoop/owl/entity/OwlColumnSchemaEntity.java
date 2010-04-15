/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.owl.entity;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.schema.Schema.ColumnSchema;

/** The entity representing a column in a schema */
public class OwlColumnSchemaEntity extends OwlEntity{

    /** The id for the column */
    private int    id;

    /** The column name. */
    private String name;

    /** The column data type. */
    private String columnType;

    /** The parent schema for the column. */
    private OwlSchemaEntity parentSchema;

    /** The sub schema for the column. */
    private Integer subSchemaId = null;

    /** The subschema for the column. Not persisted to database (the subSchemaId only is persisted).
     * This field is present so that the recursive object creation can be done. */
    transient OwlSchemaEntity subSchema = null;

    /** The column number of the OwlColumnSchema in the schema 
    /* If this field is null, this indicates that list of OwlColumnSchema is not sorted */
    Integer columnNumber = null; 

    /**
     * Instantiates a new owl column schema entity.
     */
    public OwlColumnSchemaEntity(){
    }

    /**
     * Instantiates a new owl column schema entity.
     * @param cs the column schema
     * @param parentSchema the parent schema
     * @throws OwlException 
     */
    public OwlColumnSchemaEntity(ColumnSchema cs, OwlSchemaEntity parentSchema, int columnNumber) throws OwlException {
        //Entity object id is auto-updated by orm when calling backend.create()
        this.name = cs.name;
        this.columnType = cs.type.getName();
        this.parentSchema = parentSchema;

        if( cs.getSchema() != null ) {
            this.subSchema = new OwlSchemaEntity(cs.getSchema());
        }

        this.columnNumber = columnNumber;
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
     * Gets the value of name
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the value of columnType
     * @return the columnType
     */
    public String getColumnType() {
        return columnType;
    }

    /**
     * Gets the value of parentSchema
     * @return the parentSchema
     */
    public OwlSchemaEntity getParentSchema() {
        return parentSchema;
    }

    /**
     * Gets the value of subSchemaId
     * @return the subSchemaId
     */
    public Integer getSubSchemaId() {
        return subSchemaId;
    }

    /**
     * Sets the value of id
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Sets the value of name
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets the value of columnType
     * @param columnType the columnType to set
     */
    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    /**
     * Sets the value of parentSchema
     * @param parentSchema the parentSchema to set
     */
    public void setParentSchema(OwlSchemaEntity parentSchema) {
        this.parentSchema = parentSchema;
    }

    /**
     * Sets the value of subSchemaId
     * @param subSchemaId the subSchemaId to set
     */
    public void setSubSchemaId(Integer subSchemaId) {
        this.subSchemaId = subSchemaId;
    }

    /**
     * Gets the value of subSchema
     * @return the subSchema
     */
    public OwlSchemaEntity getSubSchema() {
        return subSchema;
    }

    /**
     * Sets the value of subSchema
     * @param subSchema the subSchema to set
     */
    public void setSubSchema(OwlSchemaEntity subSchema) {
        this.subSchema = subSchema;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.entity.OwlEntity#parentResource()
     */
    @Override
    public OwlResourceEntity parentResource() {
        return parentSchema;
    }

    /**
     * Gets the column number.
     * 
     * @return the column number
     */
    public Integer getColumnNumber() {
        return columnNumber;
    }

    /**
     * Sets the column number.
     * 
     * @param columnNumber the new column number
     */
    public void setColumnNumber(Integer columnNumber) {
        this.columnNumber = columnNumber;
    }
}


