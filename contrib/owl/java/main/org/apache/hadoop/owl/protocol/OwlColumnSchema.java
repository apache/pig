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
package org.apache.hadoop.owl.protocol;

import java.io.Serializable;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;

/**
 * Class which represents a ColumnSchema in the OwlSchema.
 */
public class OwlColumnSchema extends OwlObject implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;     

    /** The column name. */
    private String name;

    /** The column data type. */
    private ColumnType type;

    /** The subschema for the column. */
    private OwlSchema schema;

    /** The description. */
    private String description;

    private Integer columnNumber = null;


    public OwlColumnSchema(){
    }
    /**
     * Initialize an OwlColumnSchema.
     * @param name column name
     * @param type column type
     * @throws OwlException 
     */
    public OwlColumnSchema(String name, ColumnType type) throws OwlException {
        this(name, type, null);
    }

    /**
     * Initialize an OwlColumnSchema.
     * @param name column name
     * @param type the column type
     * @param schema column schema
     * @throws OwlException the owl exception
     */
    public OwlColumnSchema(String name, ColumnType type, OwlSchema schema ) throws OwlException {

        if ((schema != null) && !(ColumnType.isSchemaType(type))) {
            throw new OwlException(
                    ErrorType.ZEBRA_SCHEMA_EXCEPTION, "Only a COLLECTION or RECORD or MAP can have schemas.");
        }

        this.name = OwlUtil.toLowerCase( name );
        this.schema = schema;
        this.type = type;
    }

    /**
     * Initialize an OwlColumnSchema.
     * @param type the column type
     * @param schema column schema
     * @throws OwlException the owl exception
     */
    public OwlColumnSchema(ColumnType type, OwlSchema schema ) throws OwlException {

        if (type != ColumnType.RECORD){
            throw new OwlException ( ErrorType.ERROR_MISSING_COLUMNNAME, "for schema ["+ schema.getSchemaString() +"]");
        }
        this.name = null;
        this.schema = schema;
        this.type = type;
    }

    /**
     * Gets the name.
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name.
     * @param name the new name
     */
    public void setName(String name) {
        this.name = OwlUtil.toLowerCase( name );
    }

    /**
     * Gets the type.
     * @return the type
     */
    public ColumnType getType() {
        return type;
    }

    /**
     * Sets the type.
     * @param type the new type
     */
    public void setType(ColumnType type) {
        this.type = type;
    }

    /**
     * Gets the schema.
     * @return the schema
     */
    public OwlSchema getSchema() {
        return schema;
    }

    /**
     * Sets the schema.
     * @param schema the new schema
     */
    public void setSchema(OwlSchema schema) {
        this.schema = schema;
    }

    /**
     * Gets the description.
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the description.
     * @param description the new description
     */
    public void setDescription(String description) {
        this.description = description;
    }



    /**
     * Gets the columnNumber.
     * @return the columnNumber
     */
    public Integer getColumnNumber() {
        return columnNumber;
    }

    /**
     * Sets the columnNumber.
     * @param columnNumber the columnNumber
     */
    public void setColumnNumber(Integer columnNumber) {
        this.columnNumber = columnNumber;
    }

    // description is not included in hashCode() and equals()
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result
        + ((columnNumber == null) ? 0 : columnNumber.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        OwlColumnSchema other = (OwlColumnSchema) obj;
        if (columnNumber == null) {
            if (other.columnNumber != null)
                return false;
        } else if (!columnNumber.equals(other.columnNumber))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (schema == null) {
            if (other.schema != null)
                return false;
        } else if (!schema.equals(other.schema))
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }

}
