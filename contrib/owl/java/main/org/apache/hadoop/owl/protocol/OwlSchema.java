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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlSchemaUtil;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;

/**
 * Class which represents a schema in OwlBase.
 */
public class OwlSchema extends OwlObject implements Serializable {    

    /** The serialization version */
    private static final long serialVersionUID = 1L;     

    /** The list of OwlColumnSchema. */
    private ArrayList<OwlColumnSchema> columnSchema;

    /**
     * Instantiates a new owl schema.
     */
    public OwlSchema() {
    }

    /**
     * Instantiates a new owl schema.
     */
    public OwlSchema(ArrayList<OwlColumnSchema> columnSchema) {
        this.columnSchema = columnSchema;
    }

    /**
     * Adds the given column schema to the current schema's list of columns
     * @param column the column schema to add
     */
    public void addColumnSchema(OwlColumnSchema column) {
        if( columnSchema == null ) {
            columnSchema = new ArrayList<OwlColumnSchema>();
        }

        if( column.getColumnNumber() == null ) {
            column.setColumnNumber(columnSchema.size());
        }

        columnSchema.add(column);
    }

    /**
     * Gets the column count.
     * @return the column count
     */
    public int getColumnCount() {
        if( columnSchema == null ) {
            return 0;
        }

        return columnSchema.size();
    }

    /**
     * Gets the schema for the column at given index.
     * @param index the index
     * @return the column schema
     * @throws OwlException the owl exception
     */
    public OwlColumnSchema columnAt(int index) throws OwlException {
        if( columnSchema == null ) {
            throw new OwlException(ErrorType.INVALID_OWL_SCHEMA, "Column schema not initialized");
        }else if( index < 0 || index >= columnSchema.size() ) {
            throw new OwlException(ErrorType.INVALID_OWL_SCHEMA, "Invalid column index " + index);
        } else {
            return columnSchema.get(index);
        }
    }

    /**
     * Gets the value of columnSchema
     * @return the columnSchema
     */
    public ArrayList<OwlColumnSchema> getColumnSchema(){
        return columnSchema;
    }

    /**
     * Sort the list of OwlColumnSchema based on each column's positionNumber
     * @param owlColumnSchema the column schema list
     * @throws OwlException 
     */
    public void setColumnSchema(ArrayList<OwlColumnSchema> owlColumnSchema){
        this.sortColumnSchemaList(owlColumnSchema);
        this.columnSchema = owlColumnSchema;
    }

    public void sortColumnSchemaList(ArrayList<OwlColumnSchema> owlColumnSchema){
        boolean hasColumnNumber = true;
        // sort recursively
        for(OwlColumnSchema column : owlColumnSchema ) {
            if ( column.getColumnNumber() != null){
                if (column.getSchema() !=  null) {
                    // if has subschema
                    OwlSchema s = column.getSchema();
                    s.sortColumnSchemaList(s.getColumnSchema());
                }
            }else {
                hasColumnNumber = false;
            }
        }//for
        if (hasColumnNumber == true){
            Collections.sort(owlColumnSchema, new OwlColumnSchemaComparable());
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result
        + ((columnSchema == null) ? 0 : columnSchema.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        OwlSchema other = (OwlSchema) obj;
        if (columnSchema == null) {
            if (other.columnSchema != null) {
                return false;
            }
        } else if (!columnSchema.equals(other.columnSchema)) {
            return false;
        }
        return true;
    }

    /**
     * Gets the internal string representation of the schema. String format is subject to change.
     * @return the schema string
     * @throws OwlException the owl exception
     */
    public String getSchemaString() throws OwlException {
        return OwlSchemaUtil.getSchemaString(this);
    }

    public static class OwlColumnSchemaComparable implements Comparator<OwlColumnSchema>, Serializable {

        /** The serialization version */
        private static final long serialVersionUID = 1L;

        //@Override
        public int compare(OwlColumnSchema c1, OwlColumnSchema c2) {
            int i1 = c1.getColumnNumber().intValue();
            int i2 = c2.getColumnNumber().intValue();
            return (i1 < i2 ? -1 : ((i1 == i2) ? 0 : 1));
        }
    }
}
