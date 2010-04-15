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
package org.apache.hadoop.owl.protocol;

import java.io.Serializable;

import org.apache.hadoop.owl.common.OwlUtil;

/** Class to represent a OwlTable name */
public class OwlTableName extends OwlObject implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;     

    /** The database name */
    private String databaseName;

    /** The table name */
    private String tableName;

    /**
     * Instantiates a new owl table name.
     */
    public OwlTableName() {
    }

    /**
     * Instantiates a new owl table name.
     * @param databaseName the database name
     * @param tableName the table name
     */
    public OwlTableName(String databaseName, String tableName) {
        this.databaseName =  OwlUtil.toLowerCase( databaseName );
        this.tableName = OwlUtil.toLowerCase( tableName );
    }

    /**
     * Gets the value of databaseName
     * @return the databaseName
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Gets the value of tableName
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets the value of databaseName
     * @param databaseName the databaseName to set
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = OwlUtil.toLowerCase( databaseName );
    }

    /**
     * Sets the value of tableName
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = OwlUtil.toLowerCase( tableName );
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
        + ((databaseName == null) ? 0 : databaseName.hashCode());
        result = prime * result
        + ((tableName == null) ? 0 : tableName.hashCode());
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
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        OwlTableName other = (OwlTableName) obj;
        if (databaseName == null) {
            if (other.databaseName != null) {
                return false;
            }
        } else if (!databaseName.equals(other.databaseName)) {
            return false;
        }
        if (tableName == null) {
            if (other.tableName != null) {
                return false;
            }
        } else if (!tableName.equals(other.tableName)) {
            return false;
        }
        return true;
    }
}
