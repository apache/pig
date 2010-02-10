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
package org.apache.pig;

import java.io.Serializable;

import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A Class which will encapsulate metadata information that a
 * OutputFormat (or possibly StoreFunc) may want to know
 * about the data that needs to be stored.  
 */
public class StoreConfig implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private String location;
    private Schema schema;
    private SortInfo sortInfo;
    
    
    /**
     * @param location
     * @param schema
     */
    public StoreConfig(String location, Schema schema, SortInfo sortInfo) {
        this.location = location;
        this.schema = schema;
        this.sortInfo = sortInfo;
    }
    
    /**
     * @return the location
     */
    public String getLocation() {
        return location;
    }
    /**
     * @param location the location to set
     */
    public void setLocation(String location) {
        this.location = location;
    }
    /**
     * @return the schema
     */
    public Schema getSchema() {
        return schema;
    }
    /**
     * @param schema the schema to set
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "{location:" + location + ", schema:" + schema + "}";
    }

    /**
     * @param sortInfo the sortInfo to set
     */
    public void setSortInfo(SortInfo sortInfo) {
        this.sortInfo = sortInfo;
    }

    /**
     * This method returns a {@link SortInfo} object giving
     * information on the column names in the output schema which
     * correspond to the sort columns and which columns are
     * ascending and those which are descending
     * @return the sortInfo object if one could be determined else null
     * null is returned in the following scenarios (wherein
     * the sortInfo could not be determined):
     * 1) the store does not follow an order by
     * 2) There are operators other than limit between "order by"
     * and store. If there is a limit between order by and store and
     * if non of the above conditions are true, then sortInfo will be
     * non-null.
     * 
     * IMPORTANT NOTE:
     * The caller should check if the return value is null and
     * take appropriate action
     */
    public SortInfo getSortInfo() {
        return sortInfo;
    }

}
