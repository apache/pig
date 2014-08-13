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

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.util.Utils;

/**
 * A class representing information about a sort column in {@link SortInfo} 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SortColInfo implements Serializable {
    
    private static final long serialVersionUID = 1L;

    // name of sort column
    private String colName;
    
    // index position (0 based) of sort column
    private int colIndex;
    
    public enum Order { ASCENDING, DESCENDING }
    
    private Order sortOrder;

    
    /**
     * @param colName sort column name
     * @param colIndex index position (0 based) of sort column
     * @param orderingType whether the column is sorted ascending or descending
     */
    public SortColInfo(String colName, int colIndex, Order orderingType) {
        this.colName = colName;
        this.colIndex = colIndex;
        this.sortOrder = orderingType;
    }

    /**
     * @return the sort column name - could be null or empty string if
     * column name could not be determined either because of the absence of
     * a schema or because the schema had the column name as null or empty
     * string - caller should check for these conditions.
     */
    public String getColName() {
        return colName;
    }

    /**
     * @return index position (0 based) of sort column
     */
    public int getColIndex() {
        return colIndex;
    }

    /**
     * @return whether the column is sorted ascending or descending
     */
    public Order getSortOrder() {
        return sortOrder;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31; 
        int result = 1;
        result = prime * result + ((colName == null) ? 0 : colName.hashCode());
        result = prime * result + colIndex;
        result = prime * result + ((sortOrder == Order.ASCENDING) ? 1 : 2);
        return result;
    }   
    
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if(!Utils.checkNullAndClass(this, obj)) {
            return false;
        }
        SortColInfo other = (SortColInfo)obj;
        return Utils.checkNullEquals(this.colName, other.colName, true) &&
        this.colIndex == other.colIndex && 
        this.sortOrder == other.sortOrder;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "colname:" + colName +",colindex:" + colIndex + ",orderingType:" 
        + (sortOrder == Order.ASCENDING ? "ascending" : "descending");
    }
    
    
    
    
}
