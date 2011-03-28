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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;

// Represent one column inside order key, this is a direct mapping from POProject
public class ColumnInfo implements Cloneable {
    List<Integer> columns;
    byte resultType;
    int startCol = -1;
    boolean isRangeProject = false;
    
    public ColumnInfo(List<Integer> columns, byte type) {
        this.columns = columns;
        this.resultType = type;
    }

    /**
     * Constructor for range projection or project-star
     * @param startCol
     * @param type
     */
    public ColumnInfo(int startCol, byte type) {
        this.startCol = startCol;
        this.resultType = type;
        this.isRangeProject = true;
    }
    
    public String toString() {
        String result;
        if (isStar())
            result = "[*]:";
        result = columns.toString()+":";
        result+=DataType.findTypeName(resultType);
        return result;
    }
    
    private boolean isStar() {
        return isRangeProject && startCol == 0;
    }

    public boolean equals(Object o2)
    {
        
        if (o2 == null || !(o2 instanceof ColumnInfo))
            return false;
        ColumnInfo c2 = (ColumnInfo)o2;
        if (
                isRangeProject == c2.isRangeProject &&
                startCol == c2.startCol &&
                ((columns == null && c2.columns == null) || 
                        (columns != null && columns.equals(c2.columns)))
        )
            return true;
        
        return false;
    }
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException{
        ColumnInfo newColInfo = (ColumnInfo)super.clone();
        //copy the mutable field
        List<Integer> cols = new ArrayList<Integer>();
        cols.addAll(this.columns);
        newColInfo.columns = cols;
        return newColInfo;
        
    }
}
