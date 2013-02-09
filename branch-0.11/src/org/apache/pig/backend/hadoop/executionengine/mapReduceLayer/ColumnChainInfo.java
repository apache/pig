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

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;

// Representing one sort key. Sort key may be compound if we sort on multiple columns, 
// if that is the case, then this sort key contains multiple ColumnChainInfo
public class ColumnChainInfo implements Cloneable {
    private List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();
    public boolean equals(Object o2)
    {
        if (!(o2 instanceof ColumnChainInfo))
            return false;
        ColumnChainInfo c2 = (ColumnChainInfo)o2;
        if (columnInfos.size()!=c2.columnInfos.size())
            return false;
        for (int i=0;i<columnInfos.size();i++)
        {
            if (!columnInfos.get(i).equals(c2.columnInfos.get(i)))
                return false;
        }
        return true;
    }
    public void insert(List<Integer> columns, byte type)
    {
        ColumnInfo newColumnInfo = new ColumnInfo(columns, type);
        columnInfos.add(newColumnInfo);
    }
    
    /**
     * Insert new ColumnInfo for a project-star or project-range-to-end
     * @param startCol
     * @param type
     */
    public void insert(int startCol, byte type)
    {
        ColumnInfo newColumnInfo = new ColumnInfo(startCol, type);
        columnInfos.add(newColumnInfo);
    }
    
    // In reduce, the input#1 represent the first input, put 0 instead of 1, so 
    // that we can match the sort information collected from POLocalRearrange
    public void insertInReduce(POProject project)
    {
        if (size()==0)
        {
            int col;
            if(project.isProjectToEnd() || project.getColumns().size() != 1){
                // expecting first project to be projecting one of the bags
                // so getting here is unexpected.
                // setting -1 as the column so that it secondary sort optimization
                // will not get used
                col = -1;
                return;
            }else{
                col = project.getColumns().get(0) - 1;
            }
            List<Integer> newColumns = new ArrayList<Integer>();
            newColumns.add(col);
            ColumnInfo newColumnInfo = new ColumnInfo(newColumns, project.getResultType());
            columnInfos.add(newColumnInfo);
        }
        else if (project.isProjectToEnd()){
            insert(project.getStartCol(), project.getResultType());
        }
        else {
            insert(project.getColumns(), project.getResultType());
        }
    }
    public void insertColumnChainInfo(ColumnChainInfo columnChainInfo)
    {
        columnInfos.addAll(columnChainInfo.columnInfos);
    }
    public List<ColumnInfo> getColumnInfos()
    {
        return columnInfos;
    }
    public String toString() {
        return columnInfos.toString();
    }
    @Override
    public Object clone() throws CloneNotSupportedException {
        super.clone();
        ColumnChainInfo result = new ColumnChainInfo();
        for (ColumnInfo columnInfo:columnInfos)
        {
            ColumnInfo newColumnInfo = (ColumnInfo) columnInfo.clone();
            result.columnInfos.add(newColumnInfo);
        }
        return result;
    }
    public int size() {
        return columnInfos.size();
    }
    public ColumnInfo getColumnInfo(int i) {
        return columnInfos.get(i);
    }
    @Override
    public int hashCode() {
        int result = 0;
        for (ColumnInfo columnInfo : columnInfos)
            result+=columnInfo.hashCode();
        return result;
    }
}
