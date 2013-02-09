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

// Represent a set of sort keys. All the sort keys are equivalent. 
// Eg:
// C = cogroup A by ($0, $1), B by ($0, $1)
// This query have two sort keys, (A.$0, A.$1) and (B.$0, B.$1)
public class SortKeyInfo {
    private List<ColumnChainInfo> columnChains = new ArrayList<ColumnChainInfo>();
    private List<Boolean> ascs = new ArrayList<Boolean>();
    
    public boolean moreSpecificThan(Object o2)
    {
        if (!(o2 instanceof SortKeyInfo))
            return false;
        SortKeyInfo s2 = (SortKeyInfo)o2;
        
        if (columnChains.size()<s2.columnChains.size())
            return false;
        for (int i=0;i<columnChains.size();i++)
        {
            if (i<s2.columnChains.size() && (!(columnChains.get(i).equals(s2.columnChains.get(i))) ||
                    !ascs.get(i).equals(s2.ascs.get(i))))
                return false;
        }
        return true;
    }
    
    public void insertColumnChainInfo(int index, ColumnChainInfo columnChainInfo, boolean asc)
    {
        if (columnChains.size()<=index)
        {
            columnChains.add(columnChainInfo);
            ascs.add(asc);
            return;
        }
        
        ColumnChainInfo chain = columnChains.get(index);
        chain.insertColumnChainInfo(columnChainInfo);
        ascs.set(index, asc);
    }
    public String toString() {
        return columnChains.toString();
    }
    
    public List<ColumnChainInfo> getColumnChains() {
        return columnChains;
    }
    
    public boolean[] getAscs() {
        boolean[] result = new boolean[ascs.size()];
        for (int i=0;i<ascs.size();i++)
            result[i] = ascs.get(i);
        return result;
    }
}
