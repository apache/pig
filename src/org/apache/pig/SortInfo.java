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
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.util.Utils;

/**
 * Class to communicate sort column information based on 
 * order by statment's sort columns and schema
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SortInfo implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    boolean isGloballySorted = true; // in pig this is the default
    
    List<SortColInfo> sortColInfoList;
    
    /**
     * @param sortColInfoList list of sortColInfo, one for each field in the data
     */
    public SortInfo(List<SortColInfo> sortColInfoList){
        this.sortColInfoList = sortColInfoList;
    }

    /**
     * @return the sortColInfoList the list of sortColInfo for this data
     */
    public List<SortColInfo> getSortColInfoList() {
        return new ArrayList<SortColInfo>(sortColInfoList);
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31; 
        int result = 1;
        result = prime * result + ((sortColInfoList == null) ? 0 : 
            sortColInfoList.hashCode());
        result = prime * result + (isGloballySorted ? 1: 0);
        return result;
    }   

    /**
     * @return the isGloballySorted true if the data is globally sorted, false if it is sorted
     * only within each part file.
     */
    public boolean isGloballySorted() {
        return isGloballySorted;
    }

    
        /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if(!Utils.checkNullAndClass(this, obj)) {
            return false;
        }
        SortInfo other = (SortInfo)obj;
        return (
            isGloballySorted == other.isGloballySorted &&
            Utils.checkNullEquals(sortColInfoList, other.sortColInfoList, true));
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "GlobalSort:" + isGloballySorted +", sort column info list:" + sortColInfoList;
    }
    


}
