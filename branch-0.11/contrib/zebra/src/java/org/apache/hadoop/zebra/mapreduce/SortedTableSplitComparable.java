/**
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

package org.apache.hadoop.zebra.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.data.DataReaderWriter;

public class SortedTableSplitComparable implements WritableComparable<SortedTableSplitComparable> {
    private static final long serialVersionUID = 1L;
    
    protected Integer index;
    
    //need a default constructor to be able to de-serialize using just 
    // the Writable interface
    public SortedTableSplitComparable(){}
    
    public SortedTableSplitComparable(int index){
        this.index = index;
    }


    @Override
    public int compareTo(SortedTableSplitComparable other) {
        return Integer.signum( index - other.index );
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        index = (Integer)DataReaderWriter.readDatum(in);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        DataReaderWriter.writeDatum(out, index);
    }

    @Override
    public String toString(){
        return "Index = " + index ; 
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return index;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SortedTableSplitComparable other = (SortedTableSplitComparable) obj;
        return this.index.intValue() == other.index.intValue();
    }
}
