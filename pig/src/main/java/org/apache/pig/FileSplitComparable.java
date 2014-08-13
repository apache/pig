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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.DataReaderWriter;

/**
 * This class represents a relative position in a file.  It records a filename
 * and an offset.  This allows Pig to order FileSplits.
 * @since Pig 0.7
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving // Since we haven't done outer join for merge join yet
public class FileSplitComparable implements WritableComparable<FileSplitComparable>, Serializable{

    private static final long serialVersionUID = 1L;
    protected String filename;
    protected Long offset;
    
    //need a default constructor to be able to de-serialize using just 
    // the Writable interface
    public FileSplitComparable(){}
    
    public FileSplitComparable(String fileName, long offset){
        this.filename = fileName;
        this.offset = offset;
    }


    @Override
    public int compareTo(FileSplitComparable other) {
        int rc = filename.compareTo(other.filename);
        if (rc == 0)
            rc = Long.signum(offset - other.offset);
        return rc;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        filename = (String) DataReaderWriter.readDatum(in);
        offset = (Long)DataReaderWriter.readDatum(in);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        DataReaderWriter.writeDatum(out, filename);
        DataReaderWriter.writeDatum(out, offset);
    }

    @Override
    public String toString(){
        return "FileName: '" + filename + "' Offset: " + offset; 
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((filename == null) ? 0 : filename.hashCode());
        result = prime * result + ((offset == null) ? 0 : offset.hashCode());
        return result;
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
        FileSplitComparable other = (FileSplitComparable) obj;
        if (filename == null) {
            if (other.filename != null)
                return false;
        } else if (!filename.equals(other.filename))
            return false;
        if (offset == null) {
            if (other.offset != null)
                return false;
        } else if (!offset.equals(other.offset))
            return false;
        return true;
    }
}
