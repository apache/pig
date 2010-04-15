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
package org.apache.hadoop.owl.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlSchema;

/** The OwlSplit wrapper around the InputSplit returned by the underlying InputFormat */ 
class OwlSplit extends InputSplit implements Writable {

    /** The partition info for the split. */
    private OwlPartitionInfo partitionInfo;

    /** The split returned by the underlying InputFormat split. */
    private InputSplit baseSplit;

    /** The schema for the OwlTable */
    private OwlSchema tableSchema;
    /**
     * Instantiates a new owl split.
     */
    public OwlSplit() {
    }

    /**
     * Instantiates a new owl split.
     * 
     * @param partitionInfo the partition info
     * @param baseSplit the base split
     * @param tableSchema the owl table level schema
     */
    public OwlSplit(OwlPartitionInfo partitionInfo, InputSplit baseSplit, OwlSchema tableSchema) {
        this.partitionInfo = partitionInfo;
        this.baseSplit = baseSplit;
        this.tableSchema = tableSchema;
    }

    /**
     * Gets the partition info.
     * @return the partitionInfo
     */
    public OwlPartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    /**
     * Gets the underlying InputSplit.
     * @return the baseSplit
     */
    public InputSplit getBaseSplit() {
        return baseSplit;
    }

    /**
     * Sets the table schema.
     * @param tableSchema the new table schema
     */
    public void setTableSchema(OwlSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    /**
     * Gets the table schema.
     * @return the table schema
     */
    public OwlSchema getTableSchema() {
        return tableSchema;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
        return baseSplit.getLength();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.InputSplit#getLocations()
     */
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return baseSplit.getLocations();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput input) throws IOException {
        String partitionInfoString = WritableUtils.readString(input);
        partitionInfo = (OwlPartitionInfo) SerializeUtil.deserialize(partitionInfoString);

        String baseSplitClassName = WritableUtils.readString(input);
        InputSplit split;
        try{
            Class<? extends InputSplit> splitClass = 
                (Class<? extends InputSplit>) Class.forName(baseSplitClassName);

            //Class.forName().newInstance() does not work if the underlying 
            //InputSplit has package visibility 
            Constructor<? extends InputSplit> constructor = 
                splitClass.getDeclaredConstructor(new Class[]{});
            constructor.setAccessible(true);

            split = constructor.newInstance();
            // read baseSplit from input
            ((Writable)split).readFields(input);
            this.baseSplit = split;
        }catch(Exception e){          
            throw new OwlException (ErrorType.ERROR_CREATE_INPUT_SPLIT,
                    baseSplitClassName, e);
        }

        String tableSchemaString = WritableUtils.readString(input);
        tableSchema = (OwlSchema) SerializeUtil.deserialize(tableSchemaString);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput output) throws IOException {
        String partitionInfoString = SerializeUtil.serialize(partitionInfo);

        // write partitionInfo into output
        WritableUtils.writeString(output, partitionInfoString);

        WritableUtils.writeString(output, baseSplit.getClass().getName());
        Writable baseSplitWritable = (Writable)baseSplit;
        //write  baseSplit into output
        baseSplitWritable.write(output);

        //write the table schema into output
        String tableSchemaString = SerializeUtil.serialize(tableSchema);
        WritableUtils.writeString(output, tableSchemaString);
    }
}
