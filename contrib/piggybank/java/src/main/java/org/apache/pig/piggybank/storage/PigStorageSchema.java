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

package org.apache.pig.piggybank.storage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreMetadata;
import org.apache.pig.builtin.PigStorage;

/**
 *  This Load/Store Func reads/writes metafiles that allow the schema and 
 *  aliases to be determined at load time, saving one from having to manually
 *  enter schemas for pig-generated datasets.  
 *  
 *  It also creates a ".pig_headers" file that simply lists the delimited aliases.
 *  This is intended to make export to tools that can read files with header
 *  lines easier (just cat the header to your data).
 *  
 *  Due to StoreFunc limitations, you can only write the metafiles in MapReduce 
 *  mode. You can read them in Local or MapReduce mode.
 */
public class PigStorageSchema extends PigStorage implements LoadMetadata, StoreMetadata {

    public PigStorageSchema() {
        super();
    }
    
    public PigStorageSchema(String delim) {
        super(delim);
    }
     
    //------------------------------------------------------------------------
    // Implementation of LoadMetaData interface
    
    @Override
    public ResourceSchema getSchema(String location,
            Job job) throws IOException {
        return (new JsonMetadata()).getSchema(location, job);
    }

    @Override
    public ResourceStatistics getStatistics(String location,
            Job job) throws IOException {        
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
            throws IOException { 
    }
    
    @Override
    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        return null;
    }

    //------------------------------------------------------------------------
    // Implementation of StoreMetadata

    @Override
    public void storeSchema(ResourceSchema schema, String location,
            Job job) throws IOException {
        JsonMetadata metadataWriter = new JsonMetadata();
        byte fieldDel = '\t';
        byte recordDel = '\n';
        metadataWriter.setFieldDel(fieldDel);
        metadataWriter.setRecordDel(recordDel);
        metadataWriter.storeSchema(schema, location, job);               
    }

    @Override
    public void storeStatistics(ResourceStatistics stats, String location,
            Job job) throws IOException {
        
    }
}
