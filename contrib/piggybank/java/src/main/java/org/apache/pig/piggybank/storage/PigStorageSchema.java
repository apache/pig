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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.ExecType;
import org.apache.pig.experimental.JsonMetadata;
import org.apache.pig.experimental.LoadMetadata;
import org.apache.pig.experimental.StoreMetadata;
import org.apache.pig.experimental.ResourceSchema;
import org.apache.pig.experimental.ResourceStatistics;
import org.apache.pig.StoreConfig;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

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
public class PigStorageSchema extends PigStorage implements StoreMetadata {

    private static final Log log = LogFactory.getLog(PigStorageSchema.class);

    public PigStorageSchema() {
        super();
    }
    
    public PigStorageSchema(String delim) {
        super(delim);
    }
    
    @Override
    public Schema determineSchema(String fileName, ExecType execType,
            DataStorage storage) throws IOException {

        // TODO fullPath should be retrieved ia relativeToAbsolutePath once PIG-966 is complete
        String fullPath = FileLocalizer.fullPath(fileName, storage);
        LoadMetadata metadataLoader = new JsonMetadata(fullPath, storage);
        ResourceSchema resourceSchema = metadataLoader.getSchema(fullPath, null);
        if (resourceSchema == null) {
            return null;
        }
        Schema pigSchema = new Schema();
        for (ResourceSchema.ResourceFieldSchema field : resourceSchema.getFields()) {
            FieldSchema pigFieldSchema = DataType.determineFieldSchema(field);
            // determineFieldSchema only sets the types. we also want the aliases.
            // TODO this doesn't work properly for complex types
            pigFieldSchema.alias = field.getName();
            pigSchema.add(pigFieldSchema);
        }
        log.info("Loaded Schema: "+pigSchema);
        return pigSchema;
    }

    @Override
    public void finish() throws IOException {
        super.finish();
        JobConf jobConf = PigMapReduce.sJobConf;
        if(jobConf != null){
            StoreConfig storeConfig = MapRedUtil.getStoreConfig(jobConf);
            DataStorage store = new HDataStorage(ConfigurationUtil.toProperties(jobConf));
            Schema schema = storeConfig.getSchema();
            ResourceSchema resourceSchema = new ResourceSchema(schema);
            JsonMetadata metadataWriter = new JsonMetadata(storeConfig.getLocation(), store);
            metadataWriter.setFieldDel(fieldDel);
            metadataWriter.setRecordDel(recordDel);
            metadataWriter.setSchema(resourceSchema, storeConfig.getLocation(), null);
        }
    }

    /**
     * @see org.apache.pig.experimental.StoreMetadata#setSchema(ResourceSchema)
     * Does not do anything in this implementation. The finish() call writes the schema.
     */
    @Override
    public void setSchema(ResourceSchema schema, String location, Configuration conf) throws IOException {
        // n\a
    }

    /**
     * @see org.apache.pig.experimental.StoreMetadata#setStatistics(ResourceStatistics)
     * Does not do anything in this implementation.
     */
    @Override
    public void setStatistics(ResourceStatistics stats, String location, Configuration conf) throws IOException {
        // n\a
    }
}
