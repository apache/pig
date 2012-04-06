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
import java.util.Properties;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreMetadata;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.parser.ParserException;
import org.apache.pig.impl.util.CastUtils;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

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

    private ResourceSchema schema;
    LoadCaster caster;

    public PigStorageSchema() {
        super();
    }
    
    public PigStorageSchema(String delim) {
        super(delim);
    }
     
    @Override
    public Tuple getNext() throws IOException {
        Tuple tup = super.getNext();
        if (tup == null) return null;

        if ( caster == null) {
            caster = getLoadCaster();

            if (signature != null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                      new String[] {signature});
                String serializedSchema = p.getProperty(signature+".schema");
                if (serializedSchema != null) {
                  try {
                      schema = new ResourceSchema(Utils.getSchemaFromString(serializedSchema));
                  } catch (ParserException e) {
                      mLog.error("Unable to parse serialized schema " + serializedSchema, e);
                  }
                }
            }
        }

        if (schema != null) {

            ResourceFieldSchema[] fieldSchemas = schema.getFields();
            int tupleIdx = 0;
            // If some fields have been projected out, the tuple
            // only contains required fields.
            // We walk the requiredColumns array to find required fields,
            // and cast those.
            for (int i = 0; i < fieldSchemas.length; i++) {
                if (mRequiredColumns == null || (mRequiredColumns.length>i && mRequiredColumns[i])) {
                    Object val = null;
                    if(tup.get(tupleIdx) != null){
                        byte[] bytes = ((DataByteArray) tup.get(tupleIdx)).get();
                        val = CastUtils.convertToType(caster, bytes,
                                fieldSchemas[i], fieldSchemas[i].getType());
                    }
                    tup.set(tupleIdx, val);
                    tupleIdx++;
                }
            }
        }
        return tup;
    }

    //------------------------------------------------------------------------
    // Implementation of LoadMetaData interface
    
    @Override
    public ResourceSchema getSchema(String location,
            Job job) throws IOException {
        schema = (new JsonMetadata()).getSchema(location, job);
        if (signature != null && schema != null) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                    new String[] {signature});
            p.setProperty(signature + ".schema", schema.toString());
    }
        return schema;
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
