
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

package org.apache.hadoop.owl.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.entity.DataElementEntity;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyValueEntity;
import org.apache.hadoop.owl.entity.OwlSchemaEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.PartitionEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.protocol.OwlObject;

public class PublishDataElementCommand extends Command {

    CommandInfo info = null;
    String schemaString = "";
    String loader = null;
    String location = null;

    PublishDataElementCommand() {
        this.noun = Noun.DATAELEMENT;
        this.verb = Verb.CREATE;
        info = new CommandInfo();
    }

    @Override
    public void setName(String name){
        info.setOwlTableName(name);
    }

    @Override
    public void setParentDatabase(String databaseName){
        info.setParentDatabase(databaseName);
    }

    @Override
    public void addPartitionKeyValue(String keyName, String value) throws OwlException {
        info.addPartitionKeyValue(keyName, value);
    }

    @Override
    public void addPropertyKeyValue(String keyName, String value) throws OwlException {
        info.addPropertyKeyValue(keyName,value);
    }

    @Override
    public void addSchemaElement(String schemaString) throws OwlException {
        this.schemaString = schemaString;
    }

    @Override
    public void addLoaderInformation(String loader) throws OwlException {
        this.loader = loader;
    }

    @Override
    public void setLocation(String location) throws OwlException {
        this.location = location;
    }

    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{

        if (schemaString == null || schemaString.trim().length() == 0){
            if (schemaString == null){
                LogHandler.getLogger("server").warn("[publish DE schema was null]");
            }else{
                LogHandler.getLogger("server").warn("[publish DE schema was empty] ["+this.schemaString+"]");
            }
            throw new OwlException(ErrorType.NO_SCHEMA_PROVIDED);
        }else{
            LogHandler.getLogger("server").warn("[publish DE schema] ["+this.schemaString+"]");
        }

        OwlTableEntity otable = getBackendOwlTable(backend, this.info.getDatabaseName(), this.info.getOwlTableName());
        int otableId = otable.getId();

        // check for schema-consolidatability
        SchemaUtil.validateSchema(this.schemaString);

        List<OwlSchemaEntity> owe = backend.find(OwlSchemaEntity.class, "id = " + otable.getSchemaId());

        String consolidatedSchema = SchemaUtil.consolidateSchema(owe.get(0).getSchemaString(backend), this.schemaString);

        if (! consolidatedSchema.equalsIgnoreCase(owe.get(0).getSchemaString(backend))){
            // we have a schema update - we update.
            //otable.setSchemaString(consolidatedSchema);
            OwlSchemaEntity oSchema = backend.create(new OwlSchemaEntity(consolidatedSchema));
            otable.setSchemaId(oSchema.getId());
            backend.update(otable);
        }

        if( otable.getPartitionKeys().size() == 0 ) {
            //Publish into a non partitioned owltable
            publishNonPartitionedTable(backend, otable);
            return null;
        }

        // prepare maps from level
        Map<Integer, PartitionKeyEntity> partitionKeysByLevel = getPartitionKeyMapByLevel(otable);

        int numLevels = partitionKeysByLevel.size();

        if (this.info.getPartitionKeyValues().size() != numLevels){
            throw new OwlException(
                    ErrorType.ERROR_MISMATCH_PARTITION_KEYVALUES,
                    "OwlTable requires ["+numLevels+"] keys, publish provided ["+this.info.getPartitionKeyValues().size()+"]"
            );
        }

        // follow the partition key-values specified, one-by-one, 
        // trying to query for existing partitions till one partition does not exist.
        // No property key value changes are made to these partitions - that requires a MODIFY PARTITION PROPERTY

        Map<String,String> ptnKeyValues = this.info.getPartitionKeyValues();

        int lastExistingPartitionLevel = 0;
        PartitionEntity lastParentPartition = getBackendDeepestExistingPartition(backend,otableId, partitionKeysByLevel, ptnKeyValues );

        if (lastParentPartition != null){
            lastExistingPartitionLevel = lastParentPartition.getPartitionLevel();
        }

        // create partitions not present yet, and set property keys as specified on them as they come along.

        if (lastExistingPartitionLevel == partitionKeysByLevel.size()){
            // leaf partition we want to create already exists 
            // that means a de has already been published here.
            // that's not allowed, the user should delete the partition (or DE), and redo the publish

            throw new OwlException(ErrorType.ERROR_DUPLICATE_LEAF_PARTITION);
        }

        while (lastExistingPartitionLevel < partitionKeysByLevel.size()){

            PartitionKeyEntity pke = partitionKeysByLevel.get(lastExistingPartitionLevel+1);
            if (!ptnKeyValues.containsKey(pke.getName())){
                throw new OwlException(
                        ErrorType.ERROR_MISMATCH_PARTITION_KEYVALUES,
                        "Partition key ["+pke.getName()+"] at level ["+pke.getPartitionLevel()+"] has no value assigned in input"
                );
            }

            PartitionEntity ptn = new PartitionEntity();
            ptn.setOwlTableId(otableId);
            if (lastParentPartition == null){
                ptn.setParentPartition(null);
            } else {
                ptn.setParentPartition(lastParentPartition.getId());
            }

            List<KeyValueEntity> keyValuesDefined = new ArrayList<KeyValueEntity>();

            keyValuesDefined.add(instantiatePartitionKeyValueEntityForPartition(ptn, pke, ptnKeyValues));

            if (lastExistingPartitionLevel == partitionKeysByLevel.size()-1){
                // This partition we're about to create is the last partition - we set all our defined propkeyvalues here

                Map<String,GlobalKeyEntity> globalKeysByName = getGlobalKeysByName(backend);
                Map<String,PropertyKeyEntity> propertyKeysByName = getPropertyKeyMapByName(otable);

                for (Map.Entry<String, String> entry : this.info.getPropertyKeyValues().entrySet()){
                    KeyValueEntity kve = new KeyValueEntity();
                    kve.setOwlTableId(ptn.getOwlTableId());
                    kve.setPartition(ptn);

                    if (globalKeysByName.containsKey(entry.getKey())){
                        GlobalKeyEntity gke = globalKeysByName.get(entry.getKey());
                        kve.setGlobalKeyId(gke.getId());
                        setKeyValue(kve, gke, entry.getValue());
                    } else if (propertyKeysByName.containsKey(entry.getKey())){
                        PropertyKeyEntity propKey = propertyKeysByName.get(entry.getKey());
                        kve.setPropertyKeyId(propKey.getId());
                        setKeyValue(kve,propKey, entry.getValue());
                    } else {
                        // unknown property key type - not a global key and not a regular property key?
                        throw new OwlException(
                                ErrorType.ERROR_UNKNOWN_PROPERTY_KEY_TYPE,
                                "Key :[" + entry.getKey() + "]"
                        );
                    }
                    keyValuesDefined.add(kve);
                }
            }

            ptn.setKeyValues(keyValuesDefined);


            lastParentPartition = backend.create(ptn);
            lastExistingPartitionLevel++;
        }
        // update last created partition

        // create the DataElement
        DataElementEntity de = new DataElementEntity();
        de.setLocation(this.location);
        de.setOwlTableId(otableId);
        de.setPartitionId(lastParentPartition.getId());
        de.setLoader(this.loader);

        SchemaUtil.validateSchema(this.schemaString);
        OwlSchemaEntity partitionSchema = backend.create(new OwlSchemaEntity(schemaString));
        //SchemaUtil.checkPartitionKeys(partitionSchema, otable, true);
        de.setSchemaId(new Integer(partitionSchema.getId()));

        backend.create(de);

        return null;
    }

    private void publishNonPartitionedTable(OwlBackend backend, OwlTableEntity otable) throws OwlException {

        List<DataElementEntity> deList = backend.find(
                DataElementEntity.class, "owlTableId = " + otable.getId());
        if( deList.size() != 0 ) {
            throw new OwlException(ErrorType.ERROR_DUPLICATE_PUBLISH);
        }

        // create the DataElement
        DataElementEntity de = new DataElementEntity();
        de.setLocation(location);
        de.setOwlTableId(otable.getId());
        de.setPartitionId(null);
        de.setLoader(loader);

        SchemaUtil.validateSchema(schemaString);
        OwlSchemaEntity partitionSchema = backend.create(new OwlSchemaEntity(schemaString));
        SchemaUtil.checkPartitionKeys(partitionSchema, otable, true);
        de.setSchemaId(new Integer(partitionSchema.getId()));

        backend.create(de);
    }

}
