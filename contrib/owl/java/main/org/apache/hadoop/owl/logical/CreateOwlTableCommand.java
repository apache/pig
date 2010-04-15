
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyListValueEntity;
import org.apache.hadoop.owl.entity.OwlSchemaEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.OwlTableKeyValueEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlKeyListValue;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlPartitionKey.IntervalFrequencyUnit;

public class CreateOwlTableCommand extends Command {

    public enum Type {
        BASIC  (101),
        COMPOSITE (102);

        public int type;
        private Type(int ctype){
            type = ctype;
        }

    };

    private CommandInfo info = null;
    private String schemaString = "";
    private String loader = null;

    protected Type type = null;
    protected List<String> dataSetUnion = null;

    public CreateOwlTableCommand() {
        this.noun = Noun.OWLTABLE;
        this.verb = Verb.CREATE;
        this.info = new CommandInfo();
    }

    @Override
    public void addPropertyKeyValue(String keyName, String value) throws OwlException {
        info.addPropertyKeyValue(keyName, value);
    }

    @Override
    public void addPartitionKey(String keyName, String keyType, String partitioningType, List<? extends Object> listValues,
            String intervalStart, Integer freq, IntervalFrequencyUnit freqUnit) throws OwlException{
        info.addPartitionKey(keyName, keyType, partitioningType, listValues, intervalStart, freq, freqUnit);
    }

    @Override
    public void addPropertyKey(String keyName, String keyType) throws OwlException{
        info.addPropertyKey(keyName, keyType);
    }

    @Override
    public void unionDataSet(String dsName) throws OwlException {
        setCompositeDataSetType();
        if (dataSetUnion == null){
            dataSetUnion = new ArrayList<String>();
        }
        dataSetUnion.add(dsName);
    }

    @Override
    public void setBasicDataSetType() throws OwlException {
        if (type != Type.COMPOSITE){
            type = Type.BASIC;
        }else{
            throw new OwlException(ErrorType.ERROR_BASIC_DATASET_OPERATION_ON_COMPOSITE_DATASET);
        }
    }

    @Override
    public void setCompositeDataSetType() throws OwlException {
        unimplementedMethod(); // composite types currently not implemented
        if (type != Type.BASIC){
            type = Type.COMPOSITE;
        }else{
            throw new OwlException(ErrorType.ERROR_COMPOSITE_DATASET_OPERATION_ON_BASIC_DATASET);
        }
    }

    @Override
    public void setParentDatabase(String databaseName){
        info.setParentDatabase(databaseName);
    }

    @Override
    public void setName(String name){
        info.setOwlTableName(name);
    }


    @Override
    public void addLoaderInformation(String loader) throws OwlException {
        this.loader = loader;
    }

    @Override
    public void addSchemaElement(String schemaString) throws OwlException {
        this.schemaString = schemaString;
    }

    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{

        OwlTableEntity owlTableToCreate = new OwlTableEntity();

        if( schemaString == null || schemaString.trim().length() == 0 ){
            throw new OwlException(ErrorType.NO_SCHEMA_PROVIDED);
        }
        // check for schema-consolidatability
        SchemaUtil.validateSchema(this.schemaString);

        owlTableToCreate.setDatabaseId(getBackendDatabaseId(backend, this.info.getDatabaseName()));

        owlTableToCreate.setName(this.info.getOwlTableName());

        List <PartitionKeyEntity> ptnKeyList = new ArrayList<PartitionKeyEntity>();

        for ( OwlPartitionKey ptnKey : this.info.getPartitionKeys() ){

            Integer freqUnit = ptnKey.getIntervalFrequencyUnit() == null ? null :
                Integer.valueOf(ptnKey.getIntervalFrequencyUnit().getCode());


            PartitionKeyEntity partitionEntity = new PartitionKeyEntity(
                    owlTableToCreate,
                    ptnKey.getLevel(),
                    ptnKey.getName(),
                    ptnKey.getDataType().getCode(),
                    ptnKey.getPartitioningType().getCode(),
                    ptnKey.getIntervalStart(),
                    ptnKey.getIntervalFrequency(),
                    freqUnit
            );

            //Copy list values if bounded list
            List<KeyListValueEntity> listValues = new ArrayList<KeyListValueEntity>();

            if( ptnKey.getListValues() != null ) {
                for(OwlKeyListValue listValue : ptnKey.getListValues()) {

                    if( listValue.getIntValue() != null && ptnKey.getDataType() != OwlKey.DataType.INT ) {
                        throw new OwlException(ErrorType.ERROR_INVALID_VALUE_DATATYPE, "Key <" + ptnKey.getName() + ">");
                    }

                    listValues.add(new KeyListValueEntity(listValue.getIntValue(), listValue.getStringValue()));
                }
            }
            partitionEntity.setListValues(listValues);

            ptnKeyList.add(partitionEntity);
        }

        owlTableToCreate.setPartitionKeys(ptnKeyList);

        Map<String, GlobalKeyEntity> globalKeysByName = getGlobalKeysByName(backend);

        List<PropertyKeyEntity> propKeyList = new ArrayList<PropertyKeyEntity>();
        for ( OwlPropertyKey propKey : this.info.getPropertyKeys() ){
            // we validate no duplication of property key name and global key name here and also on backend
            if (! globalKeysByName.containsKey(propKey.getName())){
                propKeyList.add(new PropertyKeyEntity(
                        owlTableToCreate,
                        propKey.getName(),
                        propKey.getDataType().getCode()
                ));
            }else{
                throw new OwlException (ErrorType.ERROR_DUPLICATED_RESOURCENAME, "Property key name ["+ propKey.getName() +"] is duplicated with existing global key name.");
            }
        }

        owlTableToCreate.setPropertyKeys(propKeyList);

        OwlSchemaEntity newSchema = new OwlSchemaEntity(schemaString);
        SchemaUtil.checkPartitionKeys(newSchema, owlTableToCreate, false);
        OwlSchemaEntity oSchema = backend.create(newSchema);
        owlTableToCreate.setSchemaId(oSchema.getId());

        if (this.loader != null){
            owlTableToCreate.setLoader(loader);
        }

        OwlTableEntity otable = backend.create(owlTableToCreate);

        Map <String,PropertyKeyEntity> propKeysByName = new HashMap<String,PropertyKeyEntity>();
        for (PropertyKeyEntity propKeyEntity : otable.getPropertyKeys()){
            propKeysByName.put(propKeyEntity.getName(), propKeyEntity);
        }

        List<OwlTableKeyValueEntity> keyValues = new ArrayList<OwlTableKeyValueEntity>();
        Map<String,String> propKeyValues = this.info.getPropertyKeyValues();

        for (Map.Entry<String, String> entry : propKeyValues.entrySet()){

            OwlTableKeyValueEntity okv = new OwlTableKeyValueEntity();
            okv.setOwlTable(otable);

            if ( globalKeysByName.containsKey(entry.getKey())){
                GlobalKeyEntity globalKey = globalKeysByName.get(entry.getKey());
                okv.setGlobalKeyId(globalKey.getId());
                setKeyValue(okv,globalKey, entry.getValue());
            } else if (propKeysByName.containsKey(entry.getKey())) {
                PropertyKeyEntity propKey = propKeysByName.get(entry.getKey());
                okv.setPropertyKeyId(propKey.getId());
                setKeyValue(okv,propKey, entry.getValue());
            } else {
                throw new OwlException(
                        ErrorType.ERROR_UNKNOWN_PROPERTY_KEY_TYPE,
                        "Key :[" + entry.getKey() + "]"
                );
            }

            keyValues.add(okv);
        }

        otable.setKeyValues(keyValues);
        backend.update(otable);

        return null;
    }

}
