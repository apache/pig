
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
import org.apache.hadoop.owl.entity.DataElementEntity;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyValueEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;
import org.apache.hadoop.owl.entity.OwlSchemaEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.OwlTableKeyValueEntity;
import org.apache.hadoop.owl.entity.PartitionEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.orm.OwlEntityManager;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;

public class AlterOwlTableCommand extends Command {

    private enum AlterType {

        MODIFY_PARTITION_PROPERTY ("MODIFY PARTITION PROPERTY"),
        DROP_PARTITION_PROPERTY ("DROP PARTITION PROPERTY"),
        DROP_PARTITION ("DROP PARTITION"),
        MODIFY_DATA_ELEMENT ("MODIFY DATA ELEMENT"),
        DELETE_DATA_ELEMENT ("DELETE DATA ELEMENT"), 
        ADD_PROPERTY_KEY ("ADD PROPERTY KEY"),
        SET_SCHEMA ("SET SCHEMA");

        private String action;

        private AlterType(String alterType){
            this.action = alterType;
        }

        public String getAlterType() {
            return action;
        }

        public static AlterType fromAction(String inputAction) throws OwlException {
            for (AlterType t : AlterType.values()) {
                if (t.getAlterType().equalsIgnoreCase(inputAction)) {
                    return t;
                }
            }

            throw new OwlException(ErrorType.ERROR_UNKNOWN_ENUM_TYPE, "Type [" + inputAction + "] for " + AlterType.class.getSimpleName());
        }
    };

    CommandInfo info = null;
    AlterType action = null;

    String schemaString = "";


    AlterOwlTableCommand() {
        this.noun = Noun.OWLTABLE;
        this.verb = Verb.UPDATE;
        this.info = new CommandInfo();
    }

    @Override
    public void setName(String name){
        this.info.setOwlTableName(name);
    }

    @Override
    public void setParentDatabase(String databaseName){
        this.info.setParentDatabase(databaseName);
    }

    @Override
    public void addPartitionKeyValue(String keyName, String value) throws OwlException {
        this.info.addPartitionKeyValue(keyName, value);
    }

    @Override
    public void addPropertyKeyValue(String keyName, String value) throws OwlException {
        this.info.addPropertyKeyValue(keyName, value);
    }

    @Override
    public void addPartitionFilter(String keyName, String operator, String value) throws OwlException {
        info.addPartitionFilter(keyName, operator, value);
    }

    @Override
    public void addPropertyFilter(String keyName, String operator, String value) throws OwlException {
        info.addPropertyFilter(keyName, operator, value);
    }

    @Override
    public void addSchemaElement(String schema) throws OwlException {
        this.schemaString = schema;
    }

    @Override
    public CommandInfo getCommandInfo(){
        return info;
    }

    @Override
    public void addAdditionalActionInfo(String additionalAction) throws OwlException {
        this.action = AlterType.fromAction(additionalAction);
    }

    @Override
    public void addPropertyKey(String keyName, String keyType) throws OwlException {
        this.info.addPropertyKey(keyName, keyType);
    }

    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException {
        if ((this.action == AlterType.DELETE_DATA_ELEMENT) || (this.action == AlterType.DROP_PARTITION)){
            return executeDropPartition(backend);
        } else if(this.action == AlterType.ADD_PROPERTY_KEY){
            return executeAddPropertyKey(backend);
        } else if (action == AlterType.SET_SCHEMA ){
            return executeSetSchema(backend);
        } else {
            return executeModifyPartitionProperty(backend);
        }
    }

    public List<? extends OwlObject> executeModifyPartitionProperty(OwlBackend backend) throws OwlException {

        OwlTableEntity otable = getBackendOwlTable(backend, this.info.getDatabaseName(), this.info.getOwlTableName());
        int otableId = otable.getId();

        Map<String,GlobalKeyEntity> globalKeysByName = getGlobalKeysByName(backend);
        Map<String,PropertyKeyEntity> propertyKeysByName = getPropertyKeyMapByName(otable);

        Map<Integer,PropertyKeyEntity> propertyKeysToModifyValuesById = new HashMap<Integer,PropertyKeyEntity>();
        Map<Integer,GlobalKeyEntity> globalKeysToModifyValuesById = new HashMap<Integer,GlobalKeyEntity>();

        for (String keyName : this.info.getPropertyKeyValues().keySet()){
            if (globalKeysByName.containsKey(keyName)){
                GlobalKeyEntity globalKey = globalKeysByName.get(keyName);
                globalKeysToModifyValuesById.put(globalKey.getId(), globalKey);
            } else if (propertyKeysByName.containsKey(keyName)){
                PropertyKeyEntity propKey = propertyKeysByName.get(keyName);
                propertyKeysToModifyValuesById.put(propKey.getId(),propKey);
            } else {
                throw new OwlException(
                        ErrorType.ERROR_UNKNOWN_PROPERTY_KEY,
                        "No property key or global key ["+keyName+"] in owlTable ["+otable.getName()+"]"
                );
            }
        }

        if (this.info.ptnKeyFilters.size() == 0){
            // No PartitionKeyValues specified => operation at OwlTable level

            List<OwlTableKeyValueEntity> keyValues = otable.getKeyValues();
            List<OwlTableKeyValueEntity> modifiedKeyValues = new ArrayList<OwlTableKeyValueEntity>();
            List<OwlTableKeyValueEntity> keyValuesToPurge = new ArrayList<OwlTableKeyValueEntity>();

            Map<Integer,PropertyKeyEntity> propKeysToModify = new HashMap<Integer,PropertyKeyEntity>(propertyKeysToModifyValuesById);
            Map<Integer,GlobalKeyEntity> globalKeysToModify = new HashMap<Integer,GlobalKeyEntity>(globalKeysToModifyValuesById);

            for (int i = 0 ; i < keyValues.size() ; i++){

                OwlTableKeyValueEntity kve = keyValues.get(i);

                System.out.println("JIMIx kve non null for ["+i+"/"+keyValues.size()+"]");

                if ((kve.getPropertyKeyId() != null) && propKeysToModify.containsKey(kve.getPropertyKeyId())){
                    // This property key value pair is being modified in this operation
                    PropertyKeyEntity propKey = propKeysToModify.get(kve.getPropertyKeyId());
                    if ((this.action == AlterType.MODIFY_PARTITION_PROPERTY)
                            ||(this.action == AlterType.MODIFY_DATA_ELEMENT)) {

                        // TODO: code cleanup : instead of copying the kv, we should be editing the base kv itself
                        // Otherwise, it's also acceptable to do this if we make sure that the new kv does not copy the old one's id
                        OwlTableKeyValueEntity copiedKV = new OwlTableKeyValueEntity(kve);
                        // Currently, given that this copy copies the id, ambiguity can result if we try to delete the old and persist the new

                        setKeyValue(copiedKV, propKey, this.info.getPropertyKeyValues().get(propKey.getName()));
                        System.out.println("167");
                        modifiedKeyValues.add(copiedKV);
                        keyValuesToPurge.add(kve);
                        propKeysToModify.remove(propKey.getId());
                    } else if (this.action == AlterType.DROP_PARTITION_PROPERTY) {
                        propKeysToModify.remove(propKey.getId()); // drop it, we don't add it to the new list.
                        keyValuesToPurge.add(kve);
                    } else {
                        unimplementedMethod();
                    }
                }else if ((kve.getGlobalKeyId() != null) && globalKeysToModify.containsKey(kve.getGlobalKeyId())){
                    // This global key value pair is being modified in this operation
                    GlobalKeyEntity globalKey = globalKeysToModify.get(kve.getGlobalKeyId());
                    if ((this.action == AlterType.MODIFY_PARTITION_PROPERTY)
                            ||(this.action == AlterType.MODIFY_DATA_ELEMENT)) {

                        // TODO: code cleanup : instead of copying the kv, we should be editing the base kv itself
                        // Otherwise, it's also acceptable to do this if we make sure that the new kv does not copy the old one's id
                        OwlTableKeyValueEntity copiedKV = new OwlTableKeyValueEntity(kve);
                        // Currently, given that this copy copies the id, ambiguity can result if we try to delete the old and persist the new

                        setKeyValue(copiedKV, globalKey, this.info.getPropertyKeyValues().get(globalKey.getName()));
                        System.out.println("181");
                        modifiedKeyValues.add(copiedKV);
                        keyValuesToPurge.add(kve);
                        globalKeysToModify.remove(globalKey.getId());
                    } else if (this.action == AlterType.DROP_PARTITION_PROPERTY) {
                        globalKeysToModify.remove(globalKey.getId()); // drop it, we don't add it to the new list.
                        keyValuesToPurge.add(kve);
                    } else {
                        unimplementedMethod();
                    }
                }else{
                    // Do nothing - this keyvalue is not being modified in this operation
                }
            }

            // if we still have entries in propKeysToModify, we need to create it, if we've been asked to modify ptn property
            if (((this.action == AlterType.MODIFY_PARTITION_PROPERTY)
                    ||(this.action == AlterType.MODIFY_DATA_ELEMENT))
            ){
                for (PropertyKeyEntity propKey : propKeysToModify.values()){
                    OwlTableKeyValueEntity kve = new OwlTableKeyValueEntity();
                    kve.setOwlTable(otable);
                    kve.setPropertyKeyId(propKey.getId());
                    setKeyValue(kve, propKey, this.info.getPropertyKeyValues().get(propKey.getName()));
                    System.out.println("205");
                    modifiedKeyValues.add(kve);
                }
                for (GlobalKeyEntity globalKey : globalKeysToModify.values()){
                    OwlTableKeyValueEntity kve = new OwlTableKeyValueEntity();
                    kve.setOwlTable(otable);
                    kve.setGlobalKeyId(globalKey.getId());
                    setKeyValue(kve, globalKey, this.info.getPropertyKeyValues().get(globalKey.getName()));
                    System.out.println("213");
                    modifiedKeyValues.add(kve);
                }
            }

            for (OwlTableKeyValueEntity mkv : modifiedKeyValues){
                System.out.println(
                        "FELA UP"
                        + " otable["+ mkv.getOwlTable().getId()
                        + "] propKeyId[" + nullGuardedPrint(mkv.getPropertyKeyId())
                        + "] globalKeyId[" + nullGuardedPrint(mkv.getGlobalKeyId())
                        + "] stringValue[" + nullGuardedPrint(mkv.getStringValue())
                        + "] intValue[" + nullGuardedPrint(mkv.getIntValue())
                        + "]"
                );
            }
            for (OwlTableKeyValueEntity kv : keyValuesToPurge){
                System.out.println(
                        "FELA DOWN"
                        + " id["+ kv.getId() 
                        + "] otable["+ kv.getOwlTable().getId()
                        + "] propKeyId[" + nullGuardedPrint(kv.getPropertyKeyId())
                        + "] globalKeyId[" + nullGuardedPrint(kv.getGlobalKeyId())
                        + "] stringValue[" + nullGuardedPrint(kv.getStringValue())
                        + "] intValue[" + nullGuardedPrint(kv.getIntValue())
                        + "]"
                );
            }

            System.out.println("FELA: number of entries in modifiedKeyValues["+modifiedKeyValues.size()+"]");
            System.out.println("FELA: number of entries in keyValuesToPurge["+keyValuesToPurge.size()+"]");
            // update to backend

            for (OwlTableKeyValueEntity kv : keyValuesToPurge){
                otable.removeKeyValuePair(kv);
                backend.deleteDependantEntity(OwlTableEntity.class, kv); // explicitly remove the old keyvalue
            }

            for (OwlTableKeyValueEntity kv : modifiedKeyValues){
                otable.addKeyValuePair(kv);
            }
            backend.update(otable);

        } else {
            List<PartitionEntity> ptnsToModify = getPartitionsFromPartitionFilters(
                    backend,otable,this.info.ptnKeyFilters, false
            );

            //Loop across all matching partitions, apply changes
            for( PartitionEntity ptnToModify : ptnsToModify ) {

                Map<Integer,PropertyKeyEntity> propKeysToModify = new HashMap<Integer,PropertyKeyEntity>(propertyKeysToModifyValuesById);
                Map<Integer,GlobalKeyEntity> globalKeysToModify = new HashMap<Integer,GlobalKeyEntity>(globalKeysToModifyValuesById);

                List<KeyValueEntity> keyValues = ptnToModify.getKeyValues();
                List<KeyValueEntity> modifiedKeyValues = new ArrayList<KeyValueEntity>();

                for (int i = 0 ; i < keyValues.size() ; i++){

                    KeyValueEntity kve = keyValues.get(i);

                    System.out.println("JIMIx kve non null for ["+i+"/"+keyValues.size()+"]");

                    if ((kve.getPropertyKeyId() != null) && propKeysToModify.containsKey(kve.getPropertyKeyId())){
                        PropertyKeyEntity propKey = propKeysToModify.get(kve.getPropertyKeyId());
                        if ((this.action == AlterType.MODIFY_PARTITION_PROPERTY)
                                ||(this.action == AlterType.MODIFY_DATA_ELEMENT)) {
                            KeyValueEntity copiedKV = new KeyValueEntity(kve);
                            setKeyValue(copiedKV, propKey, this.info.getPropertyKeyValues().get(propKey.getName()));
                            System.out.println("248");
                            modifiedKeyValues.add(copiedKV);
                            propKeysToModify.remove(propKey.getId());
                        } else if (this.action == AlterType.DROP_PARTITION_PROPERTY) {
                            propKeysToModify.remove(propKey.getId()); // drop it, we don't add it to the new list.
                        } else {
                            unimplementedMethod();
                        }
                    } else if ((kve.getGlobalKeyId() != null) && globalKeysToModify.containsKey(kve.getGlobalKeyId())){
                        GlobalKeyEntity globalKey = globalKeysToModify.get(kve.getGlobalKeyId());
                        if ((this.action == AlterType.MODIFY_PARTITION_PROPERTY)
                                ||(this.action == AlterType.MODIFY_DATA_ELEMENT)) {
                            KeyValueEntity copiedKV = new KeyValueEntity(kve);
                            setKeyValue(copiedKV, globalKey, this.info.getPropertyKeyValues().get(globalKey.getName()));
                            System.out.println("262");
                            modifiedKeyValues.add(copiedKV);
                            globalKeysToModify.remove(globalKey.getId());
                        } else if (this.action == AlterType.DROP_PARTITION_PROPERTY) {
                            globalKeysToModify.remove(globalKey.getId()); // drop it, we don't add it to the new list.
                        } else {
                            unimplementedMethod();
                        }
                    } else {
                        //New key value list needs to have the key value entities inserted into it as-is
                        KeyValueEntity copiedKV = new KeyValueEntity(kve);
                        modifiedKeyValues.add(copiedKV);
                    }
                }

                // if we still have entries in propKeysToModify, we need to create it, if we've been asked to modify ptn property
                if (((this.action == AlterType.MODIFY_PARTITION_PROPERTY)
                        ||(this.action == AlterType.MODIFY_DATA_ELEMENT))
                ){
                    for (PropertyKeyEntity propKey : propKeysToModify.values()){
                        KeyValueEntity kve = new KeyValueEntity();
                        kve.setOwlTableId(otableId);
                        kve.setPartition(ptnToModify);
                        kve.setPropertyKeyId(propKey.getId());
                        setKeyValue(kve, propKey, this.info.getPropertyKeyValues().get(propKey.getName()));
                        System.out.println("287");
                        modifiedKeyValues.add(kve);
                    }
                    for (GlobalKeyEntity globalKey : globalKeysToModify.values()){
                        KeyValueEntity kve = new KeyValueEntity();
                        kve.setOwlTableId(otableId);
                        kve.setPartition(ptnToModify);
                        kve.setGlobalKeyId(globalKey.getId());
                        setKeyValue(kve, globalKey, this.info.getPropertyKeyValues().get(globalKey.getName()));
                        System.out.println("296");
                        modifiedKeyValues.add(kve);
                    }
                }

                for ( KeyValueEntity mkv : modifiedKeyValues){
                    System.out.println(
                            "JIMIy"
                            + " otable["+ mkv.getOwlTableId()
                            + "] ptn[" + mkv.getPartition().getId()
                            + "] ptnKeyId[" + nullGuardedPrint(mkv.getPartitionKeyId())
                            + "] propKeyId[" + nullGuardedPrint(mkv.getPropertyKeyId())
                            + "] globalKeyId[" + nullGuardedPrint(mkv.getGlobalKeyId())
                            + "] stringValue[" + nullGuardedPrint(mkv.getStringValue())
                            + "] intValue[" + nullGuardedPrint(mkv.getIntValue())
                            + "]"
                    );
                }

                // update to backend
                ptnToModify.setKeyValues(modifiedKeyValues);
                backend.update(ptnToModify);

            }

        }

        return null;
    }

    private String nullGuardedPrint(Object o) {
        if (o != null) {
            return o.toString();
        }else{
            return "null";
        }
    }

    public List<? extends OwlObject> executeDropPartition(OwlBackend backend) throws OwlException {

        OwlTableEntity otable = getBackendOwlTable(backend,
                this.info.getDatabaseName(),
                this.info.getOwlTableName());

        if( otable.getPartitionKeys().size() == 0) {
            //Non-partitioned owltable, delete all data elements
            backend.delete(DataElementEntity.class, "owlTableId = " + otable.getId());
            return null;
        }

        List<PartitionEntity> ptnsToDrop = getPartitionsFromPartitionFilters(backend, otable, info.ptnKeyFilters, true);

        for(PartitionEntity partition : ptnsToDrop) {
            backend.delete(PartitionEntity.class, "id = "+ partition.getId());
        }

        return null;
    }

    public List<? extends OwlObject> executeAddPropertyKey(OwlBackend backend) throws OwlException {

        OwlTableEntity otable = getBackendOwlTable(backend,
                this.info.getDatabaseName(),
                this.info.getOwlTableName());

        Map<String, GlobalKeyEntity> globalKeysByName = getGlobalKeysByName(backend);

        List<PropertyKeyEntity> propKeyList = otable.getPropertyKeys();
        //new ArrayList<PropertyKeyEntity>();
        for ( OwlPropertyKey propKey : this.info.getPropertyKeys() ){
            PropertyKeyEntity propKeyEntity = 
                new PropertyKeyEntity(
                        otable,
                        propKey.getName(),
                        propKey.getDataType().getCode());
            if (! globalKeysByName.containsKey(propKey.getName())) {
                for ( PropertyKeyEntity pke : propKeyList){
                    if ((pke.getName()).equals(propKey.getName()) ){
                        throw new OwlException(ErrorType.ERROR_DUPLICATE_PROPERTY_KEY, "Property key name is " + propKey.getName());
                    }
                }
                propKeyList.add(propKeyEntity);
            }
        }

        otable.setPropertyKeys(propKeyList);

        backend.update(otable);
        return null;

    }

    @SuppressWarnings("boxing")
    private List<? extends OwlObject> executeSetSchema(
            OwlBackend backend) throws OwlException {

        OwlTableEntity otable = getBackendOwlTable(backend,
                this.info.getDatabaseName(),
                this.info.getOwlTableName());

        SchemaUtil.validateSchema(schemaString);
        int currentSchemaId = otable.getSchemaId();
        List<OwlSchemaEntity> owe = backend.find(OwlSchemaEntity.class,
                "id = " + currentSchemaId);

        SchemaUtil.consolidateSchema(
                owe.get(0).getSchemaString(backend), schemaString);

        //If consolidate does not throw exception, update the table level schema
        //to the given schema (not to the consolidated schema)
        OwlSchemaEntity newSchema = new OwlSchemaEntity(schemaString);

        //Add partitioning keys to the schema if not present
        SchemaUtil.checkPartitionKeys(newSchema, otable, false);

        OwlSchemaEntity oSchema = backend.create(newSchema);
        otable.setSchemaId(oSchema.getId());
        backend.update(otable);

        //Delete the previous schema (currently partitions do not share schema with table)
        backend.delete(OwlSchemaEntity.class, "id = " + currentSchemaId);

        return null;
    }

    private PropertyKeyEntity findBackendPropertyKeyEntity(
            OwlBackend backend, OwlTableEntity otable, String keyName) throws OwlException {
        for (PropertyKeyEntity propKey : otable.getPropertyKeys()){
            if (propKey.getName().equalsIgnoreCase(keyName)){
                return propKey;
            }
        }
        // if we reached here, we found no matches. Throw exception.
        throw new OwlException(
                ErrorType.ERROR_UNKNOWN_PROPERTY_KEY,
                "No property key ["+keyName+"] in owlTable ["+otable.getName()+"]"
        );
    }

}
