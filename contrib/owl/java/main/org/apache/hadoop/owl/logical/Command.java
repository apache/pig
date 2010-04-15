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
import java.util.TreeMap;

import org.apache.hadoop.owl.backend.BackendUtil;
import org.apache.hadoop.owl.backend.IntervalUtil;
import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.entity.DatabaseEntity;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyBaseEntity;
import org.apache.hadoop.owl.entity.KeyValueEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.OwlTableKeyValueEntity;
import org.apache.hadoop.owl.entity.PartitionEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.logical.CommandInfo.FilterString;
import org.apache.hadoop.owl.logical.CommandInfo.Operator;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartitionKey.IntervalFrequencyUnit;

public class Command {

    public enum Noun {
        OWLTABLE  ("OWLTABLE"),
        DATAELEMENT ("DATAELEMENT"), 
        DATABASE ("DATABASE"), 
        OBJECTS ("OBJECTS"), 
        GLOBALKEY ("GLOBALKEY");

        public String type;
        private Noun(String ntype){
            this.type = ntype;
        }
        public Noun fromString(String ntype){
            this.type = ntype;
            return this;
        }

    }

    protected static final String DEFAULT_OWNER = null;
    protected static final String DEFAULT_DESCRIPTION = null;

    Verb verb;
    Noun noun;

    public Verb getCommandType(){
        return verb;
    }

    public Noun getCommandResource(){
        return noun;
    }

    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{
        unimplementedMethod();
        return null; // dummy return.
    }


    protected void unimplementedMethod() throws OwlException{
        // default impl template, needs to be overridden
        throw new OwlException(ErrorType.ERROR_UNIMPLEMENTED);
        /*
         *  We call this from methods rather than keeping them abstract because 
         *  the parser will use a Command as its primary interface,
         *  so all callable functions must be valid at compile time,
         *  but result in a runtime exception if a method undefined 
         *  for a concrete command type is called at runtime
         */
    }

    public void setName(String name) throws OwlException {
        unimplementedMethod();
    }

    public void setParentDatabase(String databaseName) throws OwlException {
        unimplementedMethod();
    }

    public void inDatabase(String databaseName) throws OwlException {
        unimplementedMethod();
    }

    public void addPartitionKeyValue(String keyName, String value)
    throws OwlException {
        unimplementedMethod();
    }

    public void addPropertyKeyValue(String keyName, String value)
    throws OwlException {
        unimplementedMethod();
    }

    public void addPartitionFilter(String keyName, String operator, String value)
    throws OwlException {
        unimplementedMethod();
    }

    public void addPropertyFilter(String keyName, String operator, String value)
    throws OwlException {
        unimplementedMethod();
    }

    public void setExternalStorageIdentifier(String externalStorageIdentifier)
    throws OwlException {
        unimplementedMethod();
    }

    public void setCompositeDataSetType() throws OwlException {
        unimplementedMethod();
    }

    public void setBasicDataSetType() throws OwlException {
        unimplementedMethod();
    }

    //The Filter string for the query
    public void setFilter(ExpressionTree filter) throws OwlException {
        unimplementedMethod();
    }

    public void likeTables(String likeTableNameStr) throws OwlException {
        unimplementedMethod();
    }


    //The partition level to fetch
    public void setPartitionLevel(String partitionKey) throws OwlException {
        unimplementedMethod();
    }

    public void unionDataSet(String dsName) throws OwlException {
        unimplementedMethod();
    }

    public void addPropertyKey(String keyName, String keyType)
    throws OwlException {
        unimplementedMethod();
    }

    public void addPartitionKey(String keyName, String keyType,
            String partitioningType, List<? extends Object> listValues,
            String intervalStart, Integer freq, IntervalFrequencyUnit freqUnit) throws OwlException {
        unimplementedMethod();
    }

    public void addSchemaElement(String schema) throws OwlException {
        unimplementedMethod();
    }

    public void addLoaderInformation(String loader) throws OwlException {
        unimplementedMethod();
    }

    public void addAdditionalActionInfo(String additionalAction)
    throws OwlException {
        unimplementedMethod();
    }

    public void setLocation(String location) throws OwlException {
        unimplementedMethod();
    }

    public void inPropertyKeySet(String partitionName) throws OwlException {
        unimplementedMethod();
    }

    public CommandInfo getCommandInfo() throws OwlException{
        unimplementedMethod();
        return null;
    }

    // protected helper methods follow

    protected GlobalKeyEntity getBackendGlobalKey(OwlBackend backend, String globalKeyName) throws OwlException {
        List<GlobalKeyEntity> globalKeyList = backend.find(GlobalKeyEntity.class, "name = \""+ globalKeyName + "\"");
        if (globalKeyList.size() != 1){
            throw new OwlException(ErrorType.ERROR_UNKNOWN_GLOBAL_KEY,
                    Integer.toString(globalKeyList.size()) + " matches for "+ globalKeyName
            );
        } else {
            return globalKeyList.get(0);
        }
    }

    protected DatabaseEntity getBackendDatabase(OwlBackend backend, String databaseName) throws OwlException{
        List<DatabaseEntity> parentDatabaseList = backend.find(DatabaseEntity.class, "name = \"" + databaseName + "\"");
        if (parentDatabaseList.size() != 1){
            throw new OwlException(ErrorType.ERROR_UNKNOWN_DATABASE,
                    Integer.toString(parentDatabaseList.size()) + " matches for "+databaseName
            );
        } else {
            return parentDatabaseList.get(0);
        }
    }

    protected int getBackendDatabaseId(OwlBackend backend, String databaseName) throws OwlException{
        return getBackendDatabase(backend,databaseName).getId();
    }

    protected OwlTableEntity getBackendOwlTable(OwlBackend backend, int databaseId, String owlTableName) throws OwlException {
        List<OwlTableEntity> owlTableList = backend.find(
                OwlTableEntity.class,
                "databaseId = "+ databaseId +" and name = \"" + owlTableName + "\""
        );
        if (owlTableList.size() != 1){
            throw new OwlException(ErrorType.ERROR_UNKNOWN_OWLTABLE,
                    Integer.toString(owlTableList.size()) + " matches for "+owlTableName+" in database "+ databaseId
            );
        } else {
            return owlTableList.get(0);
        }           
    }

    protected OwlTableEntity getBackendOwlTable(OwlBackend backend, String databaseName, String owlTableName) throws OwlException{
        try {
            return getBackendOwlTable(backend,getBackendDatabaseId(backend, databaseName),owlTableName);
        } catch (OwlException e){
            if (e.getErrorType() == ErrorType.ERROR_UNKNOWN_OWLTABLE){
                OwlException translatedException = new OwlException(
                        ErrorType.ERROR_UNKNOWN_OWLTABLE,
                        "OwlTable " + owlTableName + " in database " + databaseName,
                        e
                );
                throw translatedException;
            }else{
                throw e;
            }
        }
    }

    protected int getBackendOwlTableId(OwlBackend backend, String databaseName, String owlTableName) throws OwlException {
        return getBackendOwlTable(backend,databaseName,owlTableName).getId();
    }

    protected int getBackendOwlTableId(OwlBackend backend, int databaseId, String owlTableName) throws OwlException {
        return getBackendOwlTable(backend,databaseId,owlTableName).getId();
    }

    protected KeyValueEntity instantiatePartitionKeyValueEntityForPartition(
            PartitionEntity partition,
            PartitionKeyEntity partitionKey, 
            Map<String, String> ptnKeyValues) 
    throws OwlException {
        KeyValueEntity partitionKeyValueEntity = new KeyValueEntity();
        partitionKeyValueEntity.setOwlTableId(partition.getOwlTableId());
        partitionKeyValueEntity.setPartition(partition);
        partitionKeyValueEntity.setPartitionKeyId(partitionKey.getId());

        // add current step ptn key-value to filter
        setKeyValue(partitionKeyValueEntity, partitionKey, ptnKeyValues.get(partitionKey.getName()));
        return partitionKeyValueEntity;
    }

    protected Map<String, PropertyKeyEntity> getPropertyKeyMapByName(OwlTableEntity otable) {
        Map<String,PropertyKeyEntity> propertyKeysByName = new HashMap<String,PropertyKeyEntity>();
        for (PropertyKeyEntity propke : otable.getPropertyKeys()){
            propertyKeysByName.put(propke.getName(), propke);
        }
        return propertyKeysByName;
    }

    protected Map<Integer, PartitionKeyEntity> getPartitionKeyMapByLevel(OwlTableEntity otable) {
        //        System.out.println(
        //                "JIMI:getPartitionKeyMapByLevel:otable["+otable.getName()
        //                +"],otableId:["+otable.getId()+"], databaseId:["+otable.getDatabaseId()+"]"
        //                );
        Map<Integer,PartitionKeyEntity> partitionKeyByLevel = new TreeMap<Integer,PartitionKeyEntity>();
        for (PartitionKeyEntity pke : otable.getPartitionKeys()){
            partitionKeyByLevel.put(pke.getPartitionLevel(), pke);
            //            System.out.println(
            //                    "JIMI:getPartitionKeyMapByLevel: pkey name["+pke.getName()
            //                    +"],datatype[" + pke.getDataType() + 
            //                    "],partitioningType["+ pke.getPartitioningType() + 
            //                    "],partitionLevel["+pke.getPartitionLevel() + "]"
            //                    );
        }
        return partitionKeyByLevel;
    }

    protected PartitionEntity getBackendDeepestExistingPartition(
            OwlBackend backend, int otableId,
            Map<Integer, PartitionKeyEntity> partitionKeyByLevel,
            Map<String, String> ptnKeyValues) 
    throws OwlException {

        PartitionEntity lastParentPartition = null;

        //        System.out.println("JIMI:partitionKeyByLevel.size():"+partitionKeyByLevel.size()); //debug

        for(int level = 1; level < partitionKeyByLevel.size()+1; level++){

            //            System.out.println("JIMI:level:"+level); // debug
            PartitionKeyEntity pke = partitionKeyByLevel.get(level);

            //            if (pke == null){ // debug
            //                System.out.println("JIMI:pkey was null"); // debug
            //            } else { // debug
            //                System.out.println("JIMI:pkey.getName():"+pke.getName()); // debug
            //                System.out.println("JIMI:pkey.getId():"+pke.getId());
            //            } // debug

            if (!ptnKeyValues.containsKey(pke.getName())){
                throw new OwlException(
                        ErrorType.ERROR_MISMATCH_PARTITION_KEYVALUES,
                        "Partition key ["+pke.getName()+"] at level ["+pke.getPartitionLevel()+"] has no value assigned in input"
                );
            }

            String filter = "owlTableId = " + otableId; 

            // add current step ptn key-value to filter            
            filter += getSelectionFilter(pke, ptnKeyValues.get(pke.getName()));

            // add parentPartition clause
            if (lastParentPartition != null){
                filter += " and parentPartitionId = "+lastParentPartition.getId();
            }else{
                filter += " and parentPartitionId is null";
            }

            List<PartitionEntity> partitions = backend.find(PartitionEntity.class, filter); // should find only one matching entry

            if (partitions.size() == 0){
                break;
            } else {
                lastParentPartition = partitions.get(0);
            }
        }
        return lastParentPartition;
    }

    protected String getSelectionFilter(KeyBaseEntity pke, String value) throws OwlException {
        String filter = " and [" + pke.getName() + "] = ";
        if (pke.getDataType() == OwlKey.DataType.STRING.getCode()){
            filter += "\"" + value + "\"";
        } else if (pke.getDataType() == OwlKey.DataType.INT.getCode()){
            filter += value;
        } else if (pke.getDataType() == OwlKey.DataType.LONG.getCode()){
            filter += IntervalUtil.getIntervalOffset((PartitionKeyEntity) pke, value);
        } else {
            unimplementedMethod(); // unknown datatype - other datatypes need impl after specification
        }
        return filter;
    }


    protected void setKeyValue(KeyValueEntity kve,KeyBaseEntity key, String valueToSet) throws OwlException {
        if (key.getDataType() == OwlKey.DataType.STRING.getCode()){
            System.out.println("JIMIs setting ["+key.getName()+"] to ["+valueToSet+"] as String");
            kve.setStringValue(valueToSet);
        } else if (key.getDataType() == OwlKey.DataType.INT.getCode()){
            System.out.println("JIMIi setting ["+key.getName()+"] to ["+valueToSet+"] as Integer");
            kve.setIntValue(Integer.valueOf(valueToSet));
        } else if (key.getDataType() == OwlKey.DataType.LONG.getCode()){
            kve.setIntValue(Integer.valueOf(
                    IntervalUtil.getIntervalOffset((PartitionKeyEntity) key, valueToSet)));
        } 
        else {
            unimplementedMethod(); // unknown datatype - other datatypes need impl after specification
        } 
    }

    protected void setKeyValue(OwlTableKeyValueEntity kve,KeyBaseEntity key, String valueToSet) throws OwlException {
        if (key.getDataType() == OwlKey.DataType.STRING.getCode()){
            System.out.println("JIMIs setting ["+key.getName()+"] to ["+valueToSet+"] as String");
            kve.setStringValue(valueToSet);
        } else if (key.getDataType() == OwlKey.DataType.INT.getCode()){
            System.out.println("JIMIi setting ["+key.getName()+"] to ["+valueToSet+"] as Integer");
            kve.setIntValue(Integer.valueOf(valueToSet).intValue());
        } else {
            unimplementedMethod(); // unknown datatype - other datatypes need impl after specification
        } 
    }


    // get partitions from PartitionFilters
    protected List<PartitionEntity> getPartitionsFromPartitionFilters(
            OwlBackend backend, OwlTableEntity otable,
            Map<String, FilterString> ptnFilters,
            boolean getTopLevelOnly) throws OwlException {
        int otableId = otable.getId();

        // get partition keys from otable
        Map<Integer, PartitionKeyEntity> partitionKeyByLevel = getPartitionKeyMapByLevel(otable);

        int maxLevelPossibleForOwlTable = partitionKeyByLevel.size();

        int ptnLevel = ptnFilters.size();

        System.out.println(
                "JIMI Looking for partition at level ["+ptnLevel+"] whereas possible max["+maxLevelPossibleForOwlTable+"] keys defined for the owltable, "
        );

        if (ptnLevel > maxLevelPossibleForOwlTable){
            throw new OwlException(
                    ErrorType.ERROR_MISMATCH_PARTITION_KEYVALUES,
                    "Looking for partition at level ["+ptnLevel+"] whereas possible max["+maxLevelPossibleForOwlTable+"] keys defined for the owltable, "
            );
        }

        // construct filter from partition keys and ptnKeyValue pairs from Command

        StringBuffer filter = new StringBuffer("owlTableId = " + otableId); 

        for(int level = 1; level < ptnLevel+1; level++){
            // add current step ptn key-operator-value to filter            
            PartitionKeyEntity pke = partitionKeyByLevel.get(level);
            if (!ptnFilters.containsKey(pke.getName())){
                throw new OwlException(
                        ErrorType.ERROR_MISMATCH_PARTITION_KEYVALUES,
                        "Partition key ["+pke.getName()+"] at level ["+pke.getPartitionLevel()+"] has no value assigned in input"
                );
            }
            filter.append(getFilterForKey(pke, ptnFilters.get(pke.getName())));
        }

        //Check if only top level partition for given keys is required. Otherwise all partitions will be returned
        if( getTopLevelOnly ) { //
            filter.append(" and partitionLevel ="+ ptnLevel);
        }

        List<PartitionEntity> partitions = backend.find(PartitionEntity.class, filter.toString());
        if (partitions.size() == 0){
            throw new OwlException(
                    ErrorType.ERROR_UNKNOWN_DATAELEMENT_PARTITION,
                    "matched [" + partitions.size() + "] matches."
            );
        }
        return partitions;
    }

    protected Map<String, GlobalKeyEntity> getGlobalKeysByName(OwlBackend backend)
    throws OwlException {
        List<GlobalKeyEntity> globalKeys = backend.find(GlobalKeyEntity.class, null);
        Map<String,GlobalKeyEntity> globalKeysByName = new HashMap<String,GlobalKeyEntity>();
        for (GlobalKeyEntity gkey : globalKeys){
            globalKeysByName.put(gkey.getName(),gkey);
        }
        return globalKeysByName;
    }

    /**
     * Generates the search filter for given key values.
     * 
     * @param backend the backend instance
     * @param owlTable the owl table entity
     * @param partitionFilters the partition key value filter map
     * @param propertyFilters the property key/global key value filter map
     * @param globalKeys the list of global keys
     * @param leafLevel return only leaf level partitions
     * @param subtree return the subtree matching the filter
     * @return the generated filter
     * 
     * @throws OwlException the owl exception
     */
    protected String generateFilterForKeys(OwlBackend backend,
            OwlTableEntity owlTable,
            Map<String, FilterString> partitionFilters,
            Map<String, FilterString> propertyFilters,
            List<GlobalKeyEntity> globalKeys,
            boolean leafLevel,
            boolean subtree) throws OwlException {

        StringBuffer filter = new StringBuffer("owlTableId = " + owlTable.getId());

        if( leafLevel ) {
            filter.append(" and isLeaf = 'Y' ");
        }

        int maxPartitionLevel = 0;

        for(Map.Entry<String, FilterString> entry : partitionFilters.entrySet()) {
            KeyBaseEntity key = BackendUtil.getKeyForName(entry.getKey(), owlTable, globalKeys);

            //Partition key names should be given in the partition key list only
            if(! (key instanceof PartitionKeyEntity) ) {
                throw new OwlException(ErrorType.ERROR_UNKNOWN_PARTITION_KEY, entry.getKey());
            }

            if( ((PartitionKeyEntity) key).getPartitionLevel() > maxPartitionLevel ) {
                maxPartitionLevel = ((PartitionKeyEntity) key).getPartitionLevel();
            }

            filter.append(getFilterForKey(key, entry.getValue()));
        }

        for(Map.Entry<String, FilterString> entry : propertyFilters.entrySet()) {
            KeyBaseEntity key = BackendUtil.getKeyForName(entry.getKey(), owlTable, globalKeys);

            //Property key names should be given in the property key list only
            if(! (key instanceof PropertyKeyEntity || key instanceof GlobalKeyEntity) ) {
                throw new OwlException(ErrorType.ERROR_UNKNOWN_PROPERTY_KEY_TYPE, entry.getKey());
            }

            filter.append(getFilterForKey(key, entry.getValue()));
        }

        //If leafLevel is set, partitionLevel is not set in filter
        //If leafLevel is not set and subtree is not set, then partitionLevel is set if
        //at least one partition key is specified in the query
        if( (! leafLevel) && (! subtree) && maxPartitionLevel > 0 ) {
            filter.append(" and partitionLevel = " + maxPartitionLevel);
        }

        LogHandler.getLogger("server").debug("Filter " + filter);
        return filter.toString();
    }

    /**
     * Gets the filter string for specified key and value.
     * 
     * @param key the key
     * @param operatorValue the operator and value
     * @return the filter for key
     * @throws OwlException the owl exception
     */
    public static String getFilterForKey(KeyBaseEntity key, FilterString operatorValue) throws OwlException {

        validateFilterOperator(key, operatorValue.getFilterOperator());

        Operator operator = getFilterForOperator(key, operatorValue.getFilterOperator(), operatorValue.getFilterValue());
        StringBuffer filter = new StringBuffer(" and [" + key.getName() + "] " + operator.getOp());

        String valueString = getFilterForValue(key, operatorValue.getFilterOperator(), operatorValue.getFilterValue());
        filter.append(valueString);

        return filter.toString();
    }


    public static Operator getFilterForOperator(KeyBaseEntity key, Operator operator, String value) throws OwlException {

        if (key.getDataType() == OwlKey.DataType.LONG.getCode()) {
            //For interval datatype, > is converted to >= in the backend filter.
            // < is converted to <= unless the time specified is an exact interval start time.
            //These are required to ensure that valid partitions do not get pruned.
            if( operator == Operator.GREATERTHAN ) {
                operator = Operator.GREATERTHANOREQUALTO;
            } else if( operator == Operator.LESSTHAN ) {
                if(! IntervalUtil.isIntervalStart((PartitionKeyEntity) key, value) ) {
                    operator = Operator.LESSTHANOREQUALTO;
                }
            }
        }

        return operator;
    }

    public static String getFilterForValue(KeyBaseEntity key, Operator operator, String value) throws OwlException {
        switch(OwlKey.DataType.fromCode(key.getDataType())) {
        case STRING:
            if( operator == Operator.IN ) {
                return value; 
            } else {
                return "\"" + value + "\"";
            }

        case INT:
            return value;

        case LONG:
            return String.valueOf(IntervalUtil.getIntervalOffset((PartitionKeyEntity) key, value)); 
        }

        return null;
    }


    /**
     * Validate whether specified filter operator is valid for the give keys data type.
     * 
     * @param key the key
     * @param operator the operator
     * @throws OwlException the owl exception
     */
    public static void validateFilterOperator(KeyBaseEntity key, Operator operator) throws OwlException {

        DataType type = OwlKey.DataType.fromCode(key.getDataType());

        switch(type) {
        case STRING:
            if( operator == Operator.LIKE || operator == Operator.EQUAL || operator == Operator.GREATERTHAN || 
                    operator == Operator.GREATERTHANOREQUALTO  || operator == Operator.LESSTHAN ||
                    operator == Operator.LESSTHANOREQUALTO || operator == Operator.IN ||
                    operator == Operator.NOTEQUALS ) {
                return;
            }
            break;

        case INT:
            if( operator == Operator.EQUAL  || operator == Operator.GREATERTHAN || operator == Operator.GREATERTHANOREQUALTO 
                    || operator == Operator.LESSTHAN || operator == Operator.LESSTHANOREQUALTO || operator == Operator.IN 
                    || operator == Operator.NOTEQUALS ) {
                return;
            }
            break;

        case LONG:
            if( operator == Operator.EQUAL  || operator == Operator.GREATERTHAN || operator == Operator.GREATERTHANOREQUALTO 
                    || operator == Operator.LESSTHAN || operator == Operator.LESSTHANOREQUALTO 
                    || operator == Operator.NOTEQUALS ) {
                return;
            }
            break;
        }

        throw new OwlException(ErrorType.INVALID_FILTER_OPERATOR, "Key <" + key.getName() +
                "> type <" + type + "> operation <" + operator.getOp() + ">");
    }

}
