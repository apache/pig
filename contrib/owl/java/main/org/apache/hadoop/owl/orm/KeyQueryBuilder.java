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

package org.apache.hadoop.owl.orm;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.OwlEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.protocol.OwlKey;


/**
 * This class implements the functionality for transforming filters which have keys into the equivalent query
 * doing join with the key table. buildQuery is the main interface which returns two strings, the from clause
 * and where clause.
 */
public class KeyQueryBuilder {

    /** The entity being queries for, these are the entities for which key filter is supported */
    private enum QueriedEntity {
        OWLTABLE,
        PARTITION
    }

    private static class KeyInfo {
        int keyId; //The key id for abc after name resolution
        OwlKey.KeyType keyType; //The type of key
        OwlKey.DataType dataType; //The data type for the key

        /**
         * Instantiates a new key info.
         * 
         * @param keyId
         *            the key id
         * @param keyType
         *            the key type
         * @param dataType
         *            the data type
         */
        public KeyInfo (int keyId, OwlKey.KeyType keyType, OwlKey.DataType dataType) {
            this.keyId = keyId;
            this.keyType = keyType;
            this.dataType = dataType;
        }
    }

    /**
     * Class used to store information read after parsing the user supplied filter
     */
    private static class KeyParseInfo {
        int startIndex; //The start index of [abc]
        int endIndex; //the end index of [abc]
        String name; //the key name abc
        KeyInfo keyInfo; //Info about the key type

        /**
         * Instantiates a new key parse info object.
         * 
         * @param startIndex
         *            the start index
         * @param endIndex
         *            the end index
         * @param alias
         *            the alias
         * @param keyInfo
         *            the key info
         */
        public KeyParseInfo(int startIndex, int endIndex, String name, KeyInfo keyInfo) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.name = name;
            this.keyInfo = keyInfo;
        }
    }

    /** The entity being queries for */
    private QueriedEntity queriedEntity;

    /** The base entity manager. */
    private OwlEntityManager<? extends OwlEntity> baseEntityManager;

    /** Cached parent owl table. */
    private OwlTableEntity owlTable;

    /** The entity on which the query is working. */
    private String entityName;

    /** Regex for spaces. */
    private final static String spaces = "[\\s]*";

    /** The key regex, within square brackets, used for matching  key value pairs */
    private final static String key = "(\\[([\\w:\\.\\-_]+)\\])";

    /** The key operator regex. */
    private final static String keyOperator = "([=><]|<>|>=|<=|IN|NOT " + spaces + "IN|LIKE)";

    /** Regex for equals, used for matching id (only equality supported) */
    private final static String equals = spaces + "=" + spaces;

    /** The regex for id value. */
    private final static String idValue = "([\\d]+)";

    /** The  regex, the pattern for matching  key value pair, either "[abc] >" or "> [abc]"*/
    private final static String keyExpr = 
        "(" +
        key + spaces + keyOperator + 
        ")|(" +
        keyOperator + spaces + key +
        ")";

    /** The static Pattern for Key value regex */ 
    private final static Pattern keyValuePattern = Pattern.compile(keyExpr, Pattern.CASE_INSENSITIVE);

    /** The static Pattern for id value regex */ 
    private final static Pattern idPattern = Pattern.compile(getIdValueRegex("id"));

    /** The static Pattern for owl table id regex */ 
    private final static Pattern owlTableIdPattern = Pattern.compile(getIdValueRegex("owlTableId"));

    /**
     * Instantiates a new key query builder.
     * 
     * @param baseEntityManager
     *            the base entity manager, for query on partition, this should be the partition entity manager
     * @param entityName
     *            the name of the resource being queried for
     * @throws OwlException 
     */
    KeyQueryBuilder(
            OwlEntityManager<? extends OwlEntity> baseEntityManager,
            String entityName) throws OwlException {
        this.baseEntityManager = baseEntityManager;
        this.entityName = entityName;

        if( entityName.equals("OwlTableEntity") ) {
            this.queriedEntity = QueriedEntity.OWLTABLE;
        } else if( entityName.equals("PartitionEntity") ) {
            this.queriedEntity = QueriedEntity.PARTITION;
        } else {
            throw new OwlException(ErrorType.ERROR_UNRECOGNIZED_RESOURCE_TYPE, entityName);
        }
    }


    /**
     * Parses the filter and generates a parse info list containing the position and names of all the key operations.
     * 
     * @param inputString
     *            the input string
     * 
     * @return the list of key parse info
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public List<KeyParseInfo> parseForKeys(String inputString) throws OwlException {

        List<KeyParseInfo>  parseInfo = new ArrayList<KeyParseInfo>();

        //Match for the "[abc] >" or "> [abc]" part
        Matcher m = keyValuePattern.matcher(inputString);

        //Loop through all matching key patterns, saving them in the parse info list
        while( m.find() ) {

            //For "[abc] > 10", "[abc]" will be group 2 and "abc" will be group 3
            //For "10 > [abc]", "[abc]" will be group 7 and "abc" will be group 8
            int KeyGroup = 3;
            String keyName = m.group(KeyGroup);

            if( keyName == null || keyName.trim().length() == 0) {
                //Must be in "10 > [abc]" format
                KeyGroup = 8;
                keyName = m.group(KeyGroup);
            }

            keyName = keyName.trim();

            //Save the begin and end index of second group [abc], including the brackets, so that it can be substituted later
            //(KeyGroup - 1) is the group for "[abc]"
            int startIndex = m.start(KeyGroup - 1);
            int endIndex = m.end(KeyGroup - 1);

            //Add the info to the parse info list
            KeyParseInfo info = new KeyParseInfo(startIndex, endIndex, keyName, null);
            parseInfo.add(info);
        }

        return parseInfo;
    }

    /**
     * Gets the id value regex.
     * @param idFieldName the id field name
     * @return the regex string
     */
    private static String getIdValueRegex(String idFieldName) {
        return 
        "(" + 
        idFieldName + equals + idValue +
        ")|(" +
        idValue + equals + idFieldName +
        ")";
    }

    /**
     * Parses the filter to get the value of id.
     * @param inputString the input string
     * @param idFieldName the id field name
     * @param exactMatch should an exact match be performed
     * @return the value of id, -1 if no id found
     * @throws OwlException the meta data exception
     */
    public static int parseId(String inputString, String idFieldName, boolean exactMatch) throws OwlException {

        Pattern pattern = null;
        inputString = inputString.trim();

        if( "id".equals(idFieldName) ) {
            pattern = idPattern;
        } else if( "owlTableId".equals(idFieldName)) {
            pattern = owlTableIdPattern;
        } else {
            pattern = Pattern.compile(getIdValueRegex(idFieldName));
        }

        Matcher m = pattern.matcher(inputString);
        int parentId = -1;

        if( m.find() ) {

            if( m.group(1) == null || m.group(1).length() != inputString.length() ) {
                if( exactMatch )  {
                    //For exact match, the pattern differs from input
                    return -1;
                }
            }

            //For "parentId = 10", "10" is group 2
            //For "10 = parentId", "10" is group 4
            String parentIdString = m.group(2);
            if( parentIdString == null || parentIdString.trim().length() == 0) {
                //Must be in "10 = parentId" format
                parentIdString = m.group(4);
            }

            try {
                parentId = Integer.parseInt(parentIdString);
            } catch(NumberFormatException nfe) {
                throw new OwlException(
                        ErrorType.INVALID_PARENTID_VALUE,
                        parentIdString,
                        nfe);
            }
        }

        return parentId;
    }


    /**
     * Resolve the key name.
     * 
     * @param owlTableId
     *            the owl table id
     * @param keyName
     *            the key name
     * 
     * @return the KeyInfo for the resolved key, throws exception if not found
     * 
     * @throws OwlException the meta data exception
     */
    public KeyInfo resolveKey(int owlTableId, String keyName) throws OwlException {

        if( owlTable == null ) {
            OwlEntityManager<OwlTableEntity> em = OwlEntityManager.createEntityManager(OwlTableEntity.class, baseEntityManager);
            owlTable = em.fetchById(owlTableId);

            if( owlTable == null ) {
                throw new OwlException(ErrorType.ERROR_INVALID_OWLTABLE, "id = " + owlTableId);
            }
        }

        //Check if specified name is a partition key name (only for queries for Partition entities */
        if( owlTable.getPartitionKeys() != null ) {
            for(PartitionKeyEntity partKey : owlTable.getPartitionKeys()) {
                if( partKey.getName().equals(keyName)) {
                    KeyInfo keyInfo = new KeyInfo(partKey.getId(), OwlKey.KeyType.PARTITION, OwlKey.DataType.fromCode(partKey.getDataType()));
                    return keyInfo;
                }
            }
        }

        //Check if specified name is a property key name
        if( owlTable.getPropertyKeys() != null ) {
            for(PropertyKeyEntity propKey : owlTable.getPropertyKeys()) {
                if( propKey.getName().equals(keyName)) {
                    return new KeyInfo(propKey.getId(), OwlKey.KeyType.PROPERTY, OwlKey.DataType.fromCode(propKey.getDataType()));
                }
            }
        }

        //Check if specified name is a global key name
        OwlEntityManager<GlobalKeyEntity> em = OwlEntityManager.createEntityManager(GlobalKeyEntity.class, baseEntityManager);
        List<GlobalKeyEntity> globalKeys = em.fetchByFilter(null);

        for(GlobalKeyEntity globalKey : globalKeys) {
            if( globalKey.getName().equals(keyName)) {
                KeyInfo keyInfo = new KeyInfo(globalKey.getId(), OwlKey.KeyType.GLOBAL, OwlKey.DataType.fromCode(globalKey.getDataType()));
                return keyInfo;
            }
        }

        return null;
    }


    /**
     * Update key type info.
     * 
     * @param parentId
     *            the parent id
     * @param parseList
     *            the parse list
     * 
     * @throws OwlException
     *             the owl exception
     */
    private void updateKeyTypeInfo(int parentId, List<KeyParseInfo> parseList) throws OwlException {
        for(KeyParseInfo info : parseList) {
            KeyInfo keyInfo = resolveKey(parentId, info.name);
            if( keyInfo == null ) {
                throw new OwlException(ErrorType.ERROR_INVALID_KEY_NAME, info.name);
            }
            info.keyInfo = keyInfo;
        }
    }


    /**
     * Generate from clause for the key query.
     * 
     * @param parseList
     *            the parse list
     * 
     * @return the from clause
     * 
     * @throws OwlException
     *             the meta data exception
     */
    String generateFromClause(List<KeyParseInfo> parseList) throws OwlException {
        StringBuffer buf = new StringBuffer(30);

        buf.append(entityName + " " + "entity");

        //Do joins between the entity and the keyentity as many times as there are key operations in the filter
        for(int i = 0;i < parseList.size();i++) {
            String keyName = "k" + (i + 1);

            buf.append(", ");

            if( queriedEntity == QueriedEntity.PARTITION ) {
                buf.append("KeyValueEntity " + keyName);
            } else if( queriedEntity == QueriedEntity.OWLTABLE ) {
                buf.append("OwlTableKeyValueEntity " + keyName);
            }
        }

        return buf.toString();
    }


    /**
     * Generate where clause for the the key query.
     * 
     * @param parseList
     *            the parse list
     * @param filter
     *            the filter
     * @param queriedObject
     *            the queried object
     * 
     * @return the where string
     * 
     * @throws OwlException
     *             the meta data exception
     */
    String generateWhereClause(List<KeyParseInfo> parseList, String filter, String queriedObject) throws OwlException {
        StringBuffer outputBuffer = new StringBuffer(filter);

        //Go backwards in the list, replace all the [abc] occurrences with equivalent key value check
        for(int i = parseList.size() - 1;i >= 0;i--) {
            StringBuffer buf = new StringBuffer(100);
            String keyName = "k" + (i + 1);
            KeyParseInfo info = parseList.get(i);

            if( info.keyInfo.dataType == OwlKey.DataType.INT || 
                    info.keyInfo.dataType == OwlKey.DataType.LONG ) {
                buf.append(keyName + ".intValue");
            } else {
                buf.append(keyName + ".stringValue");
            }   

            //Update the main buffer and replace [abc] with proper key value check
            outputBuffer.replace(info.startIndex, info.endIndex, buf.toString());
        }

        //Put user supplied filter within brackets
        outputBuffer.insert(0, "(");
        outputBuffer.append(")");

        //Add the join condition and the key type check
        for(int i = 0;i < parseList.size();i++) {
            String keyName = "k" + (i + 1);
            KeyParseInfo info = parseList.get(i);

            outputBuffer.append(" AND " + keyName + "." + queriedObject + " = entity" );

            switch( info.keyInfo.keyType ) {
            case PARTITION :
                outputBuffer.append(" AND " + keyName + ".partitionKeyId = " + info.keyInfo.keyId);
                break;
            case PROPERTY :
                outputBuffer.append(" AND " + keyName + ".propertyKeyId = " + info.keyInfo.keyId);
                break;
            case GLOBAL :
                outputBuffer.append(" AND " + keyName + ".globalKeyId = " + info.keyInfo.keyId);
                break;
            }
        }

        return outputBuffer.toString();
    }

    /**
     * Builds the query for the use provided filter.
     * 
     * @param filter
     *            the filter
     * @param queriedObject
     *            the queried object, for Partition it is partition (the field name within KeyValue)
     * 
     * @return array of two string, first being the from clause and second being the where clause
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public String[] buildQuery(String filter, String queriedObject) throws OwlException {

        String[] returnArray = { null, null };

        if( filter == null ) {
            //null values are interpreted as defaults in OwlEntityManager.fetchByFilter
            return returnArray;
        }

        //Get parent id
        int parentId = parseId(filter, "owlTableId", false);

        //Get list of key operations in the filter 
        List<KeyParseInfo> parseList = parseForKeys(filter);

        if( parseList == null || parseList.size() == 0 ) {
            //there are no keys in the filter, return the filter as is, the from clause being null is interpreted as a single table query
            returnArray[0] = null;
            returnArray[1] = filter;
            return returnArray;
        }

        //Do unaliasing if required, get the key type information
        updateKeyTypeInfo(parentId, parseList);

        //Generate the from clause and where clause
        returnArray[0] = generateFromClause(parseList);
        returnArray[1] = generateWhereClause(parseList, filter, queriedObject);

        return returnArray;
    }
}
