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
package org.apache.hadoop.owl.backend;

import java.util.List;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyBaseEntity;
import org.apache.hadoop.owl.entity.KeyListValueEntity;
import org.apache.hadoop.owl.entity.KeyValueBaseEntity;
import org.apache.hadoop.owl.entity.KeyValueEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlKey.KeyType;

/** Utility functions used by the backend and logical layer */ 
public class BackendUtil {

    /**
     * Gets the key type.
     * 
     * @param keyValue
     *            the key value
     * 
     * @return the key type
     * 
     * @throws OwlException
     *             the owl exception
     */
    public static KeyType getKeyType(KeyValueBaseEntity keyValue) throws OwlException {

        if( keyValue.getPropertyKeyId() != null ) {
            return KeyType.PROPERTY; 
        } else if( keyValue.getGlobalKeyId() != null ) {
            return KeyType.GLOBAL; 
        } else if( keyValue.getClass().equals(KeyValueEntity.class) ) {
            if( ((KeyValueEntity) keyValue).getPartitionKeyId() != null ) {
                return KeyType.PARTITION;
            }
        } 

        throw new OwlException(ErrorType.ERROR_INVALID_TYPE, "Invalid KeyType for id " + keyValue.getId() );
    }

    /**
     * Gets the key id.
     * 
     * @param keyValue
     *            the key value
     * 
     * @return the key id
     * 
     * @throws OwlException
     *             the owl exception
     */
    public static Integer getKeyId(KeyValueBaseEntity keyValue) throws OwlException {

        if( keyValue.getPropertyKeyId() != null ) {
            return keyValue.getPropertyKeyId(); 
        } else if( keyValue.getGlobalKeyId() != null ) {
            return keyValue.getGlobalKeyId(); 
        } else if( keyValue.getClass().equals(KeyValueEntity.class) ) {
            if( ((KeyValueEntity) keyValue).getPartitionKeyId() != null ) {
                return ((KeyValueEntity) keyValue).getPartitionKeyId();
            }
        } 

        throw new OwlException(ErrorType.ERROR_INVALID_TYPE, "Invalid KeyType for id " + keyValue.getId() );
    }

    /**
     * Gets the key for value.
     * 
     * @param keyValue
     *            the key value
     * @param owlTable
     *            the owl table
     * @param globalKeys
     *            the global keys
     * 
     * @return the key for value
     * 
     * @throws OwlException
     *             the owl exception
     */
    public static KeyBaseEntity getKeyForValue(KeyValueBaseEntity keyValue, OwlTableEntity owlTable, List<GlobalKeyEntity> globalKeys) throws OwlException {
        KeyType type = getKeyType(keyValue);
        Integer keyId = getKeyId(keyValue);

        if( type == KeyType.PROPERTY ) {
            if( owlTable.getPropertyKeys() != null ) {
                for(PropertyKeyEntity propKey : owlTable.getPropertyKeys()) {
                    if( propKey.getId() == keyId.intValue() ) {
                        return propKey;
                    }
                }
            }
        } else if( type == KeyType.PARTITION ) {
            if( owlTable.getPartitionKeys() != null ) {
                for(PartitionKeyEntity partKey : owlTable.getPartitionKeys()) {
                    if( partKey.getId() == keyId.intValue() ) {
                        return partKey;
                    }
                }
            }
        } else if( type == KeyType.GLOBAL ) {
            if( globalKeys != null ) {
                for(GlobalKeyEntity globKey : globalKeys) {
                    if( globKey.getId() == keyId.intValue() ) {
                        return globKey;
                    }
                }
            }
        }

        throw new OwlException(ErrorType.ERROR_INVALID_TYPE, "KeyType " + type + " id " + keyId );
    }

    /**
     * Gets the value specified for a KeyValue.
     * 
     * @param kv
     *            the key value
     * @param key
     *            the key
     * 
     * @return the value
     * 
     * @throws OwlException
     *             the owl exception
     */
    public static Object getValue(KeyValueBaseEntity kv, KeyBaseEntity key) throws OwlException {

        if( OwlKey.DataType.fromCode(key.getDataType()) == OwlKey.DataType.STRING ) {
            return kv.getStringValue();
        } else if( OwlKey.DataType.fromCode(key.getDataType()) == OwlKey.DataType.INT ) {
            return kv.getIntValue();
        }

        throw new OwlException(ErrorType.ERROR_INVALID_KEY_DATATYPE, OwlKey.DataType.fromCode(key.getDataType()).toString());
    }


    /**
     * Gets the value specified for a KeyListValue.
     * 
     * @param listValue
     *            the list value
     * @param key
     *            the key
     * 
     * @return the value
     * 
     * @throws OwlException
     *             the owl exception
     */
    public static Object getValue(KeyListValueEntity listValue, KeyBaseEntity key) throws OwlException {

        if( OwlKey.DataType.fromCode(key.getDataType()) == OwlKey.DataType.STRING ) {
            return listValue.getStringValue();
        } else if( OwlKey.DataType.fromCode(key.getDataType()) == OwlKey.DataType.INT ) {
            return listValue.getIntValue();
        }

        throw new OwlException(ErrorType.ERROR_INVALID_KEY_DATATYPE, OwlKey.DataType.fromCode(key.getDataType()).toString());
    }

    /**
     * Validate key value, checks if the value lies in the bounded list (if specified for the key).
     * 
     * @param kv the key value
     * @param table the owl table
     * @param globalKeys the global keys
     * 
     * @throws OwlException the owl exception
     */
    public static void validateKeyValue(KeyValueBaseEntity kv, OwlTableEntity table, List<GlobalKeyEntity> globalKeys) throws OwlException {

        KeyBaseEntity key = BackendUtil.getKeyForValue(kv, table, globalKeys);

        //Don't invalidate if key has no bound list specified
        if( key.getListValues() != null && key.getListValues().size() > 0 ) {
            boolean matched = false;
            Object inputValue = BackendUtil.getValue(kv, key);

            for(KeyListValueEntity valueEntity :  key.getListValues()) {
                Object listValue = BackendUtil.getValue(valueEntity, key);

                if( inputValue.equals(listValue) ) {
                    //Value satisfies the bounded list
                    matched = true;
                    break;
                }
            }

            if( matched == false ) {
                throw new OwlException(ErrorType.ERROR_INVALID_LIST_VALUE, "Value <" + inputValue + "> for key <" + key.getName() + ">");
            }
        }
    }

    /**
     * Gets the key corresponding to given name.
     * 
     * @param keyName
     *            the name of key to search for
     * @param owlTable
     *            the owl table
     * @param globalKeys
     *            the global keys
     * 
     * @return the key for the name
     * 
     * @throws OwlException
     *             the owl exception
     */
    public static KeyBaseEntity getKeyForName(String keyName, OwlTableEntity owlTable, List<GlobalKeyEntity> globalKeys) throws OwlException {
        if (owlTable.getPartitionKeys() != null) {
            for (PartitionKeyEntity partKey : owlTable.getPartitionKeys()) {
                if (partKey.getName().equalsIgnoreCase(keyName)) {
                    return partKey;
                }
            }
        }

        if (owlTable.getPropertyKeys() != null) {
            for (PropertyKeyEntity propKey : owlTable.getPropertyKeys()) {
                if (propKey.getName().equalsIgnoreCase(keyName)) {
                    return propKey;
                }
            }
        } 

        if (globalKeys != null) {
            for (GlobalKeyEntity globKey : globalKeys) {
                if (globKey.getName().equalsIgnoreCase(keyName)) {
                    return globKey;
                }
            }
        }

        throw new OwlException(ErrorType.ERROR_INVALID_KEY_NAME, keyName );
    }
}
