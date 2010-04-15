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

package org.apache.hadoop.owl.client;

import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlDataElement;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPartitionProperty;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.protocol.OwlKey.KeyType;
import org.apache.hadoop.owl.logical.SelectPartitionpropertyObjectsCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KeyValueAssertionCriterion extends OwlTestCase {

    String keyName = null;
    String stringValue = null;
    DataType keyDataType;

    public String getKeyName(){
        return keyName;
    }

    public String getValue(){
        return stringValue;
    }

    public DataType getDataType(){
        return keyDataType;
    }

    public KeyValueAssertionCriterion(String keyName, Integer keyValue){
        this.keyName = keyName;
        this.stringValue = keyValue.toString();
        this.keyDataType = DataType.INT;
    }

    public KeyValueAssertionCriterion(String keyName, Long keyValue){
        this.keyName = keyName;
        this.stringValue = keyValue.toString();
        this.keyDataType = DataType.LONG;
    }

    public KeyValueAssertionCriterion(String keyName, String keyValue){
        this.keyName = keyName;
        this.stringValue = keyValue;
        this.keyDataType = DataType.STRING;
    }

    public KeyValueAssertionCriterion(String keyName, String keyDataType, Integer keyValue){
        this(keyName,keyValue);
        if (keyDataType.equalsIgnoreCase("INTEGER")){
            // pass okay - used to be called INTEGER before the INT now.
        } else {
            assertEquals(DataType.INT.name(),keyDataType);
        }
    }

    public KeyValueAssertionCriterion(String keyName, String keyDataType, Long keyValue){
        this(keyName,keyValue);
        assertEquals(DataType.LONG.name(),keyDataType);
    }

    public KeyValueAssertionCriterion(String keyName, String keyDataType, String keyValue){
        this(keyName,keyValue);
        assertEquals(DataType.STRING.name(),keyDataType);
    }

    public boolean matchesKeyName(String keyName){
        return (keyName.equalsIgnoreCase(this.keyName));
    }

    public static String getValueFromKeyValue(OwlKeyValue kv){
        if(kv.getDataType().equals(DataType.INT)){
            return kv.getIntValue().toString();
        } else if(kv.getDataType().equals(DataType.LONG)){
            return kv.getLongValue().toString();
        } else {
            return kv.getStringValue();
        }
    }

    public void assertMatchKey(OwlKeyValue kv){
        assertEquals(kv.getDataType(),this.keyDataType);
        assertTrue(this.keyName.equalsIgnoreCase(kv.getKeyName()));
        assertTrue(this.stringValue.equalsIgnoreCase(getValueFromKeyValue(kv)));
    }

    public static void verifyKeyValueValidity(OwlKeyValue kv, KeyValueAssertionCriterion[] assertionCriteria) throws OwlException {
        boolean found = false;

        for (KeyValueAssertionCriterion assertionCriterion : assertionCriteria){
            if (assertionCriterion.matchesKeyName(kv.getKeyName())){
                found = true;
                // System.out.println("Testing [" + kv.getKeyName() + "=" + getValueFromKeyValue(kv) + "] against [" + assertionCriterion.getKeyName()+ "=" + assertionCriterion.getValue() + "]");
                assertionCriterion.assertMatchKey(kv);
                return;
            }else{
                // System.out.println("Testing [" + kv.getKeyName() + "=" + getValueFromKeyValue(kv) + "] against [" + assertionCriterion.getKeyName()+ "=" + assertionCriterion.getValue() + "] => mismatch");
            }
        }

        if (!found){
            assertEquals(0,1); // guaranteed failure case - we didn't match any other expected property key names
        }
    }

    public static void verifyKeyValueValidity(OwlKeyValue kv, KeyType type, KeyValueAssertionCriterion[] assertionCriteria) throws OwlException {
        assertEquals(type.getCode(),kv.getKeyType().getCode());
        verifyKeyValueValidity(kv,assertionCriteria);
    }


    public static void verifyPartitionKeyValueValidity(OwlKeyValue kv, KeyValueAssertionCriterion[] assertionCriteria) throws OwlException {
        verifyKeyValueValidity(kv,KeyType.PARTITION,assertionCriteria);
    }


    public static void verifyPropertyKeyValueValidity(OwlKeyValue kv, KeyValueAssertionCriterion[] assertionCriteria) throws OwlException {
        verifyKeyValueValidity(kv,KeyType.PROPERTY,assertionCriteria);
    }

}

