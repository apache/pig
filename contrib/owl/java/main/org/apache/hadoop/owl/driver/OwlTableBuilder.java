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
package org.apache.hadoop.owl.driver;

import java.util.ArrayList;
import java.util.List;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlKeyListValue;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.protocol.OwlPartitionKey.IntervalFrequencyUnit;
import org.apache.hadoop.owl.protocol.OwlPartitionKey.PartitioningType;

/**
 * The owltable builder.
 */
public class OwlTableBuilder {

    private OwlTable owltable;
    private int partitionKeyLevel;

    public OwlTableBuilder(){
        owltable = new OwlTable();
        partitionKeyLevel = 0;
        owltable.setPropertyKeys(new ArrayList<OwlPropertyKey>());
        owltable.setPartitionKeys(new ArrayList<OwlPartitionKey>());
        owltable.setPropertyValues(new ArrayList<OwlKeyValue>());
    }

    public OwlTableBuilder setName(OwlTableName owltableName) throws OwlException {
        OwlTableHandler.validateTableName(owltableName);
        owltable.setName (owltableName);
        return this;
    }

    public OwlTableBuilder addProperty(String propertyKeyName, DataType dataType) throws OwlException {
        OwlTableHandler.validateStringValue("Property Key Name", propertyKeyName);

        OwlPropertyKey okey = new OwlPropertyKey(propertyKeyName, dataType);
        List<OwlPropertyKey> propertyKeys = owltable.getPropertyKeys();
        propertyKeys.add(okey);
        owltable.setPropertyKeys(propertyKeys);
        return this;
    }

    public OwlTableBuilder setSchema(OwlSchema owlSchema) throws OwlException {
        OwlTableHandler.validateValue("Schema", owlSchema);

        owltable.setSchema(owlSchema);
        return this;
    }

    public OwlTableBuilder addPartition(String keyName, DataType keyType) throws OwlException {
        OwlTableHandler.validateStringValue("Partition Key Name", keyName);

        partitionKeyLevel++;
        OwlPartitionKey owlpartitionkey = new OwlPartitionKey(keyName, keyType, 
                partitionKeyLevel, PartitioningType.LIST, new ArrayList<OwlKeyListValue>());

        owltable.getPartitionKeys().add(owlpartitionkey);
        return this;
    } 

    @SuppressWarnings("unchecked")
    public OwlTableBuilder addPartition(String keyName, DataType keyType, List<? extends Object> listInput) throws OwlException {
        OwlTableHandler.validateStringValue("Partition Key Name", keyName);

        partitionKeyLevel++;

        OwlPartitionKey owlpartitionkey;

        if ( keyType == DataType.STRING ){
            List<OwlKeyListValue> listValues = new ArrayList<OwlKeyListValue> ();
            for (String x:(List<String>)listInput){
                OwlKeyListValue value = new OwlKeyListValue();
                value.setStringValue(x);
                listValues.add(value);
            }
            owlpartitionkey = new OwlPartitionKey(keyName, keyType, 
                    partitionKeyLevel, PartitioningType.LIST, 
                    listValues);

        }else if (keyType == DataType.INT){
            List<OwlKeyListValue> listValues = new ArrayList<OwlKeyListValue>();
            for (Integer x:(List<Integer>)listInput) {
                OwlKeyListValue value = new OwlKeyListValue();
                value.setIntValue(x);
                listValues.add(value);
            }
            owlpartitionkey = new OwlPartitionKey(keyName, keyType, 
                    partitionKeyLevel, PartitioningType.LIST, listValues);
        }else{
            throw new OwlException (ErrorType.ERROR_INCONSISTANT_DATA_TYPE,"Unsupported data type.");
        }

        owltable.getPartitionKeys().add(owlpartitionkey);
        return this;
    }

    public OwlTableBuilder addIntervalPartition (String keyName, 
            String intervalStartString, 
            Integer intervalFrequency, 
            IntervalFrequencyUnit intervalFrequencyUnit) throws OwlException{
        OwlTableHandler.validateStringValue("Partition Key Name", keyName);

        Long intervalStart;
        partitionKeyLevel++;
        try {
            DateFormat dfm = new SimpleDateFormat(OwlUtil.DATE_FORMAT);
            intervalStart = Long.valueOf(dfm.parse(intervalStartString).getTime());
        } catch(Exception e) {
            throw new OwlException(ErrorType.ERROR_DATE_FORMAT, e);
        }

        OwlPartitionKey owlpartitionkey = new OwlPartitionKey(keyName, DataType.LONG, partitionKeyLevel, 
                PartitioningType.INTERVAL,
                intervalStart, intervalFrequency,
                intervalFrequencyUnit,
                new ArrayList<OwlKeyListValue>());

        owltable.getPartitionKeys().add(owlpartitionkey);
        return this;
    }


    public  OwlTable build() throws OwlException{
        return owltable;
    }
}
