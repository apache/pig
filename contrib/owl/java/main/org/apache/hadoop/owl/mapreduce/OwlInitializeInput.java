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
package org.apache.hadoop.owl.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.driver.OwlDriver;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;

/**
 * The Class which handles querying the owl metadata server using the OwlDriver. The list of 
 * partitions matching the partition filter is fetched from the server and the information is 
 * serialized and written into the JobContext configuration. The inputInfo is also updated with
 * info required in the client process context.
 */
class OwlInitializeInput {

    /**
     * Set the input to use for the Job. This queries the metadata server with the specified partition predicates,
     * gets the matching partitions, puts the information in the configuration object. The inputInfo object is
     * updated with information needed in the client process context.
     * @param job the job object
     * @param inputInfo the owl table input info
     * @throws OwlException 
     * @throws IOException the exception in communicating with the owl server
     */
    public static void setInput(Job job, OwlTableInputInfo inputInfo) throws OwlException {

        //* Create a OwlDriver instance with specified uri
        //* Call OwlDriver.getOwlTable to get the table schema
        //* Call OwlDriver.getPartitions to get list of partitions satisfying the given partition filter
        //* Create and initialize an OwlJobInfo object
        //* Save the OwlJobInfo object in the OwlTableInputInfo instance
        //* Serialize the OwlJobInfo and save in the Job's Configuration object 

        OwlDriver odriver = new OwlDriver(inputInfo.getServerUri());
        OwlTable otable = odriver.getOwlTable(inputInfo.getTableName());
        OwlSchema tableSchema = otable.getSchema();
        List<OwlPartition> partitions = odriver.getPartitions(inputInfo.getTableName(), inputInfo.getPartitionPredicates());
        // convert List<OwlPartition> to List<OwlPartitionInfo>
        List<OwlPartitionInfo> partitionInfoList = new ArrayList<OwlPartitionInfo>();
        for (OwlPartition partition:partitions){
            OwlPartitionInfo partitionInfo = new OwlPartitionInfo(partition.getSchema(),
                    partition.getLoader(), partition.getStorageLocation());
            partitionInfo.setPartitionValues(getPartitionValues(partition));
            partitionInfoList.add(partitionInfo);
        }

        OwlJobInfo owlJobInfo = new OwlJobInfo(inputInfo.getTableName(), tableSchema, partitionInfoList);
        inputInfo.setJobInfo(owlJobInfo);

        job.getConfiguration().set(OwlInputFormat.OWL_KEY_JOB_INFO, SerializeUtil.serialize(owlJobInfo));
    }

    /**
     * Gets the partition key values to be passed to the storage driver.
     * @param partition the partition to get the values for
     * @return the partition values
     */
    private static OwlPartitionValues getPartitionValues(OwlPartition partition) {

        List<OwlKeyValue> owlKeyValues = partition.getKeyValues();
        Map<String, OwlKeyValue> mapValues= new HashMap<String, OwlKeyValue>();

        for( OwlKeyValue  keyValue : owlKeyValues) {
            if( keyValue.getKeyType() == OwlKey.KeyType.PARTITION){
                mapValues.put(keyValue.getKeyName(), keyValue);
            }
        }

        return new OwlPartitionValues(mapValues);
    }

}
