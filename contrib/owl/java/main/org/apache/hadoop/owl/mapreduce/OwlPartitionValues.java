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

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlKeyValue;

/** Class to pass the partition key values to the storage driver. Provides serialization functions so that
 *  storage driver can save this information in the job configuration.
 */
public class OwlPartitionValues implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;     

    /** The partition values map. */
    private Map<String, OwlKeyValue> partitionMap;

    /**
     * Instantiates a new owl partition values instance.
     * @param partitionMap the partition values
     */
    OwlPartitionValues(Map<String, OwlKeyValue> partitionMap) {
        this.partitionMap = partitionMap;
    }

    /**
     * Gets the value for the specified partition key name.
     * @param partitionKeyName the partition key name
     * @return the partition value
     */
    public OwlKeyValue getPartitionValue(String partitionKeyName) {
        return partitionMap.get(partitionKeyName);
    }

    /**
     * Gets the value of partitionValues as a map with partition key name as the key and the OwlKeyValue as the value.
     * @return the partition values map
     */
    public Map<String, OwlKeyValue> getPartitionMap() {
        return partitionMap;
    }

    /**
     * Serialize this object for storing into the job configuration.
     * @return the string
     * @throws OwlException the owl exception
     */
    public String serialize() throws OwlException {
        return SerializeUtil.serialize(this);
    }

    /**
     * Deserialize the given string into the OwlPartitionValues object.
     * @param serializedString the serialized string
     * @return the owl partition values object
     * @throws OwlException the owl exception
     */
    public static OwlPartitionValues deserialize(String serializedString) throws OwlException {
        return (OwlPartitionValues) SerializeUtil.deserialize(serializedString);
    }
}
