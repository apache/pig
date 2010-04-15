
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

import org.apache.hadoop.owl.protocol.OwlTableName;

public class OwlTableInputInfo {

    /** The Owl Metadata server uri */
    private String serverUri;

    /** The owl table name */
    private OwlTableName tableName;

    /** The partition predicates to filter on, an arbitrary AND/OR filter */
    private String partitionPredicates;

    /** The information about the partitions matching the specified Owl query */
    private OwlJobInfo jobInfo;

    /**
     * Initializes a new OwlTableInput instance.
     * @param serverUri the Owl Metadata server uri
     * @param tableName the owl table name
     * @param partitionPredicates the partition predicates to filter on, an arbitrary AND/OR filter.
     */
    public OwlTableInputInfo(String serverUri, OwlTableName tableName,
            String partitionPredicates) {
        this.serverUri = serverUri;
        this.tableName = tableName;
        this.partitionPredicates = partitionPredicates;
    }

    /**
     * Gets the value of serverUri
     * @return the serverUri
     */
    public String getServerUri() {
        return serverUri;
    }

    /**
     * Gets the value of tableName
     * @return the tableName
     */
    public OwlTableName getTableName() {
        return tableName;
    }

    /**
     * Gets the value of partitionPredicates
     * @return the partitionPredicates
     */
    public String getPartitionPredicates() {
        return partitionPredicates;
    }

    /**
     * Gets the value of job info
     * @return the job info
     */
    public OwlJobInfo getJobInfo() {
        return jobInfo;
    }

    /**
     * Sets the value of jobInfo
     * @param jobInfo the jobInfo to set
     */
    public void setJobInfo(OwlJobInfo jobInfo) {
        this.jobInfo = jobInfo;
    }
}
