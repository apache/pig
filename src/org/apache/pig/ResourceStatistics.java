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
package org.apache.pig;

public class ResourceStatistics {

    public static class ResourceFieldStatistics {

        int version;

        enum Distribution {UNIFORM, NORMAL, POWER};

        public long numDistinctValues;  // number of distinct values represented in this field
        public Distribution distribution; // how values in this field are distributed

        // We need some way to represent a histogram of values in the field,
        // as those will be useful.  However, we can't count on being
        // able to hold such histograms in memory.  Have to figure out
        // how they can be kept on disk and represented here.

        // Probably more in here
    }

    public long mBytes; // size in megabytes
    public long numRecords;  // number of records
    public ResourceFieldStatistics[] fields;

    // Probably more in here
}