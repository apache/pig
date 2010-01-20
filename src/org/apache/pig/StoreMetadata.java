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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;

/**
 * This interface defines how to write metadata related to data to be loaded.
 * If a given store function does not implement this interface, it will be assumed that it
 * is unable to record metadata about the associated data.
 */

public interface StoreMetadata {

    /**
     * Store statistics about the data being written.
     * 
     * @throws IOException 
     */
    void storeStatistics(ResourceStatistics stats, String location, Configuration conf) throws IOException;

    /**
     * Store schema of the data being written
     * 
     * @throws IOException 
     */
    void storeSchema(ResourceSchema schema, String location, Configuration conf) throws IOException;
}
