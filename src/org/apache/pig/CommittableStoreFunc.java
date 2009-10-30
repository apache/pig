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
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;

/**
 * A storefunc which has an extra commit() method which is called
 * when all mappers (when the storefunc is part of map) or reducers (when the
 * storefunc is part of reduce) are finished. Currently this will allow storefuncs
 * to do any cleanup/finalizing activities knowing that all the maps/reducers
 * have finished - one such use case is for zebra storage to build an index
 * for sorted files once all writes are done.
 */
public interface CommittableStoreFunc extends StoreFunc {
    /**
     * This method is called when all mappers (when the storefunc is part of 
     * map) or reducers (when the storefunc is part of reduce) are finished.
     * This allows the storeFunc to do any global commit actions - only called
     * when all mappers/reducers successfully complete.
     * 
     * If the StoreFunc needs to get hold of StoreConfig object for the store
     * it can call {@link MapRedUtil#getStoreConfig(Configuration)} where
     * conf is the Configuration object passed in the commit() call.
     * 
     * @param conf Configuration object for the job
     */
    public void commit(Configuration conf) throws IOException;
}
