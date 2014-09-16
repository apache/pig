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

import java.util.List;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * This interface allow StoreFunc to specify resources needed
 * in distributed cache. The resources can be on dfs (getCacheFiles)
 * or locally (getShipFiles)
 * @since Pig 0.14
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface StoreResources {
    /**
     * Allow a StoreFunc to specify a list of files it would like placed in the distributed 
     * cache.
     * The default implementation returns null.
     * @return A list of files
     */
    public List<String> getCacheFiles();

    /**
     * Allow a StoreFunc to specify a list of files located locally and would like to ship to backend 
     * (through distributed cache). Check for {@link FuncUtils} for utility function to facilitate it
     * The default implementation returns null.
     * @return A list of files
     */
    public List<String> getShipFiles();
}
