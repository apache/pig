/**
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
package org.apache.pig.backend.hadoop.executionengine.tez.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;

@InterfaceAudience.Private
public class ObjectCache {

    private static final Log LOG = LogFactory.getLog(ObjectCache.class);
    private final ObjectRegistry registry = new ObjectRegistryImpl();
    private static ObjectCache cache = new ObjectCache();

    private ObjectCache() {
    }

    public static ObjectCache getInstance() {
        return cache;
    }

    public void cache(String key, Object value) {
      LOG.info("Adding " + key + " to cache");
      registry.cacheForVertex(key, value);
    }

    public Object retrieve(String key) {
      Object o = registry.get(key);
      if (o != null) {
        LOG.info("Found " + key + " in cache");
      }
      return o;
    }
}
