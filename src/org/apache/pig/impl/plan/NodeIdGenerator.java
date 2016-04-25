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

package org.apache.pig.impl.plan;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;

/**
 * Generates IDs as long values in a thread safe manner. Each thread has its own generated IDs.
 */
public class NodeIdGenerator {

	/**
	 * Holds a map of generated scoped-IDs per thread. Each map holds generated IDs per scope.
	 */
    private ThreadLocal<Map<String, AtomicLong>> scopeToIdMap
        = new ThreadLocal<Map<String, AtomicLong>>() {
            protected Map<String, AtomicLong> initialValue() {
                return new HashMap<String,AtomicLong>();
            }
        };

    /**
     * Singleton instance.
     */
    private static final NodeIdGenerator theGenerator = new NodeIdGenerator();

    /**
     * Private default constructor to force singleton use-case of this class.
     */
    private NodeIdGenerator() {}

    /**
     * Returns the NodeIdGenerator singleton.
     * @return
     */
    public static NodeIdGenerator getGenerator() {
        return theGenerator;
    }

    /**
     * Returns the next ID to be used for the current Thread.
     * 
     * @param scope
     * @return
     */
    public long getNextNodeId(final String scope) {
        // ThreadLocal usage protects us from having the same HashMap instance
        // being used by several threads, so we can use it without synchronized
        // blocks and still be thread-safe.
        Map<String, AtomicLong> map = scopeToIdMap.get();

        // the concurrent properties of the AtomicLong are useless here but
        // since it cost less to use such an object rather than created a
        // Long object instance each time we increment a counter ...
        AtomicLong l = map.get(scope);
        if ( l == null )
            map.put( scope, l = new AtomicLong() );
        return l.getAndIncrement();
    }

    /**
     * Reset the given scope IDs to 0 for the current Thread.
     * @param scope
     */
    @VisibleForTesting
    public static void reset(final String scope) {
        theGenerator.scopeToIdMap.get().remove(scope);
    }

    /**
     * Reset all scope IDs to 0 for the current Thread.
     */
    @VisibleForTesting
    public static void reset() {
        theGenerator.scopeToIdMap.remove();
    }
}
