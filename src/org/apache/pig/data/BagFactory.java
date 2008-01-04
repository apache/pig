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
package org.apache.pig.data;

import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.util.SpillableMemoryManager;

/**
 * A bag factory.  Can be used to generate different types of bags
 * depending on what is needed.
 */
public class BagFactory {
    private static BagFactory gSelf;
    private static SpillableMemoryManager gMemMgr;

    static { gSelf = new BagFactory(); }
    
    /**
     * Get a reference to the singleton factory.
     */
    public static BagFactory getInstance() {
        return gSelf;
    }
    
    /**
     * Get a default (unordered, not distinct) data bag.
     */
    public DataBag newDefaultBag() {
        DataBag b = new DefaultDataBag();
        gMemMgr.registerSpillable(b);
        return b;
    }

    /**
     * Get a sorted data bag.
     * @param spec EvalSpec that controls how the data is sorted.
     * If null, default comparator will be used.
     */
    public DataBag newSortedBag(EvalSpec spec) {
        DataBag b = new SortedDataBag(spec);
        gMemMgr.registerSpillable(b);
        return b;
    }
    
    /**
     * Get a distinct data bag.
     */
    public DataBag newDistinctBag() {
        DataBag b = new DistinctDataBag();
        gMemMgr.registerSpillable(b);
        return b;
    }

    private BagFactory() {
        gMemMgr = new SpillableMemoryManager();
    }

}

