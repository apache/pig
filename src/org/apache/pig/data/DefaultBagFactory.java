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

import java.util.Comparator;
import java.util.List;

/**
 * Default implementation of BagFactory.
 */
public class DefaultBagFactory extends BagFactory {
    /**
     * Get a default (unordered, not distinct) data bag.
     */
    @Override
    public DataBag newDefaultBag() {
        DataBag b = new DefaultDataBag();
        return b;
    }
    
    /**
     * Get a default (unordered, not distinct) data bag from
     * an existing list of tuples. Note that the bag does NOT
     * copy the tuples but uses the provided list as its backing store.
     * So it takes ownership of the list.
     */
    @Override
    public DataBag newDefaultBag(List<Tuple> listOfTuples) {
        DataBag b = new DefaultDataBag(listOfTuples);
        return b;
    }

    /**
     * Get a sorted data bag.
     * @param comp Comparator that controls how the data is sorted.
     * If null, default comparator will be used.
     */
    @Override
    public DataBag newSortedBag(Comparator<Tuple> comp) {
        DataBag b = new SortedDataBag(comp);
        return b;
    }
    
    /**
     * Get a distinct data bag.
     */
    @Override
    public DataBag newDistinctBag() {
        DataBag b = new DistinctDataBag();
        return b;
    }

    DefaultBagFactory() {
        super();
    }

}

