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
package org.apache.pig.test;

import java.util.Comparator;
import java.util.List;

import org.apache.pig.data.*;

// Test data bag factory, for testing that we can properly provide a non
// default bag factory.
public class NonDefaultBagFactory extends BagFactory {
    public DataBag newDefaultBag() { return null; }
    /* (non-Javadoc)
     * @see org.apache.pig.data.BagFactory#newDefaultBag(java.util.List)
     */
    @Override
    public DataBag newDefaultBag(List<Tuple> listOfTuples) {
        return null;
    }
    public DataBag newSortedBag(Comparator<Tuple> comp) { return null; }
    public DataBag newDistinctBag() { return null; }

    public NonDefaultBagFactory() { super(); }
}

