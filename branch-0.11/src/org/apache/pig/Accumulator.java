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

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.Tuple;

/**
 * An interface that allows UDFs that take a bag to accumulate tuples in chunks rather than take
 * the whole set at once.  This is intended for UDFs that do not need to see all of the tuples
 * together but cannot be used with the combiner.  This lowers the memory needs, avoiding the need
 * to spill large bags, and thus speeds up the query.  An example is something like session analysis.
 * It cannot be used with the combiner because all it's inputs must first be ordered.  But it does
 * not need to see all the tuples at once.  UDF implementors might also choose to implement this
 * interface so that if other UDFs in the FOREACH implement it it can be used.
 * @since Pig 0.6
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Accumulator <T> {
    /**
     * Pass tuples to the UDF.
     * @param b A tuple containing a single field, which is a bag.  The bag will contain the set
     * of tuples being passed to the UDF in this iteration.
     */
    public void accumulate(Tuple b) throws IOException;

    /**
     * Called when all tuples from current key have been passed to accumulate.
     * @return the value for the UDF for this key.
     */
    public T getValue();
    
    /** 
     * Called after getValue() to prepare processing for next key. 
     */
    public void cleanup();
}
