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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.pig.data.Tuple;


public abstract class ComparisonFunc extends WritableComparator {
    public ComparisonFunc() {
        super(Tuple.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        return compare((Tuple)a, (Tuple)b);
    }

    /**
     * This callback method must be implemented by all subclasses. Compares 
     * its two arguments for order. Returns a negative integer, zero, or a 
     * positive integer as the first argument is less than, equal to, or 
     * greater than the second. The order of elements of the tuples correspond 
     * to the fields specified in the order by clause. 
     * Same semantics as {@link java.util.Comparator}.
     * 
     * @param t1 the first Tuple to be compared.
     * @param t2 the second Tuple to be compared.
     * @throws IOException
     * @see java.util.Comparator
     */
    abstract public int compare(Tuple t1, Tuple t2);
}
