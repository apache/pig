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

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.impl.io.NullableTuple;


/**
 * An interface for custom order by comparator function.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Deprecated
public abstract class ComparisonFunc extends WritableComparator {
    // If the comparison is a time consuming process
    // this reporter must be used to report progress
    protected PigProgressable reporter;
    
    public ComparisonFunc() {
        super(NullableTuple.class, true);
    }

    /**
     * Compare two tuples.  Note that even though both args are given type of
     * WritableComparable to match the WritableComparable interface, they
     * must both be tuples.
     * @param a first tuple
     * @param b tuple to compare a to
     * @return -1 if a &lt; b, 1 if a &gt; b, 0 if a = b
     */
    public int compare(WritableComparable a, WritableComparable b) {
        // The incoming key will be in a NullableTuple.  But the comparison
        // function needs a tuple, so pull the tuple out.
        return compare((Tuple)((NullableTuple)a).getValueAsPigType(), (Tuple)((NullableTuple)b).getValueAsPigType());
    }

    /**
     * This callback method must be implemented by all subclasses. Compares 
     * its two arguments for order.  The order of elements of the tuples correspond 
     * to the fields specified in the order by clause. 
     * Same semantics as {@link java.util.Comparator}.
     * 
     * @param t1 the first Tuple to be compared.
     * @param t2 the second Tuple to be compared.
     * @return  Returns a negative integer, zero, or a positive integer as the first
     * argument is less than, equal to, or greater than the second. 
     * @throws IOException
     * @see java.util.Comparator
     */
    abstract public int compare(Tuple t1, Tuple t2);

    /**
     * Set the reporter.  If the comparison takes a long time the
     * reporter should be called occasionally to avoid Hadoop timing out
     * underneath.  The default Hadoop timeout is 600 seconds.
     * @param reporter Progress reporter
     */
    public void setReporter(PigProgressable reporter) {
        this.reporter = reporter;
    }
}
