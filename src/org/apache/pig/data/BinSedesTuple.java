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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.pig.classification.InterfaceAudience;

/**
 * This tuple has a faster (de)serialization mechanism. It to be used for
 * storing intermediate data between Map and Reduce and between MR jobs.
 * This is for internal pig use only. The serialization format can change, so
 *  do not use it for storing any persistant data (ie in load/store functions).
 */
@InterfaceAudience.Private
public class BinSedesTuple extends DefaultTuple {

    private static final long serialVersionUID = 1L;
    private static final InterSedes sedes = InterSedesFactory.getInterSedesInstance();

    @Override
    public void write(DataOutput out) throws IOException {
        sedes.writeDatum(out, this, DataType.TUPLE);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        // Clear our fields, in case we're being reused.
        mFields.clear();
        sedes.addColsToTuple(in, this);
    }



    /**
     * Default constructor
     */
    BinSedesTuple() {
       super();
    }

    /**
     * Construct a tuple with a known number of fields.  Package level so
     * that callers cannot directly invoke it.
     * @param size Number of fields to allocate in the tuple.
     */
    BinSedesTuple(int size) {
        super(size);
    }

    /**
     * Construct a tuple from an existing list of objects.  Package
     * level so that callers cannot directly invoke it.
     * @param c List of objects to turn into a tuple.
     */
    BinSedesTuple(List<Object> c) {
        super(c);
    }

    /**
     * Construct a tuple from an existing list of objects.  Package
     * level so that callers cannot directly invoke it.
     * @param c List of objects to turn into a tuple.  This list will be kept
     * as part of the tuple.
     * @param junk Just used to differentiate from the constructor above that
     * copies the list.
     */
    BinSedesTuple(List<Object> c, int junk) {
        super(c, junk);
    }

    public static Class<? extends TupleRawComparator> getComparatorClass() {
        return InterSedesFactory.getInterSedesInstance().getTupleRawComparatorClass();
    }
}
