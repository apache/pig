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

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.classification.InterfaceAudience;

/**
 * Default implementation of TupleFactory.
 */
@InterfaceAudience.Private
public class BinSedesTupleFactory extends TupleFactory {
    @Override
    public Tuple newTuple() {
        return new BinSedesTuple();
    
    }

    @Override
    public Tuple newTuple(int size) {
        return new BinSedesTuple(size);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public Tuple newTuple(List c) {
        return new BinSedesTuple(c);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple newTupleNoCopy(List list) {
        return new BinSedesTuple(list, 1);
    }

    @Override
    public Tuple newTuple(Object datum) {
        Tuple t = new BinSedesTuple(1);
        try {
            t.set(0, datum);
        } catch (ExecException e) {
            // The world has come to an end, we just allocated a tuple with one slot
            // but we can't write to that slot.
            throw new RuntimeException("Unable to write to field 0 in newly " +
                "allocated tuple of size 1!", e);
        }
        return t;
    }

    @Override
    public Class<? extends Tuple> tupleClass() {
        return BinSedesTuple.class;
    }

    @Override
    public Class<? extends TupleRawComparator> tupleRawComparatorClass() {
        return BinSedesTuple.getComparatorClass();
    }

    @Override
    public boolean isFixedSize() {
        return false;
    }
}

