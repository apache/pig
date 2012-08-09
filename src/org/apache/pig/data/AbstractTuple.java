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

import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.util.TupleFormat;

import com.google.common.base.Joiner;

/**
 * This class provides a convenient base for Tuple implementations. This makes it easier
 * to provide default implementations as the Tuple interface is evolved.
 */
public abstract class AbstractTuple implements Tuple {
    @Override
    public Iterator<Object> iterator() {
        return getAll().iterator();
    }

    @Override
    public String toString() {
        return TupleFormat.format(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toDelimitedString(String delim) throws ExecException {
        return Joiner.on(delim).useForNull("").join(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getType(int fieldNum) throws ExecException {
        return DataType.findType(get(fieldNum));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull(int fieldNum) throws ExecException {
        return (get(fieldNum) == null);
    }

    @Override
    public boolean equals(Object other) {
        return (compareTo(other) == 0);
    }

    @Override
    public void reference(Tuple t) {
        throw new RuntimeException("Tuple#reference(Tuple) is deprecated and should not be used");
    }
}
