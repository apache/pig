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
package org.apache.pig.impl.io;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 *
 */
public class NullableBytesWritable extends PigNullableWritable {

    public NullableBytesWritable() {
        mValue = TupleFactory.getInstance().newTuple();
    }

    /**
     * @param obj
     */
    public NullableBytesWritable(Object obj) {
        mValue = TupleFactory.getInstance().newTuple();
        ((Tuple)mValue).append(obj);
    }

    public Object getValueAsPigType() {
        if (isNull())
            return null;
        Object obj = null;
        try {
            obj = ((Tuple)mValue).get(0);
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
        return obj;
    }
}
