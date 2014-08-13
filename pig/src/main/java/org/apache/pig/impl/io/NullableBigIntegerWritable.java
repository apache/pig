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

import java.math.BigInteger;

import org.apache.pig.backend.hadoop.BigIntegerWritable;

public class NullableBigIntegerWritable extends PigNullableWritable {

    public NullableBigIntegerWritable() {
        mValue = new BigIntegerWritable();
    }

    public NullableBigIntegerWritable(BigInteger value) {
        mValue = new BigIntegerWritable(value);
    }

    @Override
    public Object getValueAsPigType() {
        return isNull() ? null : ((BigIntegerWritable)mValue).get();
    }

}
