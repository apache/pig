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
package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * IN EvalFunc mimics the behavior of SQL IN operator. It takes more than or
 * equal to two arguments and compares the first argument against the rest one
 * by one. If it finds a match, true is returned; otherwise, false is returned.
 * If the first argument is null, it always returns false.
 */
public class IN extends EvalFunc<Boolean> {
    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input.size() < 2) {
            throw new ExecException("Invalid number of args");
        }

        Object expr = input.get(0);
        if (expr == null) {
            // If 1st argument (lhs operand of IN operator) is null, always
            // return false.
            return false;
        }

        for (int i = 1; i < input.size(); i++) {
            if (expr.equals(input.get(i))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }
};
