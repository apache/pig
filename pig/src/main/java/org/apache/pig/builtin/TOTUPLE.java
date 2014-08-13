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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This class makes a tuple out of the parameter
 * T = foreach U generate TOTUPLE($0, $1, $2);
 * It generates a tuple containing $0, $1, and $2
 */
public class TOTUPLE extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple input) throws IOException {
       return input;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            for (int i = 0; i < input.size(); ++i) {
                tupleSchema.add(input.getField(i));
            }
            return new Schema(new Schema.FieldSchema(getSchemaName(this
                    .getClass().getName().toLowerCase(), input), tupleSchema,
                    DataType.TUPLE));
        } catch (Exception e) {
            return null;
        }
    }

}
