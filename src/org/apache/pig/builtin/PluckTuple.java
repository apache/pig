/**
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
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

/**
 * This is a UDF which allows the user to specify a string prefix, and then
 * filter for the columns in a relation that begin with that prefix.
 *
 * Example:
 * a = load 'a' as (x, y);
 * b = load 'b' as (x, y);
 * c = join a by x, b by x;
 * DEFINE pluck PluckTuple('a::');
 * d = foreach c generate FLATTEN(pluck(*));
 * describe c;
 * c: {a::x: bytearray,a::y: bytearray,b::x: bytearray,b::y: bytearray}
 * describe d;
 * d: {plucked::a::x: bytearray,plucked::a::y: bytearray}
 */
public class PluckTuple extends EvalFunc<Tuple> {
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    private boolean isInitialized = false;
    private int[] indicesToInclude;
    private String prefix;

    public PluckTuple(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (!isInitialized) {
            List<Integer> indicesToInclude = Lists.newArrayList();
            Schema inputSchema = getInputSchema();
            for (int i = 0; i < inputSchema.size(); i++) {
                String alias = inputSchema.getField(i).alias;
                if (alias.startsWith(prefix)) {
                    indicesToInclude.add(i);
                }
            }
            this.indicesToInclude = new int[indicesToInclude.size()];
            int idx = 0;
            for (int val : indicesToInclude) {
                this.indicesToInclude[idx++] = val;
            }
            isInitialized = true;
        }
        Tuple result = mTupleFactory.newTuple(indicesToInclude.length);
        int idx = 0;
        for (int val : indicesToInclude) {
            result.set(idx++, input.get(val));
        }
        return result;
    }

    public Schema outputSchema(Schema inputSchema) {
        if (!isInitialized) {
            List<Integer> indicesToInclude = Lists.newArrayList();
            for (int i = 0; i < inputSchema.size(); i++) {
                String alias;
                try {
                    alias = inputSchema.getField(i).alias;
                } catch (FrontendException e) {
                    throw new RuntimeException(e); // Should never happen
                }
                if (alias.startsWith(prefix)) {
                    indicesToInclude.add(i);
                }
            }
            this.indicesToInclude = new int[indicesToInclude.size()];
            int idx = 0;
            for (int val : indicesToInclude) {
                this.indicesToInclude[idx++] = val;
            }
            isInitialized = true;
        }
        Schema outputSchema = new Schema();
        for (int val : indicesToInclude) {
            try {
                outputSchema.add(inputSchema.getField(val));
            } catch (FrontendException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return new Schema(new Schema.FieldSchema("plucked", outputSchema, DataType.TUPLE));
        } catch (FrontendException e) {
            throw new RuntimeException(e); // Should never happen
        }
    }
}