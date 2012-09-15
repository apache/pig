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
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * This UDF takes a Map and returns a Bag containing the keyset. <br />
 *
 * <pre>
 * <code>
 * grunt> cat data
 * [open#apache,1#2,11#2]
 * [apache#hadoop,3#4,12#hadoop]
 *
 * grunt> a = load 'data' as (M:[]);
 * grunt> b = foreach a generate KEYSET($0);
 * grunt> dump b;
 * ({(open),(1),(11)})
 * ({(3),(apache),(12)})
 * </code>
 * </pre>
 */
public class KEYSET extends EvalFunc<DataBag> {
    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if(input == null || input.size() == 0) {
            return null;
        }

        Map<String, Object> m = null;
        //Input must be of type Map. This is verified at compile time
        m = (Map<String, Object>)(input.get(0));
        if(m == null) {
            return null;
        }

        DataBag bag = new NonSpillableDataBag(m.size());
        for (String s : m.keySet()) {
            Tuple t = TUPLE_FACTORY.newTuple(s);
            bag.add(t);
        }

        return bag;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            if(input.getField(0).type != DataType.MAP) {
                throw new RuntimeException("Expected map, received schema " +DataType.findTypeName(input.getField(0).type));
            }
        } catch(FrontendException e) {
            throw new RuntimeException(e);
        }

        FieldSchema innerFieldSchema = new Schema.FieldSchema(null, DataType.CHARARRAY);
        Schema innerSchema = new Schema(innerFieldSchema);
        Schema bagSchema = null;

        try {
            bagSchema = new Schema(new FieldSchema(null, innerSchema, DataType.BAG));
        } catch(FrontendException e) {
            throw new RuntimeException(e);
        }
        return bagSchema;

    }
}