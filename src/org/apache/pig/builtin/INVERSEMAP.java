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
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This UDF accepts a Map as input with values of any primitive data type. <br />
 * UDF swaps keys with values and returns the new inverse Map. <br />
 * Note in case original values are non-unique, the resulting Map would <br />
 * contain String Key -> DataBag of values. Here the bag of values is composed <br />
 * of the original keys having the same value. <br />
 *
 * <pre>
 * Note: 1. UDF accepts Map with Values of primitive data type
 *       2. UDF returns Map<String,DataBag>
 * <code>
 * grunt> cat 1data
 * [open#1,1#2,11#2]
 * [apache#2,3#4,12#24]
 *
 * <br />
 * grunt> a = load 'data' as (M:[int]);
 * grunt> b = foreach a generate INVERSEMAP($0);
 *
 * grunt> dump b;
 * ([2#{(1),(11)},apache#{(open)}])
 * ([hadoop#{(apache),(12)},4#{(3)}])
 * </code>
 * </pre>
 */
@SuppressWarnings("unchecked")
public class INVERSEMAP extends EvalFunc<Map> {
    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
    private static final BagFactory BAG_FACTORY = BagFactory.getInstance();

    @Override
    public Map exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        Map<String, Object> inputMap = (Map<String, Object>)input.get(0);
        if(inputMap == null) {
            return null;
        }

        return doInverse(inputMap);
    }

    @Override
    public Schema outputSchema(Schema input) {
        try{
            if(input.getField(0).type != DataType.MAP) {
                throw new RuntimeException("Expected map, received schema " +DataType.findTypeName(input.getField(0).type));
            }
        } catch(FrontendException e) {
            throw new RuntimeException(e);
        }
        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }

    private HashMap<String, DataBag> doInverse(Map<String,Object> original) throws ExecException {
        final HashMap<String, DataBag> inverseMap = new HashMap<String, DataBag>(original.size());

        for (Map.Entry<String, Object> entry : original.entrySet()) {
            Object o = entry.getValue();
            String newKey;

            // Call toString for all primitive types, else throw an Exception
            if (!(o instanceof Tuple || o instanceof DataBag)) {
                newKey = o.toString();
            } else {
                throw new ExecException("Wrong type. Value is of type " + o.getClass());
            }

            // Create a new bag if "newKey" does not exist in Map
            DataBag bag = inverseMap.get(newKey);
            if (bag == null) {
                bag = new NonSpillableDataBag();
                bag.add(TUPLE_FACTORY.newTuple(entry.getKey()));
                inverseMap.put(newKey, bag);
            } else {
                bag.add(TUPLE_FACTORY.newTuple(entry.getKey()));
            }
        }
        return inverseMap;
    }

}
