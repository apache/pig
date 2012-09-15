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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * This UDF takes a Map and returns a Tuple containing the value set. <br />
 * Note, this UDF returns only unique values. For all values, use <br />
 * VALUELIST instead. <br />
 *
 * <pre>
 * <code>
 * grunt> cat data
 * [open#apache,1#2,11#2]
 * [apache#hadoop,3#4,12#hadoop]
 *
 * grunt> a = load 'data' as (M:[]);
 * grunt> b = foreach a generate VALUELIST($0);
 * ({(apache),(2)})
 * ({(4),(hadoop)})
 *
 * </code>
 * </pre>
 */
public class VALUESET extends EvalFunc<DataBag> {
    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
    private static final BagFactory BAG_FACTORY = BagFactory.getInstance();

    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        Map<String, Object> m = null;
        // Input must be of type Map. This is verified at compile time
        m = (Map<String, Object>) (input.get(0));
        if (m == null) {
            return null;
        }

        int initialSetSize = getInitialSetSize(m.values());
        Set<Object> uniqueElements = new HashSet<Object>(initialSetSize);
        DataBag bag = new NonSpillableDataBag();

        Iterator<Object> iter = m.values().iterator();

        while (iter.hasNext()) {
            Object val = iter.next();
            if (!uniqueElements.contains(val)) {
                uniqueElements.add(val);
                Tuple t = TUPLE_FACTORY.newTuple(val);
                bag.add(t);
            }
        }

        return bag;
    }

    private int getInitialSetSize(Collection<Object> c) {
        return (Math.max((int) (c.size() / .75f) + 1, 16));
    }

    @Override
    public Schema outputSchema(Schema input) {
        FieldSchema f = null;
        FieldSchema innerFieldSchema = null;
        try {
            f = input.getField(0);
        } catch (FrontendException fe) {
            throw new RuntimeException(fe);
        }
        if (f.type != DataType.MAP) {
            throw new RuntimeException("Expected map, received schema "
                    + DataType.findTypeName(f.type));
        }

        Schema s = f.schema;

        if (s != null) {
            Schema.FieldSchema fs = null;
            try {
                fs = s.getField(0);
            } catch (FrontendException fe) {
                throw new RuntimeException(fe);
            }
            if (fs != null) {
                innerFieldSchema = new Schema.FieldSchema(null, fs.type);
            }
        } else {
            innerFieldSchema = new Schema.FieldSchema(null, DataType.BYTEARRAY);
        }

        Schema innerSch = new Schema(innerFieldSchema);
        Schema bagSchema = null;
        try {
            bagSchema = new Schema(new FieldSchema(null, innerSch, DataType.BAG));
        } catch (FrontendException fe) {
            throw new RuntimeException(fe);
        }
        return bagSchema;
    }
}
