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
package org.apache.pig.test.udf.evalfunc;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class AppendIndex extends AccumulatorEvalFunc<DataBag> {
    private final static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private final static BagFactory mBagFactory = BagFactory.getInstance();

    private DataBag interBag;

    long ct = 0;

    @Override
    public void accumulate(Tuple input) throws IOException {
        if (interBag == null) {
            interBag = mBagFactory.newDefaultBag();
            ct = 0;
        }
        for (Tuple t : (DataBag)input.get(0)) {
            Tuple t2 = mTupleFactory.newTupleNoCopy(t.getAll());
            t2.append(++ct);
            interBag.add(t2);
        }
    }

    @Override
    public DataBag getValue() {
        return interBag;
    }

    public void cleanup() {
        interBag = null;
    }

    @Override
    public Schema outputSchema(Schema inputSchema) {
        try {
            inputSchema.getField(0).schema.getField(0).schema.add(new Schema.FieldSchema("index", DataType.LONG));
            return inputSchema;
        } catch (Exception e) {
            return null;
        }
    }
}
