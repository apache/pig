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
import java.util.StringTokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class TOKENIZE extends EvalFunc<DataBag> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            DataBag output = mBagFactory.newDefaultBag();
            String str = (String)input.get(0);
            StringTokenizer tok = new StringTokenizer(str, " \",()*", false);
            while (tok.hasMoreTokens()) {
                output.add(mTupleFactory.newTuple(tok.nextToken()));
            }
            return output;
        } catch (ExecException ee) {
            IOException oughtToBeEE = new IOException();
            ee.initCause(ee);
            throw oughtToBeEE;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        // TODO FIX
        /*
        TupleSchema schema = new TupleSchema();
        schema.add(new AtomSchema("token"));
        return schema;
        */
        return null;
    }
}
