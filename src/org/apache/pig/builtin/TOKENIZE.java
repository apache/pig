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
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class TOKENIZE extends EvalFunc<DataBag> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            DataBag output = mBagFactory.newDefaultBag();
            Object o = input.get(0);
            if (!(o instanceof String)) {
            	int errCode = 2114;
            	String msg = "Expected input to be chararray, but" +
                " got " + o.getClass().getName();
                throw new ExecException(msg, errCode, PigException.BUG);
            }
            StringTokenizer tok = new StringTokenizer((String)o, " \",()*", false);
            while (tok.hasMoreTokens()) {
                output.add(mTupleFactory.newTuple(tok.nextToken()));
            }
            return output;
        } catch (ExecException ee) {
            throw ee;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        
        try {
            Schema.FieldSchema tokenFs = new Schema.FieldSchema("token", 
                    DataType.CHARARRAY); 
            Schema tupleSchema = new Schema(tokenFs);

            Schema.FieldSchema tupleFs;
            tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema,
                    DataType.TUPLE);

            Schema bagSchema = new Schema(tupleFs);
            bagSchema.setTwoLevelAccessRequired(true);
            Schema.FieldSchema bagFs = new Schema.FieldSchema(
                        "bag_of_tokenTuples",bagSchema, DataType.BAG);
            
            return new Schema(bagFs); 
            
            
            
        } catch (FrontendException e) {
            // throwing RTE because
            //above schema creation is not expected to throw an exception
            // and also because superclass does not throw exception
            throw new RuntimeException("Unable to compute TOKENIZE schema.");
        }   
    }

    
};
