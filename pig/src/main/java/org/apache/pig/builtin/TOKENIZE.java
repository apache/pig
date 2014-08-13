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
import java.util.ArrayList;
import java.util.List;
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
import org.apache.pig.FuncSpec;

/**
 * Given a chararray as an argument, this method will split the chararray and
 * return a bag with a tuple for each chararray that results from the split.
 * The string is split on space, double quote, comma, open parend, close parend,
 * and asterisk (star).
 */
public class TOKENIZE extends EvalFunc<DataBag> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            if (input==null)
                return null;
            if (input.size()==0)
                return null;
            Object o = input.get(0);
            if (o==null)
                return null;
            DataBag output = mBagFactory.newDefaultBag();
            if (!(o instanceof String)) {
            	int errCode = 2114;
            	String msg = "Expected input to be chararray, but" +
                " got " + o.getClass().getName();
                throw new ExecException(msg, errCode, PigException.BUG);
            }
            
            String delim = " \",()*";
            if (input.size()==2) {
                Object d = input.get(1);
                if (!(d instanceof String)) {
                    int errCode = 2114;
                    String msg = "Expected delim to be chararray, but" +
                        " got " + d.getClass().getName();
                    throw new ExecException(msg, errCode, PigException.BUG);
                }
                delim = (String)d;
            }

            StringTokenizer tok = new StringTokenizer((String)o, delim, false);
            while (tok.hasMoreTokens()) {
                output.add(mTupleFactory.newTuple(tok.nextToken()));
            }
            return output;
        } catch (ExecException ee) {
            throw ee;
        }
    }

    @SuppressWarnings("deprecation")
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
                        "bag_of_tokenTuples_from_" + input.getField(0).alias, bagSchema, DataType.BAG);
                    
            return new Schema(bagFs); 
            
            
            
        } catch (FrontendException e) {
            // throwing RTE because
            //above schema creation is not expected to throw an exception
            // and also because superclass does not throw exception
            throw new RuntimeException("Unable to compute TOKENIZE schema.");
        }   
    }

    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
};
