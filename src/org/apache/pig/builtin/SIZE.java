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

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Generates the size of the argument passed to it.  For bytearrays this
 * means the number of bytes.  For charrays the number of characters.  For
 * bags the number of tuples, for tuples the number of fields, and for maps
 * the number of keyvalue pairs.  For all other types the value of 1 is
 * always returned.
 */
public class SIZE extends EvalFunc<Long> {

    @Override
    public Long exec(Tuple input) throws IOException {
        try {
            DataByteArray dba = (DataByteArray)(input.get(0));
            return dba == null ? null : Long.valueOf(dba.size());
        } catch (ExecException exp) {
            throw exp;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing size in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);           
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.LONG)); 
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(StringSize.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.TUPLE));
        funcList.add(new FuncSpec(TupleSize.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.BAG));
        funcList.add(new FuncSpec(BagSize.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.MAP));
        funcList.add(new FuncSpec(MapSize.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.INTEGER));
        funcList.add(new FuncSpec(ConstantSize.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.LONG));
        funcList.add(new FuncSpec(ConstantSize.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.FLOAT));
        funcList.add(new FuncSpec(ConstantSize.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.DOUBLE));
        funcList.add(new FuncSpec(ConstantSize.class.getName(), s));
        return funcList;
    }    

}
