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
package org.apache.pig.piggybank.evaluation.string;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
* <dl>
* <dt><b>Syntax:</b></dt>
* <dd><code>long HashFNV(String string_to_hash, [int mod])</code>.</dd>
* </dl>
*/

public class HashFNV extends EvalFunc<Long> {
    static final int FNV1_32_INIT = 33554467;
    static final int FNV_32_PRIME = 0x01000193;
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
        } catch (Exception e) {
          return null;
        }
    }


    long hashFnv32Init(int init, String s)
    {
        int hval = init;

        byte[] bytes = null;
        try {
            bytes = s.getBytes("UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            // shall not happen
        }
        for (int i=0;i<bytes.length;i++)
        {
            /* multiply by the 32 bit FNV magic prime mod 2^32 */
            hval *= FNV_32_PRIME;
            hval ^= bytes[i];
        }
        return hval;
    }

    long hashFnv32(String s)
    {
        return hashFnv32Init(FNV1_32_INIT, s);
    }
    
    public Long exec(Tuple input) throws IOException { throw new IOException("HashFNV: internal error, try to use HashFNV directly"); }
    
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(HashFNV1.class.getName(), s));
        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.INTEGER));
        funcList.add(new FuncSpec(HashFNV2.class.getName(), s));
        return funcList;
    } 
}
