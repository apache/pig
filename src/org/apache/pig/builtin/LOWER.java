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
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.FuncSpec;

/**
 * LOWER implements eval function to convert a string to lower case
 * Example:
 *      A = load 'mydata' as (name);
 *      B = foreach A generate LOWER(name);
 */
public class LOWER extends EvalFunc<String> {

    /**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the column to convert
     * @exception java.io.IOException
     */
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        try {
            String str = (String)input.get(0);
            return str.toLowerCase();
        } catch(Exception e){
            warn("Failed to process input; error - " + e.getMessage(), PigWarning.UDF_WARNING_1);
            return null;
        }
    }

    /**
     * This method gives a name to the column.
     * @param input - schema of the input data
     * @return schema of the input data
     */
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.CHARARRAY));
    }

     /* (non-Javadoc)
      * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
      */
     @Override
     public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
     }

}