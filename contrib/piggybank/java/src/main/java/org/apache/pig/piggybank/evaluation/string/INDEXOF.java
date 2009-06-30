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
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.FuncSpec;


/**
 * string.INSTR implements eval function to search for a string
 * Example:
 *      register pigudfs.jar;
 *      A = load 'mydata' as (name);
 *      B = foreach A generate string.INDEXOF(name, ",");
 *      dump B;
 */
public class INDEXOF extends EvalFunc<Integer>
{
    /**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the column to convert
     *                     the second column is the string we search for
     *                     the third is an optional column from where to start the search
     * @exception java.io.IOException
     */
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        try{
            String str = (String)input.get(0);
            String search = (String)input.get(1);
            int fromIndex = 0;
            if (input.size() ==3)
                fromIndex = (Integer)input.get(1);
            return str.indexOf(search, fromIndex);
        }catch(Exception e){
            System.err.println("Failed to process input; error - " + e.getMessage());
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.INTEGER));
    }

}