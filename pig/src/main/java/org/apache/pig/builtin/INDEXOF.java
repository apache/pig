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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * INDEXOF implements eval function to search for a string
 * Example:
 *      A = load 'mydata' as (name);
 *      B = foreach A generate INDEXOF(name, ",");
 */
public class INDEXOF extends EvalFunc<Integer> {

    private static final Log log = LogFactory.getLog(INDEXOF.class);

    /**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; first column is assumed to have the column to convert
     *                     the second column is the string we search for
     *                     the third is an optional column from where to start the search
     * @exception java.io.IOException
     * @return index of first occurrence, or null in case of processing error
     */
    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            warn("invalid input tuple: "+input, PigWarning.UDF_WARNING_1);
            return null;
        }
        try {
            String str = (String)input.get(0);
            String search = (String)input.get(1);
            int fromIndex = 0;
            if (input.size() >=3)
                fromIndex = (Integer)input.get(2);
            return str.indexOf(search, fromIndex);
        } catch(Exception e){
            warn("Failed to process input; error - " + e.getMessage(), PigWarning.UDF_WARNING_1);
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.INTEGER));
    }

}