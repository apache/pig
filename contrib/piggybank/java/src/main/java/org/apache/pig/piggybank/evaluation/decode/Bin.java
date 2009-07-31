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
package org.apache.pig.piggybank.evaluation.decode;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
* <dl>
* <dt><b>Syntax:</b></dt>
* <dd><code>String Bin(arithmetic_expression, string1, ,..., stringN, sentinelN, default_string)</code>.</dd>
* <dt><b>Logic:</b></dt>
* <dd><code>if      (arithmetic_expression<=sentinel1) return string1; <br>
* ...... <br>
* else if (arithmetic_expression<=sentinelN) return stringN; <br>
* else                                       return default_string; <br></code>
* <br>
* arithmetic_expression can only be numeric types.</dd>
* </dl>
*/

public class Bin extends EvalFunc<String> {

    int numParams = -1;
    @Override
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(new Schema.FieldSchema(getSchemaName(this
                    .getClass().getName().toLowerCase(), input),
                    DataType.CHARARRAY));
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String exec(Tuple tuple) throws IOException {
        if (numParams == -1)  // not initialized
        {
            numParams = tuple.size();
            if (numParams <= 2) {
                String msg = "Bin : An expression & atleast a default string are required.";
                throw new IOException(msg);
            }
            if (tuple.size()%2!=0) {
                String msg = "Bin : Some parameters are unmatched.";
                throw new IOException(msg);
            }
        }
        
        if (tuple.get(0)==null)
            return null;

        try {
            for (int count = 1; count < numParams; count += 2) {
                if ((count == numParams - 1)
                        || ((Number)tuple.get(0)).doubleValue() <= ((Number)tuple.get(count + 1)).doubleValue()) {

                    return (String) tuple.get(count);
                }
            }
        } catch (ClassCastException e) {
            warn("Bin : Data type error", PigWarning.UDF_WARNING_1);
            return null;
        } catch (NullPointerException e)
        {
            String msg = "Bin : Encounter null in the input";
            throw new IOException(msg);
        }
        String msg = "Bin : Internal Failure";
        throw new IOException(msg);
    }
}
