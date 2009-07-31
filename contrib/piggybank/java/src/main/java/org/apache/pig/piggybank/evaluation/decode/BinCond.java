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
* <dd><code>String BinCond(boolean_expression1, mapping_string1, ..., boolean_expressionN, mapping_stringN, other_string)</code>.</dd>
* <dt><b>Logic:</b></dt>
* <dd><code>if      (boolean_expression1) return mapping_string1; <br>
* ...... <br>
* else if (boolean_expressionN) return mapping_stringN; <br>
* else                          return other_string; <br>
* <br></code></dd>
* </dl>
*/

public class BinCond extends EvalFunc<String> {
    int numParams=-1;

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
        if (numParams==-1)  // Not initialized
        {
            numParams = tuple.size();
            if (numParams <= 2) {
                String msg = "BinCond : UDF requires atleast one condition, mapping string & a default string.";
                throw new IOException(msg);
            }
            if (tuple.size()%2!=1) {
                String msg = "BinCond : Some parameters are unmatched.";
                throw new IOException(msg);
            }
        }
        
        if (tuple.get(0)==null)
            return null;
        
        try {
            for (int count = 0; count < numParams; count += 2) {
                int returnIndex = -1;

                if (count == numParams - 1) {
                    returnIndex = count;
                } else if ((Boolean) tuple.get(count)) {
                    returnIndex = count + 1;
                }

                if (returnIndex >= 0) {
                    return (String) tuple.get(returnIndex);
                }
            }
        } catch (ClassCastException e) {
            warn("BinCond : Data type error", PigWarning.UDF_WARNING_1);
            return null;
        }  catch (NullPointerException e) {
            String msg = "BinCond : Encounter null in the input";
            throw new IOException(msg);
        }
        String msg = "BinCond : Internal Failure";
        throw new IOException(msg);
    }
}
