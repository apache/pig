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
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This class makes a map out of the parameters passed to it
 * T = foreach U generate TOMAP($0, $1, $2, $3);
 * It generates a map $0->1, $2->$3
 *
 * This UDF also accepts a bag with 'pair' tuples (i.e. tuples with a 'key' and a 'value').
 *
 */
public class TOMAP extends EvalFunc<Map> {

    @Override
    public Map exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        Map<String, Object> output = new HashMap<String, Object>();

        try {
            // Is this a single bag with all the values?
            if (input.size() == 1) {
                if (input.get(0) instanceof DataBag) {
                    DataBag bagOfPairs = (DataBag)input.get(0);
                    if (bagOfPairs.size() == 0) {
                        return output;
                    }

                    for (Tuple tuple: bagOfPairs) {
                        if (tuple.size() != 2) {
                            throw new RuntimeException("All input tuples in the bag MUST have exactly 2 fields");
                        }
                        String key = (String)tuple.get(0);
                        Object val = tuple.get(1);
                        output.put(key, val);
                    }
                    return output;
                } else {
                    return null; // If only 1 value then it must be a bag
                }
            }

            for (int i = 0; i < input.size(); i=i+2) {
                String key = (String)input.get(i);
                Object val = input.get(i+1);
                output.put(key, val);
            }
            return output;
        } catch (ClassCastException e){
            throw new RuntimeException("Map key must be a String");
        } catch (ArrayIndexOutOfBoundsException e){
            throw new RuntimeException("Function input must have even number of parameters");
        } catch (Exception e) {
            throw new RuntimeException("Error while creating a map", e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        Byte valueType = null;
        if (input.size() == 1) {
            // If input is bag with 'pair' tuples
            Schema bagSchema = input.getFields().get(0).schema;
            if (bagSchema != null && bagSchema.size() == 1) {
                Schema tupleSchema = bagSchema.getFields().get(0).schema;
                if (tupleSchema != null) {
                    valueType = tupleSchema.getFields().get(1).type;
                }
            }
        } else if (input != null && input.getFields()!=null) {
            for (int i=0;i<input.size();i++) {
                if (i % 2 == 1) {
                    if (valueType == null) {
                        valueType = input.getFields().get(i).type;
                    } else if  (valueType != input.getFields().get(i).type) {
                        valueType = DataType.BYTEARRAY;
                        break;
                    }
                }
            }
        }
        Schema s = new Schema(new Schema.FieldSchema(null, DataType.MAP));
        if (valueType != null && valueType != DataType.BYTEARRAY) {
            s.getFields().get(0).schema = new Schema(new Schema.FieldSchema(null, valueType));
            return s;
        }
        return s;
    }

    @Override
    public boolean allowCompileTimeCalculation() {
        return true;
    }
}
