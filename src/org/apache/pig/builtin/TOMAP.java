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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This class makes a map out of the parameters passed to it
 * T = foreach U generate TOMAP($0, $1, $2, $3);
 * It generates a map $0->1, $2->$3
 */
public class TOMAP extends EvalFunc<Map> {

    @Override
    public Map exec(Tuple input) throws IOException {
	if (input == null || input.size() < 2)
		return null;
        try {
	    Map<String, Object> output = new HashMap<String, Object>();

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
        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }

}
