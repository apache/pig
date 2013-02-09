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

package org.apache.pig.test.udf.evalfunc;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.PigWarning;

public class TestWarningFunc extends EvalFunc<Double>
{
	//@Override
	public Double exec(Tuple input) throws IOException 
	{
		if (input == null || input.size() == 0) {
            pigLogger.warn(this, "Input is empty.", PigWarning.UDF_WARNING_1); 
			return null;
        }

        Double output = null;
        boolean accumulated = false;

		try {
            for(int i = 0; i < input.size(); ++i) {
                Object o = input.get(i);
                byte inputType = DataType.findType(o);
                if(DataType.isNumberType(inputType)) {
                    if(!accumulated) {
                        output = 0.0;
                        accumulated = true;
                    }
                    switch(inputType) {
                    case DataType.INTEGER:
                        output += (Integer)o;
                        break;

                    case DataType.LONG:
                        output += (Long)o;
                        break;

                    case DataType.FLOAT:
                        output += (Float)o;
                        break;

                    case DataType.DOUBLE:
                        output += (Double)o;
                        break;
                    }

                } else {
                    pigLogger.warn(this, "Found a non-numeric type.", PigWarning.UDF_WARNING_3);
                }
            }
		} catch(Exception e){
            pigLogger.warn(this, "Problem while computing output.", PigWarning.UDF_WARNING_2); 
			return null;
		}

        if(!accumulated) {
            pigLogger.warn(this, "Did not find any numeric type in the input.", PigWarning.UDF_WARNING_4);
        }

		return output;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName("output", input), DataType.DOUBLE));
    }
}
