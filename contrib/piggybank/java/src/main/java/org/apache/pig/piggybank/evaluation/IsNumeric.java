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

package org.apache.pig.piggybank.evaluation;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This UDF is used to check if a String is numeric. Note this UDF is different
 * from IsInt in 2 ways, 1. Does not check for Integer range 2. Runs faster as
 * this UDF uses Regex match and not Integer.parseInt(String)
 * 
 * This UDF is expected to perform slightly better than isInt, isLong, isFloat,
 * isDouble. However, primary goal of this UDF is NOT performance but rather to
 * check "numeric"-ness of a String. Use this UDF if you do not care for the
 * type (int, long, float, double) and would just like to check if its numeric
 * in nature.
 * 
 * It does NOT check for Range of int, long, double, parse. This function will
 * return true when the String is larger than the range of any numeric data type
 * (int, long, double, float). Use specific functions (IsInt, IsFloat, IsLong,
 * IsDouble) if range is important.
 * 
 */
public class IsNumeric extends EvalFunc<Boolean> {

	@Override
	public Boolean exec(Tuple input) throws IOException {	
		if (input == null || input.size() == 0)
			return false;
		try {
			String str = (String) input.get(0);
			if (str == null || str.length() == 0)
				return false;

			if (str.startsWith("-"))
				str = str.substring(1);

			return str.matches("\\d+(\\.\\d+)?");

		} catch (ClassCastException e) {
			warn("Unable to cast input " + input.get(0) + " of class "
					+ input.get(0).getClass() + " to String",
					PigWarning.UDF_WARNING_1);
			return false;
		}
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
	}
}
