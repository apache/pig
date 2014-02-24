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

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Given a string, this UDF replaces a substring given its starting index and
 * length with the given replacement string. If the the last argument is null, the specified
 * part of the string gets deleted. 
 * 
 * B = FOREACH A GENERATE Stuff($0, 10, 4, 'Pie')
 * If $0 is "Chocolate Cake" then the UDF will return "Chocolate Pie"
 * 
 **/

public class Stuff extends EvalFunc<String> {

	public String exec(Tuple input) throws IOException {
		int inputSize = input.size();

		if (input == null || inputSize == 0 || input.get(0) == null) {
			warn("Null input", PigWarning.UDF_WARNING_1);
			return null;
		}

		if (inputSize != 4) {
			throw new IOException("Stuff requires 4 arguments");
		}

		String inString = (String) input.get(0);
		
		Integer startIndex = null;
		Object inStartIndex = input.get(1);
		//handle Double and Float
		if(inStartIndex instanceof Number){
			startIndex = ((Number)inStartIndex).intValue();
		}else{
			warn("Specified startIndex is of type " + inStartIndex.getClass().getName() + ", only Numbers are supported", PigWarning.UDF_WARNING_1);
			return null;
		}
		
		Integer length = null;
		Object inLength = input.get(2);
		
		//handle Double and Float
		if(inLength instanceof Number){
			length = ((Number)inLength).intValue();
		}else{
			warn("Specified length is of type " + inLength.getClass().getName() + ", only Numbers are supported", PigWarning.UDF_WARNING_1);
			return null;
		}

		String replacementString = (String) input.get(3);

		int strLength = inString.length();
		if (startIndex < 0 || startIndex >= strLength) {
			throw new IOException("Given startIndex " + startIndex
					+ " is out of bounds: [0," + strLength + ")");
		}

		if (length < 0) {
			throw new IOException(
					"The number of characters to delete cannot be negative");
		}

		StringBuffer result = new StringBuffer();

		int upperBound = (startIndex + length > strLength) ? strLength
				: startIndex + length;

		for (int i = 0; i < strLength;) {
			// need to replace these characters
			if (i >= startIndex && i < upperBound) {
				if(replacementString != null)
					result.append(replacementString);
				i += length;
			} else {
				result.append(inString.charAt(i));
				i++;
			}
		}

		return result.toString();
	}

	/**
	 * @param input
	 *            , schema of the input data
	 * @return output schema
	 */
	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}
}
