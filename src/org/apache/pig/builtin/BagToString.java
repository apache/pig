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

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**	
 * Flatten a bag into a string.  This UDF will the character '-' as the default delimiter 
 * if one is not provided.
 * 
 * Example: 
 *
 * bag = {(a),(b),(c)}
 * BagToString(bag) -> 'a_b_c'
 *
 * BagToString(bag, '+') -> 'a+b+c'
 * 
 * bag = {(a,b), (c,d), (e,f)} 
 * BagToString(bag) --> 'a_b_c_d_e_f'
 * 
 * If input bag is null, this UTF will return null;
 */
public class BagToString extends EvalFunc<String> {

	private static final String USAGE_STRING = "Usage BagToString(dataBag) or BagToString(dataBag, delimiter)";
	private static final String DEFAULT_DELIMITER = "_";
	
	@Override
	public String exec(Tuple inputTuple) throws IOException {
		if ((inputTuple.size() != 1) && (inputTuple.size() != 2)) {
			throw new ExecException(USAGE_STRING, PigException.INPUT);
		}
		
		Object firstArg = inputTuple.get(0);
		if (firstArg == null) {
			return null;			
		}
	
		if (!(firstArg instanceof DataBag)) {
			  throw new ExecException(USAGE_STRING + " found type " + firstArg.getClass().getName(), PigException.INPUT);			
		}
		
		if ((inputTuple.size() == 2) && !(inputTuple.get(1) instanceof String)) {
			  throw new ExecException("Usage BagToTuple(DataBag, String)", PigException.INPUT);			
		}
		
		DataBag bag = (DataBag) inputTuple.get(0);
		
		String delimeter = DEFAULT_DELIMITER;
		if (inputTuple.size() == 2) {
			delimeter = (String)inputTuple.get(1);
		}
		StringBuilder buffer = new StringBuilder();

		try {
			for (Tuple t : bag) {
				if (t != null) {
					for (int i = 0; i < t.size(); i++) {
						if (buffer.length() > 0) {
							buffer.append(delimeter);
						}
						buffer.append(t.get(i));
					}
				}
			}
			
			return buffer.toString();
		} catch (Exception e) {
			String msg = "Encourntered error while flattening a bag "
					+ this.getClass().getSimpleName();
			throw new ExecException(msg, PigException.BUG, e);
		}
	}


	@Override
	public Schema outputSchema(Schema inputSchema) {
		try {
			if ((inputSchema == null) || ((inputSchema.size() != 1) && (inputSchema.size() != 2))) {
				throw new RuntimeException("Expecting 2 inputs, found: " + 
						((inputSchema == null) ? 0 : inputSchema.size()));
			}

			FieldSchema inputFieldSchema = inputSchema.getField(0);
			if (inputFieldSchema.type != DataType.BAG) {
				throw new RuntimeException("Expecting a bag of tuples: {()}, found data type: " + 
						DataType.findTypeName(inputFieldSchema.type));
			}

			// first field in the bag schema
			FieldSchema firstFieldSchema = inputFieldSchema.schema.getField(0);
			if ((firstFieldSchema == null) || (firstFieldSchema.schema == null)
					|| firstFieldSchema.schema.size() < 1) {
				throw new RuntimeException("Expecting a bag and a delimeter, found: " + inputSchema);
			}

			if (firstFieldSchema.type != DataType.TUPLE) {
				throw new RuntimeException("Expecting a bag and a delimeter, found: " + inputSchema);
			}
			
			if (inputSchema.size() == 2) {
				FieldSchema secondInputFieldSchema = inputSchema.getField(1);
	
				if (secondInputFieldSchema.type != DataType.CHARARRAY) {
					throw new RuntimeException("Expecting a bag and a delimeter, found: " + inputSchema);
				}
			}

			return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
		} catch (FrontendException e) {
			e.printStackTrace();
			return null;
		}
	}

}
