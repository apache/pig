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
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Flatten a bag into a tuple.  This UDF performs only flattening at the first level, 
 * it doesn't recursively flatten nested bags.
 * 
 * Example: {(a),(b),(c)} --> (a,b,c)
 *          {(a,b), (c,d), (e,f)} --> (a,b,c,d,e,f);
 * 
 * If input bag is null, this UDF will return null;
 * 
 */
public class BagToTuple extends EvalFunc<Tuple> {

	@Override
	public Tuple exec(Tuple inputTuple) throws IOException {

		if (inputTuple.size() != 1) {
			throw new ExecException("Expecting 1 input, found " + inputTuple.size(), PigException.INPUT);
		}
		
		if (inputTuple.get(0) == null) {
			return null;
		}
		
		if (!(inputTuple.get(0) instanceof DataBag)) {
		  throw new ExecException("Usage BagToTuple(DataBag)", PigException.INPUT);			
		}
		
		
		DataBag inputBag = (DataBag) (inputTuple.get(0));
		try {
			Tuple outputTuple = null;
			
			long outputTupleSize = getOuputTupleSize(inputBag);

			// TupleFactory.newTuple(int size) can only support up to Integer.MAX_VALUE
			if (outputTupleSize > Integer.MAX_VALUE) {
				throw new ExecException("Input bag is too large", 105, PigException.INPUT);
			}

			TupleFactory tupleFactory = TupleFactory.getInstance();
			outputTuple = tupleFactory.newTuple((int) outputTupleSize);

			int fieldNum = 0;
			for (Tuple t : inputBag) {
				if (t != null) {
					for (int i = 0; i < t.size(); i++) {
						outputTuple.set(fieldNum++, t.get(i));
					}
				}
			}
			return outputTuple;
		} catch (Exception e) {
			String msg = "Encourntered error while flattening a bag to tuple"
					+ this.getClass().getSimpleName();
			throw new ExecException(msg, PigException.BUG, e);
		}
	}

	/**
	 * Calculate the size of the output tuple based on the sum
     * of the size of each tuple in the input bag
	 * 
	 * @param bag
	 * @return total # of data elements in a tab
	 */
	private long getOuputTupleSize(DataBag bag) {
		long size = 0;
		if (bag != null) {
			for (Tuple t : bag) {
				size = size + t.size();
			}
		}
		return size;
	}

	@Override
	public Schema outputSchema(Schema inputSchema) {
		try {
			if ((inputSchema == null) || inputSchema.size() != 1) {
				throw new RuntimeException("Expecting 1 input, found " + 
						((inputSchema == null) ? 0 : inputSchema.size()));
			}

			Schema.FieldSchema inputFieldSchema = inputSchema.getField(0);
			if (inputFieldSchema.type != DataType.BAG) {
				throw new RuntimeException("Expecting a bag of tuples: {()}");
			}

			// first field in the bag schema
			Schema.FieldSchema firstFieldSchema = inputFieldSchema.schema.getField(0);
			if ((firstFieldSchema == null) || (firstFieldSchema.schema == null)
					|| firstFieldSchema.schema.size() < 1) {
				throw new RuntimeException("Expecting a bag of tuples: {()}, found: " + inputSchema);
			}

			if (firstFieldSchema.type != DataType.TUPLE) {
				throw new RuntimeException("Expecting a bag of tuples: {()}, found: " + inputSchema);
			}

			// now for output schema
			Schema tupleOutputSchema = new Schema();
			for (int i = 0; i < firstFieldSchema.schema.size(); ++i) {
				tupleOutputSchema.add(firstFieldSchema.schema.getField(i));
			}
			return new Schema(new Schema.FieldSchema(getSchemaName(this
					.getClass().getName().toLowerCase(), inputSchema), tupleOutputSchema,
					DataType.TUPLE));
		} catch (FrontendException e) {
			e.printStackTrace();
			return null;
		}
	}

}
