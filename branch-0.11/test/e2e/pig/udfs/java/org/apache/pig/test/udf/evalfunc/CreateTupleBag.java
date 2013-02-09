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
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

public class CreateTupleBag extends EvalFunc<DataBag>
{
	//@Override
	public DataBag exec(Tuple input) throws IOException 
	{
		if (input == null || input.size() < 2)
		{
			return null;
		}

		Object f1, f2;
		try{
        		f1 = input.get(0);
        		f2 = input.get(1);
		}catch(Exception e){
			System.err.println("Failed to process input data; error: " + e.getMessage());
			return null;
		}

		Tuple t1 = DefaultTupleFactory.getInstance().newTuple(2);
		Tuple t2 = DefaultTupleFactory.getInstance().newTuple(2);
		try{
			t1.set(0, f1);
			t1.set(1, f2);
			t2.set(0, f2);
			t2.set(1, f1);
		}catch(Exception e){}

		DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
		output.add(t1);
		output.add(t2);

		return output;
        }

	@Override
        public Schema outputSchema(Schema input) {
                return new Schema(new Schema.FieldSchema(getSchemaName("createmap", input), DataType.BAG));
        }
}
