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

package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * math.copySign implements a binding to the Java function
* {@link java.lang.Math#copySign(float,float) Math.copySign(float,float)}. 
* Given a tuple with two data atom Returns the first floating-point argument 
* with the sign of the second floating-point argument.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Tuple containing two float</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>float</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>copySign_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.copySign(float1);
* </code></dd>
* </dl>
* 
* @see Math#copySign(float,float)
* @see
* @author ajay garg
*
*/
public class FloatCopySign extends EvalFunc<Float>{
	/**
	 * java level API
	 * @param input expects a tuple containing two numeric DataAtom value
	 * @param output returns a single numeric DataAtom value, which is 
	 * first floating-point argument with the sign of the second 
	 * floating-point argument.
	 */
	@Override
	public Float exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;
		try{
			float first =  (Float)input.get(0);
			float second = (Float)input.get(1);
			return Math.copySign(first, second);
        } catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.FLOAT));
	}
}
