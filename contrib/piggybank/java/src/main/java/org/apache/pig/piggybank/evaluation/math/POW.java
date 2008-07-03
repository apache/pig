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
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * math.POW implements a binding to the Java function
* {@link java.lang.Math#pow(double,double) Math.pow(double,double)}. 
* Given a tuple with two data atom it Returns the 
* the value of the first argument raised to the power of 
* the second argument.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Tuple containing two DataAtom [double]</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataAtom [double]</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>POW_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.POW(float1);
* </code></dd>
* </dl>
* 
* @see Math#pow(double,double)
* @see
* @author ajay garg
*
*/
public class POW extends EvalFunc<DataAtom>{
	/**
	 * java level API
	 * @param input expects a tuple containing two numeric DataAtom value
	 * @param output returns a single numeric DataAtom value, which is 
	 * value of the first argument raised to the power of the second argument.
	 */
	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(pow(input));
	}
	
	protected double pow(Tuple input) throws IOException{
		try{
			double first = input.getAtomField(0).numval();
			double second = input.getAtomField(1).numval();
			return Math.pow(first, second);
		}
		catch(RuntimeException e){
			throw new IOException("invalid input "+e.getMessage());
		}
		
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new AtomSchema("POW_"+input.toString()); 
	}


}
