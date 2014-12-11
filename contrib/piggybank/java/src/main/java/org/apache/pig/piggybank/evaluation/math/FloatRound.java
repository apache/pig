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

/**
 * math.ROUND implements a binding to the Java function
* {@link java.lang.Math#round(float) Math.round(float)}. 
* Given a single data atom it Returns the closest long to the argument.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Float</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>Integer</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>ROUND_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.ROUND(float1);
* </code></dd>
* </dl>
* 
* @see Math#round(float)
* @see
* @author ajay garg
*
*/

/**
 * @deprecated Use {@link org.apache.pig.builtin.FloatRound}
 */
@Deprecated 

public class FloatRound extends EvalFunc<Integer>{
	/**
	 * java level API
	 * @param input expects a single numeric value
	 * @param output returns a single numeric value, 
	 * the closest long to the argument
	 */
	@Override
	public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null)
            return null;

        try{
            Float d =  (Float)input.get(0);
		    return Math.round(d);
        } catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
	}
	
	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
	}
}
