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
 * math.getExponent implements a binding to the Java function
* {@link java.lang.Math#getExponent(double) Math.getExponent(double)}. 
* Given a single data atom it returns the unbiased exponent used in 
* the representation of a double
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>double</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>int</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>getExponent_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.getExponent(float1);
* </code></dd>
* </dl>
* 
* @see Math#getExponent(double)
* @see
* @author ajay garg
*
*/
public class DoubleGetExponent extends EvalFunc<Integer>{
	/**
	 * java level API
	 * @param input expects a single numeric value
	 * @param output returns a single numeric value, unbiased 
	 * exponent used in the representation of a double
	 */
	public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        try {
            Double d = (Double)input.get(0);
		    return Math.getExponent(d);
        } catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }
	
	@Override
	public Schema outputSchema(Schema input) {
         return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
	}
}
