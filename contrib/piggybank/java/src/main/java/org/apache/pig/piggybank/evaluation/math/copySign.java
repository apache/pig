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
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.FuncSpec;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * math.copySign implements a binding to the Java function
* {@link java.lang.Math#copySign(double,double) Math.copySign(double,double)}. 
* Given a tuple with two data atom Returns the first floating-point argument 
* with the sign of the second floating-point argument.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Tuple containing two double</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>double</code> </dd>
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
* @see Math#copySign(double,double)
* @see
* @author ajay garg
*
*/
public class copySign extends EvalFunc<Double>{
	/**
	 * java level API
	 * @param input expects a tuple containing two numeric DataAtom value
	 * @param output returns a single numeric DataAtom value, which is 
	 * first floating-point argument with the sign of the second 
	 * floating-point argument.
	 */
	@Override
	public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;
        if (input.get(0) == null || input.get(1) == null) {
            return null;
        }
		try{
			double first =  DataType.toDouble(input.get(0));
			double second = DataType.toDouble(input.get(1));
			return Math.copySign(first, second);
		} catch (NumberFormatException nfe){
            System.err.println("Failed to process input; error - " + nfe.getMessage());
            return null;
        } catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
		}
	}
	
	@Override
	public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.DOUBLE));
	}

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Util.addToFunctionList(funcList, this.getClass().getName(), DataType.BYTEARRAY);
        Util.addToFunctionList(funcList, DoubleCopySign.class.getName(), DataType.DOUBLE);
        Util.addToFunctionList(funcList, FloatCopySign.class.getName(), DataType.FLOAT);

        return funcList;
    }
}
