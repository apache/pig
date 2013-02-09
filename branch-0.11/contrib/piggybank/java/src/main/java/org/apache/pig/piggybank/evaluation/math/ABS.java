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

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
  * math.ABS implements a binding to the Java function
 * {@link java.lang.Math#abs(double) Math.abs(double)} for computing the
 * absolute value of the argument. The returned value will be a double which is 
 * absolute value of the input.
 * 
 * <dl>
 * <dt><b>Parameters:</b></dt>
 * <dd><code>value</code> - <code>numeric</code>.</dd>
 * 
 * <dt><b>Return Value:</b></dt>
 * <dd><code>numeric</code> absolute value of input</dd>
 * 
 * <dt><b>Return Schema:</b></dt>
 * <dd>abs_inputSchema</dd>
 * 
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register math.jar;<br/>
 * A = load 'mydata' using PigStorage() as ( float1 );<br/>
 * B = foreach A generate float1, math.ABS(float1);
 * </code></dd>
 * </dl>
 * 
 * @see Math#abs(double)
 * @see
 * @author ajay garg
 *
 */

/**
 * @deprecated Use {@link org.apache.pig.builtin.ABS}
 */

@Deprecated 
public class ABS extends EvalFunc<Double>{
	/**
	 * java level API
	 * @param input expects a single numeric value
	 * @param output returns a single numeric value, absolute value of the argument
	 */
	public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        Double d;
        try{
            d = DataType.toDouble(input.get(0));
        } catch (NumberFormatException nfe){
            System.err.println("Failed to process input; error - " + nfe.getMessage());
            return null;
        } catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }

		return Math.abs(d);
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
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.BYTEARRAY))));
        funcList.add(new FuncSpec(DoubleAbs.class.getName(),  new Schema(new Schema.FieldSchema(null, DataType.DOUBLE))));
        funcList.add(new FuncSpec(FloatAbs.class.getName(),   new Schema(new Schema.FieldSchema(null, DataType.FLOAT))));
        funcList.add(new FuncSpec(IntAbs.class.getName(),  new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));
        funcList.add(new FuncSpec(LongAbs.class.getName(),  new Schema(new Schema.FieldSchema(null, DataType.LONG))));
        return funcList;
    }

}
