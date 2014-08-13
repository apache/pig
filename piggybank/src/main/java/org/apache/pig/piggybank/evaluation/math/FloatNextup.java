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
 * math.NEXTUP implements a binding to the Java function
* {@link java.lang.Math#nextUp(double) Math.nextUp(double)}. 
* Given a single data atom it return the floating-point value 
* adjacent to input in the direction of positive infinity
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Float</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>Float</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>NEXTUP_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.NEXTUP(float1);
* </code></dd>
* </dl>
* 
* @see Math#nextUp(float)
* @see
* @author ajay garg
*
*/
public class FloatNextup extends EvalFunc<Float>{
    /**
     * java level API
     * @param input expects a single numeric value
     * @param output returns a single numeric value, nextup value of the argument
     */
    public Float exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        Float d;
        try{
           d = (Float)input.get(0);
        } catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }

		return Math.nextUp(d);
}

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.FLOAT));
    }
}
