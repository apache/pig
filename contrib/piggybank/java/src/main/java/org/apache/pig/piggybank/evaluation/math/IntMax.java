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
  * math.max implements a binding to the Java function
 * {@link java.lang.Math#max(int,int) Math.max(int,int)} for computing the
 * the max value of the arguments. The returned value will be an int which is
 * the maximum of the inputs.
 *
 * <dl>
 * <dt><b>Parameters:</b></dt>
 * <dd><code>value</code> - <code>2 int values</code>.</dd>
 *
 * <dt><b>Return Value:</b></dt>
 * <dd><code>int</code> max value of two input</dd>
 *
 * <dt><b>Return Schema:</b></dt>
 * <dd>max_inputSchema</dd>
 *
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register math.jar;<br/>
 * A = load 'mydata' using PigStorage() as ( float1 );<br/>
 * B = foreach A generate float1, math.max(float1);
 * </code></dd>
 * </dl>
 *
 * @see Math#max(double)
 * @see
 * @author ajay garg
 *
 */
public class IntMax extends EvalFunc<Integer>{
    /**
     * java level API
     * @param input expects a two numeric value
     * @param output returns a single numeric value, which is the smaller of two inputs
     */
    public Integer exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        try{
            Integer first = (Integer)input.get(0);
            Integer second = (Integer)input.get(1);
            if (first==null)
                return second;
            if (second==null)
                return first;
            return Math.max(first, second);
        } catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
    }
}
