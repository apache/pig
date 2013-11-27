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
  * math.min implements a binding to the Java function
 * {@link java.lang.Math#min(long,long) Math.min(long,long)} for computing the
 * the min value of the arguments. The returned value will be an nt which is
 * the minimum of the inputs.
 *
 * <dl>
 * <dt><b>Parameters:</b></dt>
 * <dd><code>value</code> - <code>2 long values</code>.</dd>
 *
 * <dt><b>Return Value:</b></dt>
 * <dd><code>long</code> min value of two input</dd>
 *
 * <dt><b>Return Schema:</b></dt>
 * <dd>min_inputSchema</dd>
 *
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register math.jar;<br/>
 * A = load 'mydata' using PigStorage() as ( float1 );<br/>
 * B = foreach A generate float1, math.min(float1);
 * </code></dd>
 * </dl>
 *
 * @see Math#min(double)
 * @see
 * @author ajay garg
 *
 */
public class LongMin extends EvalFunc<Long>{
    /**
     * java level API
     * @param input expects a two numeric value
     * @param output returns a single numeric value, which is the smaller of two inputs
     */
    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        try{
            Long first = (Long)input.get(0);
            Long second = (Long)input.get(1);
            if (first==null)
                return first;
            if (second==null)
                return second;
            return Math.min(first, second);
        } catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
    }
}
