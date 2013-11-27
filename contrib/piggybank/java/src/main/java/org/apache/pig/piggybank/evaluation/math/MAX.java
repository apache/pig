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
 * math.MAX implements a binding to the Java function
 * {@link java.lang.Math#max(double,double) Math.max(double,double)}.
 * Given a tuple with two numeric values it returns the
 * large value.
 *
 * <dl>
 * <dt><b>Parameters:</b></dt>
 * <dd><code>value</code> - <code>Tuple containing two double</code>.</dd>
 *
 * <dt><b>Return Value:</b></dt>
 * <dd><code>double</code> </dd>
 *
 * <dt><b>Return Schema:</b></dt>
 * <dd>MAX_inputSchema</dd>
 *
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register math.jar;<br/>
 * A = load 'mydata' using PigStorage() as ( float1 );<br/>
 * B = foreach A generate float1, math.MAX(float1);
 * </code></dd>
 * </dl>
 *
 * @see Math#MAX(double,double)
 * @see
 * @author ajay garg
 *
 */
public class MAX extends EvalFunc<Double>{
    /**
     * java level API
     * @param input expects a tuple containing two numeric DataAtom value
     * @param output returns a single numeric value, which is
     * the the larger value among the two.
     */
    @Override
    public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        try{
            Double first = DataType.toDouble(input.get(0));
            Double second  = DataType.toDouble(input.get(1));
            if (first==null)
                return second;
            if (second==null)
                return first;
            return Math.max(first, second);
        } catch (NumberFormatException nfe){
            System.err.println("Failed to process input; error - " + nfe.getMessage());
            return null;
        } catch(Exception e){
            throw new IOException("Caught exception in MAX.Initial", e);
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
        Util.addToFunctionList(funcList, DoubleMax.class.getName(), DataType.DOUBLE);
        Util.addToFunctionList(funcList, FloatMax.class.getName(), DataType.FLOAT);
        Util.addToFunctionList(funcList, IntMax.class.getName(), DataType.INTEGER);
        Util.addToFunctionList(funcList, LongMax.class.getName(), DataType.LONG);

        return funcList;
    }
}
