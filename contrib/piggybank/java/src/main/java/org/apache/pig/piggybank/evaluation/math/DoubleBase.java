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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

/**
* base class for math udfs that return Double value
*/

/**
 * @deprecated Use {@link org.apache.pig.builtin.DoubleBase}
 */
@Deprecated

public abstract class DoubleBase extends Base{
    // each derived class provides the computation here
    abstract Double compute(Double input);

	/**
	 * java level API
	 * @param input expects a tuple with a single Double value
	 * @param output returns a Double value
     */
	public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        try {
            Double val = (Double)input.get(0);
            return (val == null ? null : compute(val));
        } catch (Exception e){
            throw new IOException("Caught exception processing input of " + this.getClass().getName(), e);
        }
	}

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.DOUBLE))));

        return funcList;
    }

}
