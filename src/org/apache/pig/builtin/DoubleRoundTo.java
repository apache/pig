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

package org.apache.pig.builtin;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import java.math.BigDecimal;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * ROUND_TO safely rounds a number to a given precision by using an intermediate
 * BigDecimal. The too-often seen trick of doing (1000.0 * ROUND(x/1000)) is not
 * only hard to read but also fails to produce numerically accurate results.
 *
 * Given a single data atom and precision it Returns a double extending to the
 * given number of decimal places. ROUND_TO(0.9876543, 3) is 0.988;
 * ROUND_TO(0.9876543, 0) is 1.0.
 *
 */
public class DoubleRoundTo extends EvalFunc<Double>{
    /**
     * java level API
     * @param input expects a numeric value to round and a number of digits to keep
     * @return output returns a single numeric value, the number with only those digits retained
     */
    @Override
    public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2)
            return null;

        try {
            Double     num    = (Double)input.get(0);
            Integer    digits = (Integer)input.get(1);
            BigDecimal bdnum  = BigDecimal.valueOf(num);

            bdnum = bdnum.setScale(digits, BigDecimal.ROUND_HALF_UP);
            return bdnum.doubleValue();
        } catch (Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();

        Schema s_dbl = new Schema();
        s_dbl.add(new Schema.FieldSchema(null, DataType.DOUBLE));
        s_dbl.add(new Schema.FieldSchema(null, DataType.INTEGER));

        funcList.add(new FuncSpec(this.getClass().getName(), s_dbl));

        return funcList;
    }
}
