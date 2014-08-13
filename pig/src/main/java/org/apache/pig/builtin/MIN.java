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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Generates the minimum of a set of values. This class implements
 * {@link org.apache.pig.Algebraic}, so if possible the execution will
 * performed in a distributed fashion.
 * <p>
 * MIN can operate on any numeric type and on chararrays.  It can also operate on bytearrays,
 * which it will cast to doubles.    It expects a bag of
 * tuples of one record each.  If Pig knows from the schema that this function
 * will be passed a bag of integers or longs, it will use a specially adapted version of
 * MIN that uses integer arithmetic for comparing the data.  The return type
 * of MIN will match the input type.
 * <p>
 * MIN implements the {@link org.apache.pig.Accumulator} interface as well.
 * While this will never be
 * the preferred method of usage it is available in case the combiner can not be
 * used for a given calculation.
 */
public class MIN extends  AlgebraicByteArrayMathBase {

    public MIN() {
        setOp(KNOWN_OP.MIN);
    }

    public static class Intermediate extends AlgebraicByteArrayMathBase.Intermediate {
        @Override
        public KNOWN_OP getOp() {
            return KNOWN_OP.MIN;
        }
    }

    public static class Final extends AlgebraicByteArrayMathBase.Final {
        @Override
        public KNOWN_OP getOp() {
            return KNOWN_OP.MIN;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BYTEARRAY)));
        funcList.add(new FuncSpec(DoubleMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DOUBLE)));
        funcList.add(new FuncSpec(FloatMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.FLOAT)));
        funcList.add(new FuncSpec(IntMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER)));
        funcList.add(new FuncSpec(LongMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.LONG)));
        funcList.add(new FuncSpec(StringMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.CHARARRAY)));
        funcList.add(new FuncSpec(DateTimeMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DATETIME)));
        funcList.add(new FuncSpec(BigDecimalMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGDECIMAL)));
        funcList.add(new FuncSpec(BigIntegerMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BIGINTEGER)));
        return funcList;
    }
}
