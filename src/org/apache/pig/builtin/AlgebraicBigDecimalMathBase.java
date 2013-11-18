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
import java.util.Iterator;
import java.math.BigDecimal;

import org.apache.pig.Accumulator;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Core logic for applying an SUM function to a
 * bag of BigDecimals.
 * This is a modified version of AlgebraicDoubleMathBase
 */
public abstract class AlgebraicBigDecimalMathBase extends AlgebraicMathBase<BigDecimal> implements Accumulator<BigDecimal> {

    protected static BigDecimal getSeed(KNOWN_OP op) {
        switch (op) {
        case SUM: return BigDecimal.ZERO;
        case MAX: return BigDecimalWrapper.NEGATIVE_INFINITY();
        case MIN: return BigDecimalWrapper.POSITIVE_INFINITY();
        default: return null;
        }
    }

    private static BigDecimal doWork(BigDecimal arg1, BigDecimal arg2, KNOWN_OP op) {
        if (arg1 == null) {
            return arg2;
        } else if (arg2 == null) {
            return arg1;
        } else {
            BigDecimal retVal = null;
            switch (op) {
            case SUM:
                retVal = arg1.add(arg2);
                break;
            case MAX:
                if (BigDecimalWrapper.class.isInstance(arg1) && (((BigDecimalWrapper)arg1).isNegativeInfinity())) {
                    retVal = arg2;
                } else if (BigDecimalWrapper.class.isInstance(arg2) && (((BigDecimalWrapper)arg2).isNegativeInfinity())) {
                    retVal = arg1;
                } else {
                    retVal = arg1.max(arg2);
                }
                break;
            case MIN:
                if (BigDecimalWrapper.class.isInstance(arg1) && (((BigDecimalWrapper)arg1).isPositiveInfinity())) {
                    retVal = arg2;
                } else if (BigDecimalWrapper.class.isInstance(arg2) && (((BigDecimalWrapper)arg2).isPositiveInfinity())) {
                    retVal = arg1;
                } else {
                    retVal = arg1.min(arg2);
                }
                break;
            default:
                retVal = null;
                break;
            }
            return retVal;
        }
    }

    protected static BigDecimal doTupleWork(Tuple input, KnownOpProvider opProvider) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0) {
            return null;
        }
        BigDecimal sofar = AlgebraicBigDecimalMathBase.getSeed(opProvider.getOp());
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try {
                Number n = (Number)(t.get(0));
                if (n == null) continue;
                BigDecimal d = (BigDecimal) n;
                sawNonNull = true;
                sofar = doWork(sofar, d, opProvider.getOp());
            }catch(RuntimeException exp) {
                int errCode = 2103;
                throw new ExecException("Problem doing work on BigDecimal", errCode, PigException.BUG, exp);
            }
        }
        return sawNonNull ? sofar : null;
    }

    @Override
    public BigDecimal exec(Tuple input) throws IOException {
        try {
            return doTupleWork(input, opProvider);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            throw new ExecException("Error executing function on BigDecimal", errCode, PigException.BUG, e);
        }
    }

    static public abstract class Intermediate extends AlgebraicMathBase.Intermediate {
        private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return tfact.newTuple(doTupleWork(input, this));
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                throw new ExecException("Error executing function on BigDecimal", errCode, PigException.BUG, e);
            }
        }
    }

    static public abstract class Final extends AlgebraicMathBase.Final<BigDecimal> {
        @Override
        public BigDecimal exec(Tuple input) throws IOException {
            try {
                return doTupleWork(input, this);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                throw new ExecException("Error executing function on BigDecimal", errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BIGDECIMAL));
    }

    /* Accumulator interface implementation*/
    private BigDecimal intermediateVal = null;

    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            BigDecimal curVal = doTupleWork(b, opProvider);
            if (curVal == null) {
                return;
            }
            if (intermediateVal == null) {
                intermediateVal = getSeed(opProvider.getOp());
            }
            intermediateVal = doWork(intermediateVal, curVal, opProvider.getOp());
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            throw new ExecException("Error executing function on BigDecimal", errCode, PigException.BUG, e);
        }
    }

    @Override
    public void cleanup() {
        intermediateVal = null;
    }

    @Override
    public BigDecimal getValue() {
        return intermediateVal;
    }
}
