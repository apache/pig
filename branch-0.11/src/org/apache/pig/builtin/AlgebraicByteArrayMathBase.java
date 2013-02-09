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

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Core logic for applying an accumulative/algebraic math function to a
 * bag of doubles.
 */
public abstract class AlgebraicByteArrayMathBase extends AlgebraicMathBase<Double> implements Accumulator<Double> {

    protected static Double getSeed(KNOWN_OP op) {
        switch (op) {
        case SUM: return 0.0;
        case MIN: return Double.POSITIVE_INFINITY;
        case MAX: return Double.NEGATIVE_INFINITY;
        default: return null;
        }
    }

    private static Double doWork(Double arg1, Double arg2, KNOWN_OP op) {
        if (arg1 == null) {
            return arg2;
        } else if (arg2 == null) {
            return arg1;
        } else {
            switch (op) {
            case MAX: return Math.max(arg1, arg2);
            case MIN: return Math.min(arg1, arg2);
            case SUM: return arg1 + arg2;
            default: return null;
            }
        }
    }

    protected static Double doTupleWork(Tuple input, KnownOpProvider opProvider, byte expectedType)
            throws ExecException {
        DataBag values = (DataBag)input.get(0);
        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0) {
            return null;
        }
        double sofar = AlgebraicByteArrayMathBase.getSeed(opProvider.getOp());
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try {
                Double d;
                switch (expectedType) {
                case DataType.BYTEARRAY:
                    DataByteArray dba = (DataByteArray)t.get(0);
                    d = dba != null ? Double.valueOf(dba.toString()): null;
                    break;
                case DataType.DOUBLE:
                    d = (Double) t.get(0);
                    break;
                default:
                    throw new ExecException("Unexpected type in AlgebraicByteArrayMath "
                            + DataType.findTypeName(expectedType));
                }
                if (d == null) continue;
                sawNonNull = true;
                sofar = doWork(sofar, d, opProvider.getOp());
            }catch(RuntimeException exp) {
                int errCode = 2103;
                throw new ExecException("Problem doing work on Doubles", errCode, PigException.BUG, exp);
            }
        }
        return sawNonNull ? sofar : null;
    }

    @Override
    public Double exec(Tuple input) throws IOException {
        try {
            return doTupleWork(input, opProvider, DataType.BYTEARRAY);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            throw new ExecException("Error executing function on Doubles", errCode, PigException.BUG, e);
        }
    }

    static public abstract class Intermediate extends AlgebraicMathBase.Intermediate {
        private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return tfact.newTuple(doTupleWork(input, this, DataType.DOUBLE));
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                throw new ExecException("Error executing function on Doubles", errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public String getInitial() {
        return Initial.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            // Initial is called in the map.
            // we just send the tuple down
            try {
                // input is a bag with one tuple containing
                // the column we are trying to operate on
                DataBag bg = (DataBag) input.get(0);
                if (bg.iterator().hasNext()) {
                    DataByteArray dba = (DataByteArray) bg.iterator().next().get(0);
                    Double d = dba != null ? Double.valueOf(dba.toString()): null;
                    return tfact.newTuple(d);
                } else {
                    // make sure that we call the object constructor, not the list constructor
                    return tfact.newTuple((Object) null);
                }
            } catch (ExecException e) {
                throw e;
            } catch (Exception e) {
                int errCode = 2106;
                throw new ExecException("Error executing an algebraic function", errCode, PigException.BUG, e);
            }
        }
    }

    static public abstract class Final extends AlgebraicMathBase.Final<Double> {
        @Override
        public Double exec(Tuple input) throws IOException {
            try {
                return doTupleWork(input, this, DataType.DOUBLE);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                throw new ExecException("Error executing function on Doubles", errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }

    /* Accumulator interface implementation*/
    private Double intermediateVal = null;

    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            Double curVal = doTupleWork(b, opProvider, DataType.BYTEARRAY);
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
            throw new ExecException("Error executing function on Doubles", errCode, PigException.BUG, e);
        }
    }

    @Override
    public void cleanup() {
        intermediateVal = null;
    }

    @Override
    public Double getValue() {
        return intermediateVal;
    }
}
