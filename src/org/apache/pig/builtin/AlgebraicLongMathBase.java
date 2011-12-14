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
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Core logic for applying an accumulative/algebraic math function to a
 * bag of Longs.
 */
public abstract class AlgebraicLongMathBase extends AlgebraicMathBase<Long> implements Accumulator<Long> {

    protected static Long getSeed(KNOWN_OP op) {
        switch (op) {
        case SUM: return 0L;
        case MIN: return Long.MAX_VALUE;
        case MAX: return Long.MIN_VALUE;
        default: return null;
        }
    }

    private static Long doWork(Long arg1, Long arg2, KNOWN_OP op) {
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

    protected static Long doTupleWork(Tuple input, KnownOpProvider opProvider) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0) {
            return null;
        }
        Long sofar = AlgebraicLongMathBase.getSeed(opProvider.getOp());
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try {
                // Note: use Number.longValue() instead of casting to Long
                // to allow Integers, too.
                Number n = ((Number)(t.get(0)));
                if (n == null) continue;
                Long d = n.longValue();
                sawNonNull = true;
                sofar = doWork(sofar, d, opProvider.getOp());
            }catch(RuntimeException exp) {
                int errCode = 2103;
                throw new ExecException("Problem doing work on Longs", errCode, PigException.BUG, exp);
            }
        }
        return sawNonNull ? sofar : null;
    }

    @Override
    public Long exec(Tuple input) throws IOException {
        try {
            return doTupleWork(input, opProvider);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            throw new ExecException("Error executing function on Longs", errCode, PigException.BUG, e);
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
                throw new ExecException("Error executing function on Longs", errCode, PigException.BUG, e);
            }
        }
    }

    static public abstract class Final extends AlgebraicMathBase.Final<Long> {
        @Override
        public Long exec(Tuple input) throws IOException {
            try {
                return doTupleWork(input, this);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                throw new ExecException("Error executing function on Longs", errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.LONG));
    }

    /* Accumulator interface implementation*/
    private Long intermediateVal = null;

    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            Long curVal = doTupleWork(b, opProvider);
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
            throw new ExecException("Error executing function on Longs", errCode, PigException.BUG, e);
        }
    }

    @Override
    public void cleanup() {
        intermediateVal = null;
    }

    @Override
    public Long getValue() {
        return intermediateVal;
    }
}
