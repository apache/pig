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
import java.util.Map;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Generates the count of the values of the first field of a tuple. 
 * This class is different from COUNT in that it counts all NULL values and as such
 * implements SQL COUNT(*) semantics.
 *
 * Generates the count of the number of values in a bag.  This count does 
 * include null values, and thus matches SQL semantics for COUNT(*) (where
 * in SQL indicates all) but not for COUNT(a) (where a is * field).
 * <p>
 * This class
 * implements {@link org.apache.pig.Algebraic}, so if possible the execution will
 * performed in a distributed fashion.
 * <p>
 * There are no restrictions as to the data types inside the bag to be counted.
 * <p>
 * COUNT_STAR implements the {@link org.apache.pig.Accumulator} interface as well.
 * While this will never be
 * the preferred method of usage it is available in case the combiner can not be
 * used for a given calculation.
 */

public class COUNT_STAR extends EvalFunc<Long> implements Algebraic, Accumulator<Long>{
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Long exec(Tuple input) throws IOException {
        try {
            DataBag bag = (DataBag)input.get(0);
            return bag.size();
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;                
            String msg = "Error while computing count in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermediate.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {

        @Override
        public Tuple exec(Tuple input) throws IOException {
            // Since Initial is guaranteed to be called
            // only in the map, it will be called with an
            // input of a bag with a single tuple - the 
            // count should always be 1 if bag is non empty
            DataBag bag = (DataBag)input.get(0);
            return mTupleFactory.newTuple(bag.iterator().hasNext()? 
                    Long.valueOf(1L) : Long.valueOf(0L));
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return mTupleFactory.newTuple(sum(input));
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;                
                String msg = "Error while computing count in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    static public class Final extends EvalFunc<Long> {
        @Override
        public Long exec(Tuple input) throws IOException {
            try {
                return sum(input);
            } catch (Exception ee) {
                int errCode = 2106;
                String msg = "Error while computing count in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, ee);
            }
        }
    }

    static protected Long sum(Tuple input) throws ExecException, NumberFormatException {
        DataBag values = (DataBag)input.get(0);
        long sum = 0;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            // Have faith here.  Checking each value before the cast is
            // just too much.
            sum += (Long)t.get(0);
        }
        return sum;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.LONG)); 
    }

    /* Accumulator interface imlpemenatation */
    
    private long intermediateCount = 0L;
    
    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            DataBag values = (DataBag)b.get(0);
            for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
                it.next();
                intermediateCount++;
            }
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing min in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);           
        }
	  }

    @Override
    public void cleanup() {
        intermediateCount = 0L;
    }

    @Override
    public Long getValue() {
        return intermediateCount;
    }

}
