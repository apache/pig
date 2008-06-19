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

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.executionengine.ExecException;


/**
 * Generates the average of the values of the first field of a tuple. This class is Algebraic in
 * implemenation, so if possible the execution will be split into a local and global application
 */
public class AVG extends EvalFunc<Double> implements Algebraic {
    
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Double exec(Tuple input) throws IOException {
        try {
            double sum = sum(input);
            double count = count(input);

            double avg = 0;
            if (count > 0)
                avg = sum / count;
    
            return new Double(avg);
        } catch (ExecException ee) {
            IOException oughtToBeEE = new IOException();
            oughtToBeEE.initCause(ee);
            throw oughtToBeEE;
        }
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermed.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                Tuple t = mTupleFactory.newTuple(2);
                t.set(0, new Double(sum(input)));
                t.set(1, new Long(count(input)));
                return t;
            } catch(RuntimeException t) {
                throw new RuntimeException(t.getMessage() + ": " + input);
            } catch (ExecException ee) {
                IOException oughtToBeEE = new IOException();
                oughtToBeEE.initCause(ee);
                throw oughtToBeEE;
            }
                
        }
    }

    static public class Intermed extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                DataBag b = (DataBag)input.get(0);
                return combine(b);
            } catch (ExecException ee) {
                IOException oughtToBeEE = new IOException();
                oughtToBeEE.initCause(ee);
                throw oughtToBeEE;
            }
        }
    }

    static public class Final extends EvalFunc<Double> {
        @Override
        public Double exec(Tuple input) throws IOException {
            try {
                DataBag b = (DataBag)input.get(0);
                Tuple combined = combine(b);

                double sum = (Double)combined.get(0);
                double count = (Long)combined.get(1);

                double avg = 0;
                if (count > 0) {
                    avg = sum / count;
                }
                return new Double(avg);
            } catch (ExecException ee) {
                IOException oughtToBeEE = new IOException();
                oughtToBeEE.initCause(ee);
                throw oughtToBeEE;
            }
        }
    }

    static protected Tuple combine(DataBag values) throws ExecException {
        double sum = 0;
        long count = 0;

        Tuple output = mTupleFactory.newTuple(2);

        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            sum += (Double)t.get(0);
            count += (Long)t.get(1);
        }

        output.set(0, new Double(sum));
        output.set(1, new Long(count));
        return output;
    }

    static protected long count(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        return values.size();
    }

    static protected double sum(Tuple input) throws ExecException, IOException {
        DataBag values = (DataBag)input.get(0);

        double sum = 0;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            Double d = DataType.toDouble(t.get(0));
            if (d == null) continue;
            sum += d;
        }

        return sum;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE)); 
    }

}
