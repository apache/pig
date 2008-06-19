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
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Generates the sum of the values of the first field of a tuple.
 */
public class SUM extends EvalFunc<Double> implements Algebraic {

    @Override
    public Double exec(Tuple input) throws IOException {
        try {
            return sum(input);
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
        return Initial.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return tfact.newTuple(sum(input));
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
                return sum(input);
            } catch (ExecException ee) {
                IOException oughtToBeEE = new IOException();
                oughtToBeEE.initCause(ee);
                throw oughtToBeEE;
            }
        }
    }

    static protected double sum(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);

        double sum = 0;
        int i = 0;
        Tuple t = null;
        for (Iterator it = values.iterator(); it.hasNext();) {
            try {
                t = (Tuple) it.next();
                i++;
                Double d = DataType.toDouble(t.get(0));
                if (d == null) continue;
                sum += d;
            }catch(RuntimeException exp) {
                String msg = "iteration = " + i + "bag size = " +
                    values.size() + " partial sum = " + sum + "\n";
                if (t != null)
                        msg += "previous tupple = " + t.toString();
                throw new RuntimeException(exp.getMessage() + " additional info: " + msg);
            }
        }

        return sum;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE)); 
    }

    private static int count = 1;
}
