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
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Generates the min of the values of the first field of a tuple.
 */
public class MIN extends EvalFunc<Double> implements Algebraic {

    @Override
    public Double exec(Tuple input) throws IOException {
        return min(input);
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
            return tfact.newTuple(min(input));
        }
    }
    static public class Final extends EvalFunc<Double> {
        @Override
        public Double exec(Tuple input) throws IOException {
            return min(input);
        }
    }

    static protected Double min(Tuple input) throws IOException {
        DataBag values = (DataBag)input.get(0);

        double curMin = Double.POSITIVE_INFINITY;
        for (Iterator it = values.iterator(); it.hasNext();) {
            Tuple t = (Tuple) it.next();
            try {
                curMin = java.lang.Math.min(curMin, (Double)t.get(0));
            } catch (RuntimeException exp) {
                IOException newE =  new IOException("Error processing: " +
                    t.toString() + exp.getMessage());
                newE.initCause(exp);
                throw newE;
            }
        }
    
        return new Double(curMin);
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new AtomSchema("min" + count++);
    }

    private static int count = 1;
}
