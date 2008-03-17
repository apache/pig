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
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Generates the sum of the values of the first field of a tuple.
 */
public class SUM extends EvalFunc<DataAtom> implements Algebraic {

    @Override
    public void exec(Tuple input, DataAtom output) throws IOException {
        output.setValue(sum(input));
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
        @Override
        public void exec(Tuple input, Tuple output) throws IOException {
            output.appendField(new DataAtom(sum(input)));
        }
    }
    static public class Final extends EvalFunc<DataAtom> {
        @Override
        public void exec(Tuple input, DataAtom output) throws IOException {
            output.setValue(sum(input));
        }
    }

    static protected double sum(Tuple input) throws IOException {
        DataBag values = input.getBagField(0);

        double sum = 0;
    int i = 0;
        Tuple t = null;
        for (Iterator it = values.iterator(); it.hasNext();) {
            try {
                t = (Tuple) it.next();
                i++;
                sum += t.getAtomField(0).numval();
            }catch(RuntimeException exp) {
                StringBuilder msg = new StringBuilder();
                msg.append("iteration = ");
                msg.append(i);
                msg.append("bag size = ");
                msg.append(values.size());
                msg.append(" partial sum = ");
                msg.append(sum);
                msg.append("\n");
                if (t != null) {
                    msg.append("previous tupple = ");
                    msg.append(t.toString());
                }
                StringBuilder errorMsg = new StringBuilder();
                errorMsg.append(exp.getMessage());
                errorMsg.append(" additional info: ");
                errorMsg.append(msg.toString());
                throw new RuntimeException(errorMsg.toString());
                //throw new RuntimeException(exp.getMessage() + " error processing: " + t.toString());
            }
        }

        return sum;
    }
    @Override
    public Schema outputSchema(Schema input) {
        return new AtomSchema("sum" + count++);
    }

    private static int count = 1;
}
