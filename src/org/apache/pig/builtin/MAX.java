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
import org.apache.pig.impl.util.WrappedIOException;


/**
 * Generates the max of the values of the first field of a tuple.
 */
public class MAX extends EvalFunc<DataAtom> implements Algebraic {

    @Override
    public void exec(Tuple input, DataAtom output) throws IOException {
        output.setValue(max(input));
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
            output.appendField(new DataAtom(max(input)));
        }
    }
    static public class Final extends EvalFunc<DataAtom> {
        @Override
        public void exec(Tuple input, DataAtom output) throws IOException {
            output.setValue(max(input));
        }
    }

    static protected double max(Tuple input) throws IOException {
        DataBag values = input.getBagField(0);

        double curMax = Double.NEGATIVE_INFINITY;
        for (Iterator it = values.iterator(); it.hasNext();) {
            Tuple t = (Tuple) it.next();
            try {
                curMax = java.lang.Math.max(curMax, t.getAtomField(0).numval());
            }catch(RuntimeException exp) {
                throw WrappedIOException.wrap("Error processing: " + t.toString() + exp.getMessage(), exp);

            }
        }

        return curMax;
    }
    @Override
    public Schema outputSchema(Schema input) {
        return new AtomSchema("max" + count++);
    }

    private static int count = 1;
}
