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
import org.apache.pig.data.DataMap;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

/**
 * Generates the count of the values of the first field of a tuple. This class is Algebraic in
 * implemenation, so if possible the execution will be split into a local and global functions
 */
public class COUNT extends EvalFunc<DataAtom> implements Algebraic{

    @Override
    public void exec(Tuple input, DataAtom output) throws IOException {
        output.setValue(count(input));
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
        public void exec(Tuple input, Tuple output) throws IOException {
            output.appendField(new DataAtom(count(input)));
        }
    }

    static public class Intermed extends EvalFunc<Tuple> {
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

    static protected long count(Tuple input) throws IOException {
        Datum values = input.getField(0);        
        if (values instanceof DataBag)
            return ((DataBag)values).size();
        else if (values instanceof DataMap)
            return ((DataMap)values).cardinality();
        else
            throw new IOException("Cannot count a " + values.getClass().getSimpleName());
    }

    static protected long sum(Tuple input) throws IOException {
        DataBag values = input.getBagField(0);
        long sum = 0;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try {
                sum += t.getAtomField(0).longVal();
            } catch (NumberFormatException exp) {
                throw WrappedIOException.wrap(exp.getClass().getName() + ":" + exp.getMessage(), exp);
            }
        }
        return sum;
    }

@Override
    public Schema outputSchema(Schema input) {
        return new AtomSchema("count" + count++);
    }

    private static int count = 1;

}
