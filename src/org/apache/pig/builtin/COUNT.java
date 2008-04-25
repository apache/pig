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

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Generates the count of the values of the first field of a tuple. This class is Algebraic in
 * implemenation, so if possible the execution will be split into a local and global functions
 */
public class COUNT extends EvalFunc<Long> implements Algebraic{

    @Override
    public Long exec(Tuple input) throws IOException {
        try {
            return count(input);
        } catch (ExecException ee) {
            IOException oughtToBeEE = new IOException();
            ee.initCause(ee);
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
        TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return tfact.newTuple(count(input));
            } catch (ExecException ee) {
                IOException oughtToBeEE = new IOException();
                ee.initCause(ee);
                throw oughtToBeEE;
            }
        }
    }

    static public class Intermed extends EvalFunc<Tuple> {
        TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return tfact.newTuple(count(input));
            } catch (ExecException ee) {
                IOException oughtToBeEE = new IOException();
                ee.initCause(ee);
                throw oughtToBeEE;
            }
        }
    }

    static public class Final extends EvalFunc<Long> {
        @Override
        public Long exec(Tuple input) throws IOException {
            try {
                return sum(input);
            } catch (Exception ee) {
                IOException oughtToBeEE = new IOException();
                ee.initCause(ee);
                throw oughtToBeEE;
            }
        }
    }

    static protected Long count(Tuple input) throws ExecException {
        Object values = input.get(0);        
        if (values instanceof DataBag)
            return ((DataBag)values).size();
        else if (values instanceof Map)
            return new Long(((Map)values).size());
        else
            throw new ExecException("Cannot count a " +
                DataType.findTypeName(values));
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
        // TODO FIX 
        // return new AtomSchema("count" + count++);
        return null;
    }

    private static int count = 1;

}
