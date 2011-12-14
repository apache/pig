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

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * The purpose of this class is to hold some of the common code shared among
 * the typed Basic*Funcs (BasicDoubleFunc, BasicIntegerFunc, etc).
 */
abstract class AlgebraicMathBase<T> extends EvalFunc<T> implements Algebraic {

    protected static enum KNOWN_OP {
        SUM, MIN, MAX;
    }

    protected interface KnownOpProvider {
        public KNOWN_OP getOp();
    }

    protected KnownOpProvider opProvider;

    protected void setOp(final KNOWN_OP op) {
        opProvider = new KnownOpProvider() {
            @Override
            public KNOWN_OP getOp() {
                return op;
            }};
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
                    return bg.iterator().next();
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

    @Override
    public String getIntermed() {
        for (@SuppressWarnings("rawtypes")
        Class c : this.getClass().getDeclaredClasses()) {
            if (Intermediate.class.isAssignableFrom(c)) {
                return c.getName();
            }
        }
        // Try inheritance
        for (@SuppressWarnings("rawtypes")
        Class c : this.getClass().getClasses()) {
            if (Intermediate.class.isAssignableFrom(c)) {
                return c.getName();
            }
        }
        return null;
    }

    @Override
    public String getFinal() {
        for (@SuppressWarnings("rawtypes")
        Class c : this.getClass().getDeclaredClasses()) {
            if (Final.class.isAssignableFrom(c)) {
                return c.getName();
            }
        }
        for (@SuppressWarnings("rawtypes")
        Class c : this.getClass().getClasses()) {
            if (Final.class.isAssignableFrom(c)) {
                return c.getName();
            }
        }
        return null;
    }

    static public abstract class Intermediate extends EvalFunc<Tuple> implements KnownOpProvider {
    }
    static public abstract class Final<T> extends EvalFunc<T> implements KnownOpProvider {
    }

}
