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
package org.apache.pig.test.utils;

import java.io.IOException;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.builtin.COUNT;
import org.apache.pig.builtin.LongSum;
import org.apache.pig.data.Tuple;

public class HelperEvalFuncUtils {
    public static class AlgCOUNT extends AlgFunc<Long> {
        public AlgCOUNT() {
            super(new COUNT());
        }
    }

    public static class AccCOUNT extends AccFunc<Long> {
        public AccCOUNT() {
            super(new COUNT());
        }
    }

    public static class BasicCOUNT extends BasicFunc<Long> {
        public BasicCOUNT() {
            super(new COUNT());
        }
    }

    public static class AlgSUM extends AlgFunc<Long> {
        public AlgSUM() {
            super(new LongSum());
        }
    }

    public static class AccSUM extends AccFunc<Long> {
        public AccSUM() {
            super(new LongSum());
        }
    }

    public static class BasicSUM extends BasicFunc<Long> {
        public BasicSUM() {
            super(new LongSum());
        }
    }

    public static class AlgFunc<T> extends AlgebraicEvalFunc<T> {
        Algebraic f;

        public AlgFunc(Algebraic f) {
            this.f=f;
        }

        @Override
        public String getInitial() {
            if (f==null) {
                return LongSum.Initial.class.getName();
            }
            return f.getInitial();
        }

        @Override
        public String getIntermed() {
            if (f==null) {
                return LongSum.Intermediate.class.getName();
            }
            return f.getIntermed();
        }

        @Override
        public String getFinal() {
            if (f==null) {
                return LongSum.Final.class.getName();
            }
            return f.getFinal();
        }
    }

    public static class AccFunc<T> extends AccumulatorEvalFunc<T> {
        Accumulator<T> f;

        public AccFunc(Accumulator<T> f) {
            this.f=f;
        }

        @Override
        public T getValue() {
            return f.getValue();
        }

        @Override
        public void accumulate(Tuple input) throws IOException {
            f.accumulate(input);
        }

        @Override
        public void cleanup() {
            f.cleanup();
        }
    }

    public static class BasicFunc<T> extends EvalFunc<T> {
        EvalFunc<T> f;

        public BasicFunc(EvalFunc<T> f) {
            this.f=f;
        }

        @Override
        public T exec(Tuple input) throws IOException {
            return f.exec(input);
        }
    }

    public static class AccLongCountWithConstructor extends AccFunc<Long> {
        public AccLongCountWithConstructor(String mult) {
            super(new AlgLongCountWithConstructor(mult));
        }
    }

    public static class BasicLongCountWithConstructor extends BasicFunc<Long> {
        public BasicLongCountWithConstructor(String mult) {
            super(new AlgLongCountWithConstructor(mult));
        }
    }

    //this exists to make sure that constructors work properly with AlgebraicEvalFunc
    public static class AlgLongCountWithConstructor extends AlgebraicEvalFunc<Long> {
        //this will inflate the count by the given factor
        public AlgLongCountWithConstructor(String mult) {
            super(mult);
        }

        public static class Initial extends COUNT.Initial {
            long mult=1;
            public Initial() {}

            public Initial(String mult) {
                this.mult = Long.parseLong(mult);
            }

            public Tuple exec(Tuple input) throws IOException {
                Tuple t = super.exec(input);
                t.set(0,((Long)t.get(0))*mult);
                return t;
            }
        }

        public static class Intermed extends COUNT.Intermediate {
            public Intermed() {}
            public Intermed(String mult) {}
        }

        public static class Final extends COUNT.Final {
            public Final() {}
            public Final(String mult) {}
        }

        @Override
        public String getInitial() {
            return Initial.class.getName();
        }

        @Override
        public String getIntermed() {
            return Intermed.class.getName();
        }

        @Override
        public String getFinal() {
            return Final.class.getName();
        }
    }
}
