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

package org.apache.pig.scripting.jruby;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.scripting.jruby.JrubyScriptEngine.RubyFunctions;

import org.jruby.Ruby;
import org.jruby.embed.ScriptingContainer;
import org.jruby.runtime.builtin.IRubyObject;

/**
 * This class provides the bridge between Ruby classes that extend the AlgebraicPigUdf
 * "interface" by implementing an initial, intermed, and final method. Unlike EvalFuncs
 * and Accumulators, the type must be known at compile time (ie it can't return Object),
 * as Pig inspects the type and ensures that it is valid. This is why class specific
 * shells are provided at the bottom. This class leverages AlgebraicEvalFunc to provide
 * the Accumulator and EvalFunc implementations.
 */
public abstract class JrubyAlgebraicEvalFunc<T> extends AlgebraicEvalFunc<T> {
    protected static BagFactory mBagFactory = BagFactory.getInstance();
    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    protected static final ScriptingContainer rubyEngine = JrubyScriptEngine.rubyEngine;
    protected static final Ruby ruby = rubyEngine.getProvider().getRuntime();

    // It makes no sense to instantiate this without arguments
    private JrubyAlgebraicEvalFunc() {}

    /**
     * The constructor takes information on the script and method being invoked and registers it with the
     * superclass (which is necessary for AlgebraicEvalFunc).
     */
    public JrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName, functionName); }

    /**
     * This class invokes the initial method on the given Ruby class. As a courtesy to the user, it unwraps the
     * DataBag that is given to Initial and passes Ruby just the first Tuple that it contains, as the contract
     * for the Initial function in Algebraic is that it is given a DataBag with one and only one Tuple. Finally,
     * it wraps the Ruby output in a Tuple.
     */
    public static class Initial extends AlgebraicFunctionWrapper<Tuple> {
        public Initial() {}

        public Initial(String fileName, String functionName) { super(fileName, functionName, "initial"); }

        @Override
        public Tuple exec(Tuple input) throws IOException {
            if (!isInitialized())
                initialize();

            try {
                IRubyObject inp = PigJrubyLibrary.pigToRuby(ruby, ((DataBag)input.get(0)).iterator().next().get(0));
                IRubyObject rubyResult = rubyEngine.callMethod(getReceiver(), getStage(), inp, IRubyObject.class);
                return mTupleFactory.newTuple(PigJrubyLibrary.rubyToPig(rubyResult));
            } catch (Exception e) {
                throw new IOException("Error executing initial function",  e);
            }
        }
    }

    /**
     * This class invokes the intermed method on the given Ruby class. It passes along the DataBag contained
     * in the Tuple it is given, and wraps the Ruby output in a Tuple.
     */
    public static class Intermed extends AlgebraicFunctionWrapper<Tuple> {
        public Intermed() {}

        public Intermed(String fileName, String functionName) { super(fileName, functionName, "intermed"); }

        @Override
        public Tuple exec(Tuple input) throws IOException {
            if (!isInitialized())
                initialize();

            try {
                RubyDataBag inp = new RubyDataBag(ruby, ruby.getClass("DataBag"), (DataBag)input.get(0));
                IRubyObject rubyResult = rubyEngine.callMethod(getReceiver(), getStage(), inp, IRubyObject.class);
                return mTupleFactory.newTuple(PigJrubyLibrary.rubyToPig(rubyResult));
            } catch (Exception e) {
                throw new IOException("Error executing intermediate function: ",  e);
            }
        }
    }

    /**
     * This class invokes the final method on the given Ruby class. It passes along the DataBag contained
     * in the Tuple it is given, and the raw result.
     */
    public static class Final<T> extends AlgebraicFunctionWrapper<T> {
        public Final() {}

        public Final(String fileName, String functionName) { super(fileName, functionName, "final"); }

        @SuppressWarnings("unchecked")
        @Override
        public T exec(Tuple input) throws IOException {
            if (!isInitialized())
                initialize();

            try {
                RubyDataBag inp = new RubyDataBag(ruby, ruby.getClass("DataBag"), (DataBag)input.get(0));
                IRubyObject rubyResult = rubyEngine.callMethod(getReceiver(), getStage(), inp, IRubyObject.class);
                return (T)PigJrubyLibrary.rubyToPig(rubyResult);
            } catch (Exception e) {
                throw new IOException("Error executing final function",  e);
            }
        }
    }

    /**
     * This is a lightweight wrapper shell that registers information on the method being called,
     * and provides the initializer that the static Algebraic classes (Initial, Intermed, Final)
     * will use to execute.
     */
    public static abstract class AlgebraicFunctionWrapper<T> extends EvalFunc<T> {
        private String fileName;
        private String functionName;

        protected Object receiver;
        protected boolean isInitialized = false;

        protected String stage;

        public String getStage() { return stage; }
        public Object getReceiver() { return receiver; }
        public String getFileName() { return fileName; }
        public String getFunctionName() { return functionName; }

        public AlgebraicFunctionWrapper() {}

        /**
         * In addition to registering the fileName and the functionName (which are given based on the
         * arguments passed to super() in the containing class's constructor, each extending class
         * will register their "stage," which will serve as the method to invoke on the Ruby class.
         */
        public AlgebraicFunctionWrapper(String fileName, String functionName, String stage) {
            this.fileName = fileName;
            this.functionName = functionName;
            this.stage = stage;
        }

        public boolean isInitialized() { return isInitialized; }

        public void initialize() {
            receiver = rubyEngine.callMethod(RubyFunctions.getFunctions("algebraic", fileName).get(functionName), "new");
            isInitialized = true;
        }

        @Override
        public abstract T exec(Tuple input) throws IOException;
    }

    @Override
    public abstract String getFinal();

    @Override
    public String getInitial() { return Initial.class.getName(); }

    @Override
    public String getIntermed() { return Intermed.class.getName(); }

    /**
     * Unlike EvalFuncs and Accumulators, the type must be known at compile time (ie it
     * can't return Object), as Pig inspects the type and ensures that it is valid. This
     * is why class specific shells are provided here. This is also the reason why the
     * Ruby Algebraic interface is the only interface that does not currently allow overriding
     * outputSchema, and a fixed one must be provided.
     */
    public static class BagJrubyAlgebraicEvalFunc extends JrubyAlgebraicEvalFunc<DataBag> {
        public BagJrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName,functionName); }

        @Override
        public String getFinal() { return Final.class.getName(); }

        public static class Final extends JrubyAlgebraicEvalFunc.Final<DataBag> {
            public Final() {}
            public Final(String fileName, String functionName) { super(fileName,functionName); }
        }
    }

    public static class ChararrayJrubyAlgebraicEvalFunc extends JrubyAlgebraicEvalFunc<String> {
        public ChararrayJrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName,functionName); }

        @Override
        public String getFinal() { return Final.class.getName(); }

        public static class Final extends JrubyAlgebraicEvalFunc.Final<String> {
            public Final() {}
            public Final(String fileName, String functionName) { super(fileName,functionName); }
        }
    }

    public static class DataByteArrayJrubyAlgebraicEvalFunc extends JrubyAlgebraicEvalFunc<DataByteArray> {
        public DataByteArrayJrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName,functionName); }

        @Override
        public String getFinal() { return Final.class.getName(); }

        public static class Final extends JrubyAlgebraicEvalFunc.Final<DataByteArray> {
            public Final() {}
            public Final(String fileName, String functionName) { super(fileName,functionName); }
        }
    }

    public static class DoubleJrubyAlgebraicEvalFunc extends JrubyAlgebraicEvalFunc<Double> {
        public DoubleJrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName,functionName); }

        @Override
        public String getFinal() { return Final.class.getName(); }

        public static class Final extends JrubyAlgebraicEvalFunc.Final<Double> {
            public Final() {}
            public Final(String fileName, String functionName) { super(fileName,functionName); }
        }
    }

    public static class FloatJrubyAlgebraicEvalFunc extends JrubyAlgebraicEvalFunc<Float> {
        public FloatJrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName,functionName); }

        @Override
        public String getFinal() { return Final.class.getName(); }

        public static class Final extends JrubyAlgebraicEvalFunc.Final<Float> {
            public Final() {}
            public Final(String fileName, String functionName) { super(fileName,functionName); }
        }
    }

    public static class IntegerJrubyAlgebraicEvalFunc extends JrubyAlgebraicEvalFunc<Integer> {
        public IntegerJrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName,functionName); }

        @Override
        public String getFinal() { return Final.class.getName(); }

        public static class Final extends JrubyAlgebraicEvalFunc.Final<Integer> {
            public Final() {}
            public Final(String fileName, String functionName) { super(fileName,functionName); }
        }
    }

    public static class LongJrubyAlgebraicEvalFunc extends JrubyAlgebraicEvalFunc<Long> {

        public LongJrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName,functionName); }

        @Override
        public String getFinal() { return Final.class.getName(); }

        public static class Final extends JrubyAlgebraicEvalFunc.Final<Long> {
            public Final() {}
            public Final(String fileName, String functionName) { super(fileName,functionName); }
        }
    }

    public static class MapJrubyAlgebraicEvalFunc extends JrubyAlgebraicEvalFunc<Map<?,?>> {
        public MapJrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName,functionName); }

        @Override
        public String getFinal() { return Final.class.getName(); }

        public static class Final extends JrubyAlgebraicEvalFunc.Final<Map<?,?>> {
            public Final() {}
            public Final(String fileName, String functionName) { super(fileName,functionName); }
        }
    }

    public static class TupleJrubyAlgebraicEvalFunc extends JrubyAlgebraicEvalFunc<Tuple> {
        public TupleJrubyAlgebraicEvalFunc(String fileName, String functionName) { super(fileName,functionName); }

        @Override
        public String getFinal() { return Final.class.getName(); }

        public static class Final extends JrubyAlgebraicEvalFunc.Final<Tuple> {
            public Final() {}
            public Final(String fileName, String functionName) { super(fileName,functionName); }
        }
    }
}
