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
package org.apache.pig;

import java.io.IOException;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.counters.PigCounterHelper;

/**
 * This class is used to provide a free implementation of the Accumulator interface
 * and EvalFunc class in the case of an Algebraic function. Instead of having to provide
 * redundant implementations for Accumulator and EvalFunc, implementing the
 * getInitial, getIntermed, and getFinal methods (which implies implementing the static classes
 * they reference) will give you an implementation of each of those for free. <br><br>
 * One key thing to note is that if a subclass of AlgebraicEvalFunc wishes to use any constructor
 * arguments, it MUST call super(args).
 * <br><br>
 * IMPORTANT: the implementation of the Accumulator interface that this class provides is good,
 * but it is simulated. For maximum efficiency, it is important to manually implement the accumulator
 * interface. See {@link Accumulator} for more information on how to do so.
 */
public abstract class AlgebraicEvalFunc<T> extends AccumulatorEvalFunc<T> implements Algebraic {
    private EvalFunc<Tuple> initEvalFunc;
    private EvalFunc<Tuple> intermedEvalFunc;
    private EvalFunc<T> finalEvalFunc;

    private static final BagFactory mBagFactory = BagFactory.getInstance();
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    private DataBag intermediateDB;
    private DataBag wrapDB;
    private DataBag accumDB;

    private Tuple argTuple;
    private Tuple accumTup;

    /**
     * This represents the  number of elements an intermediate bag must have
     * in order for the intermediate EvalFunc to be used on it.
     */
    private int bagCombineThreshold = 1000;

    /**
     * This represents the factor by which the result of applying the intermediate
     * EvalFunc must shrink the data in order for combining at this stage to be
     * deemed "worth it."
     */
    private int combineFactor = 2;

    private PigCounterHelper pigCounterHelper = new PigCounterHelper();

    private boolean combine;

    private String[] constructorArgs;

    /**
     * It is key that if a subclass has a constructor, that it calls super(args...) or else
     * this class will not instantiate properly.
     */
    public AlgebraicEvalFunc(String... constructorArgs) { this.constructorArgs = constructorArgs; }

    /**
     * This must be implement as per a normal Algebraic interface. See {@link Algebraic} for
     * more information.
     */
    @Override
    public abstract String getFinal();

    /**
     * This must be implement as per a normal Algebraic interface. See {@link Algebraic} for
     * more information.
     */
    @Override
    public abstract String getInitial();

    /**
     * This must be implement as per a normal Algebraic interface. See {@link Algebraic} for
     * more information.
     */
    @Override
    public abstract String getIntermed();

    private boolean init = false;

    /**
     * This helper function instantiates an EvalFunc given its String class name.
     */
    private EvalFunc<?> makeEvalFunc(String base) {
        StringBuffer sb = new StringBuffer();
        sb.append(base).append("(");

        boolean first = true;
        for (String s : constructorArgs) {
            if (!first) sb.append(",");
            else first = false;
            sb.append("'").append(s).append("'");
        }

        sb.append(")");

        return (EvalFunc<?>)PigContext.instantiateFuncFromSpec(sb.toString());
    }

    /**
     * This is the free accumulate implementation based on the static classes provided
     * by the Algebraic static classes. This implemention works by leveraging the
     * initial, intermediate, and final classes provided by the algebraic interface.
     * The exec function of the Initial EvalFunc will be called on every Tuple of the input
     * and the output will be collected in an intermediate state. Periodically, this intermediate
     * state will have the Intermediate EvalFunc called on it 1 or more times. The Final EvalFunc
     * is not called until getValue() is called.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void accumulate(Tuple input) throws IOException {
        if (!init) {
            intermediateDB = mBagFactory.newDefaultBag();
            wrapDB = mBagFactory.newDefaultBag();
            accumDB = mBagFactory.newDefaultBag();

            argTuple = mTupleFactory.newTuple(1);
            argTuple.set(0, wrapDB);

            accumTup = mTupleFactory.newTuple(1);
            accumTup.set(0,accumDB);

            initEvalFunc = (EvalFunc<Tuple>)makeEvalFunc(getInitial());
            intermedEvalFunc = (EvalFunc<Tuple>)makeEvalFunc(getIntermed());
            finalEvalFunc = (EvalFunc<T>)makeEvalFunc(getFinal());

            combine = true;
            init = true;
        }

        accumDB.clear();

        for (Tuple t : (DataBag)input.get(0)) {
            wrapDB.clear();
            wrapDB.add(t);
            accumDB.add(initEvalFunc.exec(argTuple));
        }

        intermediateDB.add(intermedEvalFunc.exec(accumTup));
        if (combine && intermediateDB.size() > bagCombineThreshold) {
           long initialSizeEstimate = intermediateDB.getMemorySize();
           DataBag newIntermediateDB = mBagFactory.newDefaultBag();
           Tuple t = mTupleFactory.newTuple(1);
           t.set(0, intermediateDB);
           newIntermediateDB.add(intermedEvalFunc.exec(t));
           intermediateDB = newIntermediateDB;
           long newSizeEstimate = intermediateDB.getMemorySize();
           pigCounterHelper.incrCounter("AlgebraicEvalFunc", "InitialSizeEst", initialSizeEstimate);
           pigCounterHelper.incrCounter("AlgebraicEvalFunc", "PostCombineSizeEst", newSizeEstimate);
           pigCounterHelper.incrCounter("AlgebraicEvalFunc", "CombineApply", 1L);
           if (combineFactor * newSizeEstimate > initialSizeEstimate) {
             combine = false;
             pigCounterHelper.incrCounter("AlgebraicEvalFunc", "CombineShutoff", 1L);
           }
        }
    }

    /**
     * Per the Accumulator interface, this clears all of the variables used in the implementation.
     */
    @Override
    public void cleanup() {
        intermediateDB = null;
        wrapDB = null;
        accumDB = null;

        argTuple = null;
        accumTup = null;

        initEvalFunc = null;
        intermedEvalFunc = null;
        finalEvalFunc = null;

        init = false;
    }

    /**
     * This function returns the ultimate result. It is when getValue() is called that
     * the Final EvalFunc's exec function is called on the accumulated data.
     */
    @Override
    public T getValue() {
        try {
            return finalEvalFunc.exec(mTupleFactory.newTuple(intermediateDB));
        } catch (IOException e) {
            throw new RuntimeException("Error in AlgebraicEvalFunc evaluating final method");
        }
    }

}
