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

package org.apache.pig.impl.physicalLayer.topLevelOperators;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

public class POUserFunc extends PhysicalOperator<PhyPlanVisitor> {

    transient EvalFunc func;
    Tuple t1, t2;
    private final Log log = LogFactory.getLog(getClass());
    String funcSpec;
    private final byte INITIAL = 0;
    private final byte INTERMEDIATE = 1;
    private final byte FINAL = 2;

    public POUserFunc(OperatorKey k, int rp, List inp) {
        super(k, rp);
        inputs = inp;

    }

    public POUserFunc(OperatorKey k, int rp, List inp, String funcSpec) {
        this(k, rp, inp, funcSpec, null);

        instantiateFunc();
    }
    
    public POUserFunc(OperatorKey k, int rp, List inp, String funcSpec, EvalFunc func) {
        super(k, rp, inp);
        this.funcSpec = funcSpec;
        this.func = func;

    }

    private void instantiateFunc() {
        this.func = (EvalFunc) PigContext.instantiateFuncFromSpec(this.funcSpec);
    }

    private Result getNext() throws ExecException {
        Tuple t = null;
        Result result = new Result();
        // instantiate the function if its null
        if (func == null)
            instantiateFunc();

        try {
            if (inputAttached) {
                result.result = func.exec(input);
                result.returnStatus = (result.result != null) ? POStatus.STATUS_OK
                        : POStatus.STATUS_EOP;
                return result;
            } else {
                Result in = inputs.get(0).getNext(t);
                if (in.returnStatus == POStatus.STATUS_EOP) {
                    result.returnStatus = POStatus.STATUS_EOP;
                    return result;
                }
                result.result = func.exec((Tuple) in.result);
                result.returnStatus = POStatus.STATUS_OK;
                return result;
            }
        } catch (IOException e) {
            log.error(e);
            //throw new ExecException(e.getCause());
        }
        result.returnStatus = POStatus.STATUS_ERR;
        return result;
    }

    @Override
    public Result getNext(Tuple tIn) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(DataBag db) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Boolean b) throws ExecException {

        return getNext();
    }

    @Override
    public Result getNext(DataByteArray ba) throws ExecException {

        return getNext();
    }

    @Override
    public Result getNext(Double d) throws ExecException {

        return getNext();
    }

    @Override
    public Result getNext(Float f) throws ExecException {

        return getNext();
    }

    @Override
    public Result getNext(Long l) throws ExecException {

        return getNext();
    }

    @Override
    public Result getNext(Map m) throws ExecException {

        return getNext();
    }

    @Override
    public Result getNext(String s) throws ExecException {

        return getNext();
    }

    public void setAlgebraicFunction(Byte Function) {
        // This will only be used by the optimizer for putting correct functions
        // in the mapper,
        // combiner and reduce. This helps in maintaining the physical plan as
        // is without the
        // optimiser having to replace any operators.
        // You wouldn't be able to make two calls to this function on the same
        // algebraic EvalFunc as
        // func is being changed.
        switch (Function) {
        case INITIAL:
            func = (EvalFunc) PigContext.instantiateFuncFromSpec(getInitial());
            setResultType(DataType.findType(((EvalFunc) func).getReturnType()));
            break;
        case INTERMEDIATE:
            func = (EvalFunc) PigContext.instantiateFuncFromSpec(getIntermed());
            setResultType(DataType.findType(((EvalFunc) func).getReturnType()));
            break;
        case FINAL:
            func = (EvalFunc) PigContext.instantiateFuncFromSpec(getFinal());
            setResultType(DataType.findType(((EvalFunc) func).getReturnType()));
            break;

        }
    }

    public String getInitial() {
        if (func == null)
            instantiateFunc();

        if (func instanceof Algebraic) {
            return ((Algebraic) func).getInitial();
        } else {
            log
                    .error("Attempt to run a non-algebraic function as an algebraic function");
        }
        return null;
    }

    public String getIntermed() {
        if (func == null)
            instantiateFunc();

        if (func instanceof Algebraic) {
            return ((Algebraic) func).getIntermed();
        } else {
            log
                    .error("Attempt to run a non-algebraic function as an algebraic function");
        }
        return null;
    }

    public String getFinal() {
        if (func == null)
            instantiateFunc();

        if (func instanceof Algebraic) {
            return ((Algebraic) func).getFinal();
        } else {
            log
                    .error("Attempt to run a non-algebraic function as an algebraic function");
        }
        return null;
    }

    public Type getReturnType() {
        if (func == null)
            instantiateFunc();

        return func.getReturnType();
    }

    public void finish() {
        if (func == null)
            instantiateFunc();

        func.finish();
    }

    public Schema outputSchema(Schema input) {
        if (func == null)
            instantiateFunc();

        return func.outputSchema(input);
    }

    public Boolean isAsynchronous() {
        if (func == null)
            instantiateFunc();
        
        return func.isAsynchronous();
    }

    @Override
    public String name() {
        if(funcSpec!=null)
            return "POUserFunc" + "(" + funcSpec + ")" + " - " + mKey.toString();
        else
            return "POUserFunc" + "(" + "DummySpec" + ")" + " - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {

        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {

        return false;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {

        v.visitUserFunc(this);
    }

}
