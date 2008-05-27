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

package org.apache.pig.impl.physicalLayer.expressionOperators;

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
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.ExprPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

public class POUserFunc extends ExpressionOperator {

	/**
     * 
     */
    private static final long serialVersionUID = 1L;
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
//		super(k, rp, inp);
        super(k, rp);
        super.setInputs(inp);
		this.funcSpec = funcSpec;
		this.func = func;
	}

	private void instantiateFunc() {
		this.func = (EvalFunc) PigContext.instantiateFuncFromSpec(this.funcSpec);
        this.func.setReporter(reporter);
	}
	
	public Result processInput() throws ExecException {

		Result res = new Result();
		Tuple inpValue = null;
		if (input == null && (inputs == null || inputs.size()==0)) {
//			log.warn("No inputs found. Signaling End of Processing.");
			res.returnStatus = POStatus.STATUS_EOP;
			return res;
		}

		//Should be removed once the model is clear
		if(reporter!=null) reporter.progress();

		
		if(isInputAttached()) {
			res.result = input;
			res.returnStatus = POStatus.STATUS_OK;
			detachInput();
			return res;
		} else {
			res.result = TupleFactory.getInstance().newTuple();
			
			Result temp = null;
			for(PhysicalOperator op : inputs) {
				switch(op.getResultType()){
                case DataType.BAG:
                    temp = op.getNext(dummyBag);
                    break;
                case DataType.BOOLEAN:
                    temp = op.getNext(dummyBool);
                    break;
                case DataType.BYTEARRAY:
                    temp = op.getNext(dummyDBA);
                    break;
                case DataType.CHARARRAY:
                    temp = op.getNext(dummyString);
                    break;
                case DataType.DOUBLE:
                    temp = op.getNext(dummyDouble);
                    break;
                case DataType.FLOAT:
                    temp = op.getNext(dummyFloat);
                    break;
                case DataType.INTEGER:
                    temp = op.getNext(dummyInt);
                    break;
                case DataType.LONG:
                    temp = op.getNext(dummyLong);
                    break;
                case DataType.MAP:
                    temp = op.getNext(dummyMap);
                    break;
                case DataType.TUPLE:
                    temp = op.getNext(dummyTuple);
                    break;
                }
                if(temp.returnStatus!=POStatus.STATUS_OK)
                    return temp;
                ((Tuple)res.result).append(temp.result);
                
			}
			res.returnStatus = temp.returnStatus;
			return res;
		}
	}

	private Result getNext() throws ExecException {
		Tuple t = null;
		Result result;
		// instantiate the function if its null
		if (func == null)
			instantiateFunc();

		result = processInput();
		try {
			if(result.returnStatus == POStatus.STATUS_OK) {
				result.result = func.exec((Tuple) result.result);
				return result;
			}
			return result;
			
		} catch (IOException e1) {
			log.error(e1);
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

		return true;
	}

	@Override
	public boolean supportsMultipleOutputs() {

		return false;
	}

	@Override
	public void visit(ExprPlanVisitor v) throws VisitorException {

		v.visitUserFunc(this);
	}

    public String getFuncSpec() {
        return funcSpec;
    }

}
