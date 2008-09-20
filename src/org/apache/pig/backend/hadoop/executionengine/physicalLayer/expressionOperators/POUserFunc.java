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

package org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;

public class POUserFunc extends ExpressionOperator {

	/**
     * 
     */
    private static final long serialVersionUID = 1L;
    transient EvalFunc func;
	
	private final Log log = LogFactory.getLog(getClass());
	FuncSpec funcSpec;
    FuncSpec origFSpec;
	public static final byte INITIAL = 0;
	public static final byte INTERMEDIATE = 1;
	public static final byte FINAL = 2;

	public POUserFunc(OperatorKey k, int rp, List<PhysicalOperator> inp) {
		super(k, rp);
		inputs = inp;

	}

	public POUserFunc(
            OperatorKey k,
            int rp,
            List<PhysicalOperator> inp,
            FuncSpec funcSpec) {
		this(k, rp, inp, funcSpec, null);
	}
	
	public POUserFunc(
            OperatorKey k,
            int rp,
            List<PhysicalOperator> inp,
            FuncSpec funcSpec,
            EvalFunc func) {
        super(k, rp);
        super.setInputs(inp);
		this.funcSpec = funcSpec;
        this.origFSpec = funcSpec;
		this.func = func;
        instantiateFunc(funcSpec);
	}

	private void instantiateFunc(FuncSpec fSpec) {
		this.func = (EvalFunc) PigContext.instantiateFuncFromSpec(fSpec);
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
                
                if(op instanceof POProject &&
                        op.getResultType() == DataType.TUPLE){
                    POProject projOp = (POProject)op;
                    if(projOp.isStar()){
                        Tuple trslt = (Tuple) temp.result;
                        Tuple rslt = (Tuple) res.result;
                        for(int i=0;i<trslt.size();i++)
                            rslt.append(trslt.get(i));
                        continue;
                    }
                }
                ((Tuple)res.result).append(temp.result);
			}
			res.returnStatus = temp.returnStatus;
			return res;
		}
	}

	private Result getNext() throws ExecException {
        Result result = processInput();
        String errMsg = "";
		try {
			if(result.returnStatus == POStatus.STATUS_OK) {
				result.result = func.exec((Tuple) result.result);
                if(resultType == DataType.BYTEARRAY) {
                    if(res.result != null && DataType.findType(result.result) != DataType.BYTEARRAY) {
                        result.result = new DataByteArray(result.result.toString().getBytes());
                    }
                }
				return result;
			}
			return result;
			
		} catch (IOException e1) {
		    errMsg = "Caught error from UDF " + funcSpec.getClassName() + 
            "[" + e1.getMessage() + "]";
			log.error(errMsg);
		}
		result.result = errMsg;
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

	public void setAlgebraicFunction(byte Function) {
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
            funcSpec = new FuncSpec(getInitial());
			break;
		case INTERMEDIATE:
            funcSpec = new FuncSpec(getIntermed());
			break;
		case FINAL:
            funcSpec = new FuncSpec(getFinal());
			break;

		}
        instantiateFunc(funcSpec);
        setResultType(DataType.findType(((EvalFunc<?>) func).getReturnType()));
	}

	public String getInitial() {
	    instantiateFunc(origFSpec);
		if (func instanceof Algebraic) {
			return ((Algebraic) func).getInitial();
		} else {
			String msg = new String("Attempt to run a non-algebraic function"
                + " as an algebraic function");
            log.error(msg);
            throw new RuntimeException(msg);
		}
	}

	public String getIntermed() {
        instantiateFunc(origFSpec);
		if (func instanceof Algebraic) {
			return ((Algebraic) func).getIntermed();
		} else {
			String msg = new String("Attempt to run a non-algebraic function"
                + " as an algebraic function");
            log.error(msg);
            throw new RuntimeException(msg);
		}
	}

	public String getFinal() {
        instantiateFunc(origFSpec);
		if (func instanceof Algebraic) {
			return ((Algebraic) func).getFinal();
		} else {
			String msg = new String("Attempt to run a non-algebraic function"
                + " as an algebraic function");
            log.error(msg);
            throw new RuntimeException(msg);
		}
	}

	public Type getReturnType() {
		return func.getReturnType();
	}

	public void finish() {
		func.finish();
	}

	public Schema outputSchema(Schema input) {
		return func.outputSchema(input);
	}

	public Boolean isAsynchronous() {
		return func.isAsynchronous();
	}

	@Override
	public String name() {
	    return "POUserFunc" + "(" + func.getClass().getName() + ")" + "[" + DataType.findTypeName(resultType) + "]" + " - " + mKey.toString();
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
	public void visit(PhyPlanVisitor v) throws VisitorException {

		v.visitUserFunc(this);
	}

    public FuncSpec getFuncSpec() {
        return funcSpec;
    }

    public boolean combinable() {
        return (func instanceof Algebraic);
    }

    @Override
    public POUserFunc clone() throws CloneNotSupportedException {
        // Inputs will be patched up later by PhysicalPlan.clone()
        POUserFunc clone = new POUserFunc(new OperatorKey(mKey.scope, 
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)),
            requestedParallelism, null, funcSpec.clone());
        clone.setResultType(resultType);
        return clone;
    }
    
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException{
        is.defaultReadObject();
        instantiateFunc(funcSpec);
    }
}
