package org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.ExprPlanVisitor;
import org.apache.pig.backend.executionengine.ExecException;


/**
 * This class implements a Constant of any type.
 * Its value can be set using the setValue method. 
 *
 */
public class ConstantExpression extends ExpressionOperator {
	
	private Log log = LogFactory.getLog(getClass());
	
	//The value that this constant represents
	Object value;
	
	//The result of calling getNext
	Result res = new Result();

	public ConstantExpression(OperatorKey k) {
		this(k,-1);
	}

	public ConstantExpression(OperatorKey k, int rp) {
		super(k, rp);
	}

	@Override
	public String name() {
		return "Constant(" + value.toString() +") - " + mKey.toString();
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
	public String typeName() {
		return getClass().getName();
	}

	@Override
	public void visit(ExprPlanVisitor v) throws ParseException {
		v.visitConstant(this);
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public Result getNext(DataBag db) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (DataBag)value;
		return res;
	}

	@Override
	public Result getNext(DataByteArray ba) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (DataByteArray)value;
		return res;
	}

	@Override
	public Result getNext(Double d) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (Double)value;
		return res;
	}

	@Override
	public Result getNext(Float f) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (Float)value;
		return res;
	}

	@Override
	public Result getNext(Integer i) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (Integer)value;
		return res;
	}

	@Override
	public Result getNext(Long l) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (Long)value;
		return res;
	}

	@Override
	public Result getNext(String s) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (String)value;
		return res;
	}

	@Override
	public Result getNext(Tuple t) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (Tuple)value;
		return res;
	}
	
	
	
	@Override
	public Result getNext(Boolean b) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (Boolean)value;
		return res;
	}

	@Override
	public Result getNext(Map m) throws ExecException {
		
		res.returnStatus = POStatus.STATUS_OK;
		res.result = (Map)value;
		return res;
	}
}
