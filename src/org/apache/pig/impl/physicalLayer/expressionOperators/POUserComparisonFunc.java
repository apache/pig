package org.apache.pig.impl.physicalLayer.expressionOperators;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ComparisonFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;

public class POUserComparisonFunc extends POUserFunc {

	/**
     * 
     */
    private static final long serialVersionUID = 1L;
    transient ComparisonFunc func;
	private Log log = LogFactory.getLog(getClass());
	
	public POUserComparisonFunc(OperatorKey k, int rp, List inp, String funcSpec, ComparisonFunc func) {
		super(k, rp, inp);
		this.funcSpec = funcSpec;
		this.func = func;
        if(func==null)
            instantiateFunc();
	}
	
	public POUserComparisonFunc(OperatorKey k, int rp, List inp, String funcSpec) {
		this(k, rp, inp, funcSpec, null);
	}
	
	private void instantiateFunc() {
		this.func = (ComparisonFunc) PigContext.instantiateFuncFromSpec(this.funcSpec);
        this.func.setReporter(reporter);
	}
	
	public ComparisonFunc getComparator() {
		return func;
	}
	
	@Override
	public Result getNext(Integer i) throws ExecException {
		Result result = new Result();

		result.result = func.compare(t1, t2);
		result.returnStatus = (t1 != null && t2 != null) ? POStatus.STATUS_OK
				: POStatus.STATUS_ERR;
		// the two attached tuples are used up now. So we set the
		// inputAttached flag to false
		inputAttached = false;
		return result;

	}
	
	private Result getNext() {
		Result res = null;
		log.error("getNext being called with non-integer");
		return res;
	}
	
	@Override
	public Result getNext(Boolean b) throws ExecException {
		return getNext();
	}

	@Override
	public Result getNext(DataBag db) throws ExecException {
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

	@Override
	public Result getNext(Tuple in) throws ExecException {
		return getNext();
	}

	public void attachInput(Tuple t1, Tuple t2) {
		this.t1 = t1;
		this.t2 = t2;
		inputAttached = true;

	}
    
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException{
        is.defaultReadObject();
        instantiateFunc();
    }

}
