package org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.ExprPlanVisitor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.physicalLayer.util.operatorHelper;

/**
 * Implements the overloaded form of the project operator.
 * Projects the specified column from the input tuple.
 * However, if asked for tuples when the input is a bag,
 * the overloaded form is invoked and the project streams
 * the tuples through instead of the bag.
 */
public class POProject extends ExpressionOperator {
	
	private Log log = LogFactory.getLog(getClass());
	
	//The column to project
	int column = 0;
	
	//True if we are in the middle of streaming tuples
	//in a bag
	boolean processingBagOfTuples = false;
	
	//The bag iterator used while straeming tuple
	Iterator<Tuple> bagIterator = null;
	
	//Temporary tuple
	Tuple temp = null;
	
	public POProject(OperatorKey k) {
		this(k,-1,0);
	}

	public POProject(OperatorKey k, int rp) {
		this(k, rp, 0);
	}
	
	public POProject(OperatorKey k, int rp, int col) {
		super(k, rp);
		this.column = col;
	}

	@Override
	public String name() {
		return "Project(" + column + ") - " + mKey.toString();
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
		v.visitProject(this);
	}
	
	
	/**
	 * Overridden since the attachment of the new input
	 * should cause the old processing to end.
	 */
	@Override
	public void attachInput(Tuple t) {
		super.attachInput(t);
		processingBagOfTuples = false;
	}
	
	/**
	 * Fetches the input tuple and returns the requested
	 * column
	 * @return
	 * @throws ExecException
	 */
	public Result getNext() throws ExecException{
		Result res = processInput();

		if(res.returnStatus != POStatus.STATUS_OK){
			if((res.returnStatus == POStatus.STATUS_ERR)){
				res.returnStatus = POStatus.STATUS_NULL;
			}
			return res;
		}
		try {
			res.result = ((Tuple)res.result).get(column);
		} catch (IOException e) {
			//Instead of propagating ERR through the pipeline
			//considering this a nullified operation and returning NULL 
			//res.returnStatus = POStatus.STATUS_ERR;
			res.returnStatus = POStatus.STATUS_NULL;
			log.fatal(e.getMessage());
		}
		return res;
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
	public Result getNext(Integer i) throws ExecException {
		return getNext();
	}

	@Override
	public Result getNext(Long l) throws ExecException {
		return getNext();
	}
	
	

	@Override
	public Result getNext(Boolean b) throws ExecException {
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
	
	/**
	 * Asked for Tuples. Check if the input is a bag.
	 * If so, stream the tuples in the bag instead of
	 * the entire bag.
	 */
	@Override
	public Result getNext(Tuple t) throws ExecException {
		Result res = new Result();
		if(!processingBagOfTuples){
			Tuple inpValue = null;
			res = processInput();
			if(res.returnStatus!=POStatus.STATUS_OK)
				return res;
			inpValue = (Tuple)res.result;
			res.result = null;
			
			try {
				Object ret = inpValue.get(column);
				if(ret instanceof DataBag){
					DataBag retBag = (DataBag)ret;
					bagIterator = retBag.iterator();
					if(bagIterator.hasNext()){
						processingBagOfTuples = true;
						res.result = bagIterator.next();
					}
				}
				else {
					res.result = (Tuple)ret;
				}
				return res;
			} catch (IOException e) {
				res.returnStatus = POStatus.STATUS_ERR;
				log.error(e.getMessage());
			}
		}
		if(bagIterator.hasNext()){
			res.result = bagIterator.next();
			res.returnStatus = POStatus.STATUS_OK;
			return res;
		}
		else{
			//done processing the bag of tuples
			processingBagOfTuples = false;
			return getNext(t);
		}
	}

	public int getColumn() {
		return column;
	}

	public void setColumn(int column) {
		this.column = column;
	}

}
