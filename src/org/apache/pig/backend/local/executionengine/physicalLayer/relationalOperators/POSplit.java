package org.apache.pig.backend.local.executionengine.physicalLayer.relationalOperators;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POSplit extends PhysicalOperator {
    
    
    /**
     * POSplit is a blocking operator. It reads the data from its input into a databag and then returns the iterator
     * of that bag to POSplitOutputs which do the necessary filtering
     */
    private static final long serialVersionUID = 1L;

    DataBag data = null;
    
    boolean processingDone = false;

    public POSplit(OperatorKey k, int rp, List<PhysicalOperator> inp) {
	super(k, rp, inp);
	// TODO Auto-generated constructor stub
	data = BagFactory.getInstance().newDefaultBag();
    }

    public POSplit(OperatorKey k, int rp) {
	this(k, rp, null);
    }

    public POSplit(OperatorKey k, List<PhysicalOperator> inp) {
	
	this(k, -1, inp);
    }

    public POSplit(OperatorKey k) {
	 this(k, -1, null);
    }

    public Result getNext(Tuple t) throws ExecException{
	if(!processingDone) {
	    for(Result input = inputs.get(0).getNext(dummyTuple); input.returnStatus != POStatus.STATUS_EOP; input = inputs.get(0).getNext(dummyTuple)) {
		if(input.returnStatus == POStatus.STATUS_ERR) {
		    throw new ExecException("Error accumulating output at local Split operator");
		}
		data.add((Tuple) input.result);
	    }
	    processingDone = true;
	}

	Result res = new Result();
	res.returnStatus = POStatus.STATUS_OK;
	res.result = data.iterator();
	return res;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
	v.visitSplit(this);
    }

    @Override
    public String name() {
	return "Split - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
	// TODO Auto-generated method stub
	return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
	// TODO Auto-generated method stub
	return true;
    }

}
