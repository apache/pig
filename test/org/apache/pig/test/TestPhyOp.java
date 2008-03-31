package org.apache.pig.test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ConstantExpression;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GreaterThanExpr;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPhyOp {
	PhysicalOperator<PhyPlanVisitor> op;
	PhysicalOperator<PhyPlanVisitor> inpOp;
	Tuple t;

	@Before
	public void setUp() throws Exception {
		op = GenPhyOp.topFilterOp();
		inpOp = GenPhyOp.topFilterOpWithExPlan(25,10);
		t = GenRandomData.genRandSmallBagTuple(new Random(), 10, 100);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testProcessInput() throws ExecException {
		//Stand-alone tests
		Result res = op.processInput();
		assertEquals(POStatus.STATUS_EOP, res.returnStatus);
		op.attachInput(t);
		res = op.processInput();
		assertEquals(POStatus.STATUS_OK, res.returnStatus);
		assertEquals(t, res.result);
		op.detachInput();
		res = op.processInput();
		assertEquals(POStatus.STATUS_EOP, res.returnStatus);
		
		//With input operator
		List<PhysicalOperator<PhyPlanVisitor>> inp = new ArrayList<PhysicalOperator<PhyPlanVisitor>>();
		inp.add(inpOp);
		op.setInputs(inp);
		op.processInput();
		assertEquals(POStatus.STATUS_EOP, res.returnStatus);
		
		inpOp.attachInput(t);
		res = op.processInput();
		assertEquals(POStatus.STATUS_OK, res.returnStatus);
		assertEquals(t, res.result);
		inpOp.detachInput();
		res = op.processInput();
		assertEquals(POStatus.STATUS_EOP, res.returnStatus);
	}

}
