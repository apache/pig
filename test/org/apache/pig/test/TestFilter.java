package org.apache.pig.test;

import static org.junit.Assert.*;

import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFilter {
	POFilter pass;
	POFilter fail;
	Tuple t;
	
	@Before
	public void setUp() throws Exception {
		pass = GenPhyOp.topFilterOpWithExPlan(50, 25);
		fail = GenPhyOp.topFilterOpWithExPlan(25, 50);
		
		t = GenRandomData.genRandSmallBagTuple(new Random(), 10, 100);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetNextTuple() throws ExecException {
		pass.attachInput(t);
		Result res = pass.getNext(t);
		assertEquals(t, res.result);
		fail.attachInput(t);
		res = fail.getNext(t);
		assertEquals(res.returnStatus, POStatus.STATUS_EOP);
	}

}
