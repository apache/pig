package org.apache.pig.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.POProject;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestProject {
	Tuple typFinder;
	Random r;
	
	Tuple t;
	Result res;
	POProject proj;
	
	@Before
	public void setUp() throws Exception {
		r = new Random();
		typFinder = GenRandomData.genRandSmallBagTuple(r, 10, 100);
		t = GenRandomData.genRandSmallBagTuple(r,10,100);
		res = new Result();
		proj = GenPhyOp.exprProject();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetNext() throws ExecException, IOException {
		proj.attachInput(t);
		for(int j=0;j<t.size();j++){
			proj.attachInput(t);
			proj.setColumn(j);

			res = proj.getNext();
			assertEquals(POStatus.STATUS_OK, res.returnStatus);
			assertEquals(t.get(j), res.result);
		}
	}

}
