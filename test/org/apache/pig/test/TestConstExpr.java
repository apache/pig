package org.apache.pig.test;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ConstantExpression;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestConstExpr {
	Random r = new Random();
	ConstantExpression ce = (ConstantExpression) GenPhyOp.exprConst();
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetNextInteger() throws ExecException {
		Integer inp = r.nextInt();
		ce.setValue(inp);
		Result resi = ce.getNext(inp);
		Integer ret = (Integer)resi.result;
		assertEquals(inp, ret);
	}

	@Test
	public void testGetNextLong() throws ExecException {
		Long inp = r.nextLong();
		ce.setValue(inp);
		Result resl = ce.getNext(inp);
		Long ret = (Long)resl.result;
		assertEquals(inp, ret);
	}

	@Test
	public void testGetNextDouble() throws ExecException {
		Double inp = r.nextDouble();
		ce.setValue(inp);
		Result resd = ce.getNext(inp);
		Double ret = (Double)resd.result;
		assertEquals(inp, ret);
	}

	@Test
	public void testGetNextFloat() throws ExecException {
		Float inp = r.nextFloat();
		ce.setValue(inp);
		Result resf = ce.getNext(inp);
		Float ret = (Float)resf.result;
		assertEquals(inp, ret);
	}

	@Test
	public void testGetNextString() throws ExecException {
		String inp = GenRandomData.genRandString(r);
		ce.setValue(inp);
		Result ress = ce.getNext(inp);
		String ret = (String)ress.result;
		assertEquals(inp, ret);
	}

	@Test
	public void testGetNextDataByteArray() throws ExecException {
		DataByteArray inp = GenRandomData.genRandDBA(r);
		ce.setValue(inp);
		Result resba = ce.getNext(inp);
		DataByteArray ret = (DataByteArray)resba.result;
		assertEquals(inp, ret);
	}

	@Test
	public void testGetNextMap() throws ExecException {
		Map<Integer,String> inp = GenRandomData.genRandMap(r, 10);
		ce.setValue(inp);
		Result resm = ce.getNext(inp);
		Map<Integer,String> ret = (Map)resm.result;
		assertEquals(inp, ret);
	}

	@Test
	public void testGetNextBoolean() throws ExecException {
		Boolean inp = r.nextBoolean();
		ce.setValue(inp);
		Result res = ce.getNext(inp);
		Boolean ret = (Boolean)res.result;
		assertEquals(inp, ret);
	}

	@Test
	public void testGetNextTuple() throws ExecException {
		Tuple inp = GenRandomData.genRandSmallBagTuple(r, 10, 100);
		ce.setValue(inp);
		Result rest = ce.getNext(inp);
		Tuple ret = (Tuple)rest.result;
		assertEquals(inp, ret);
	}

	@Test
	public void testGetNextDataBag() throws ExecException {
		DataBag inp = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
		ce.setValue(inp);
		Result res = ce.getNext(inp);
		DataBag ret = (DataBag)res.result;
		assertEquals(inp, ret);
	}

}
