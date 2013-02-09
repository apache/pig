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
package org.apache.pig.test;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestConstExpr extends junit.framework.TestCase {
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

        // test with null input
        ce.setValue(null);
        resi = ce.getNext(inp);
        ret  = (Integer)resi.result;
        assertEquals(null, ret);
    }

    @Test
    public void testGetNextLong() throws ExecException {
        Long inp = r.nextLong();
        ce.setValue(inp);
        Result resl = ce.getNext(inp);
        Long ret = (Long)resl.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resl = ce.getNext(inp);
        ret  = (Long)resl.result;
        assertEquals(null, ret);
    }

    @Test
    public void testGetNextDouble() throws ExecException {
        Double inp = r.nextDouble();
        ce.setValue(inp);
        Result resd = ce.getNext(inp);
        Double ret = (Double)resd.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resd = ce.getNext(inp);
        ret  = (Double)resd.result;
        assertEquals(null, ret);
    }

    @Test
    public void testGetNextFloat() throws ExecException {
        Float inp = r.nextFloat();
        ce.setValue(inp);
        Result resf = ce.getNext(inp);
        Float ret = (Float)resf.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resf = ce.getNext(inp);
        ret  = (Float)resf.result;
        assertEquals(null, ret);
    }

    @Test
    public void testGetNextString() throws ExecException {
        String inp = GenRandomData.genRandString(r);
        ce.setValue(inp);
        Result ress = ce.getNext(inp);
        String ret = (String)ress.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        ress = ce.getNext(inp);
        ret  = (String)ress.result;
        assertEquals(null, ret);
    }

    @Test
    public void testGetNextDataByteArray() throws ExecException {
        DataByteArray inp = GenRandomData.genRandDBA(r);
        ce.setValue(inp);
        Result resba = ce.getNext(inp);
        DataByteArray ret = (DataByteArray)resba.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resba = ce.getNext(inp);
        ret  = (DataByteArray)resba.result;
        assertEquals(null, ret);
    }

    @Test
    public void testGetNextMap() throws ExecException {
        Map<String,Object> inp = GenRandomData.genRandMap(r, 10);
        ce.setValue(inp);
        Result resm = ce.getNext(inp);
        Map<Integer,String> ret = (Map)resm.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resm = ce.getNext(inp);
        ret  = (Map)resm.result;
        assertEquals(null, ret);
    }

    @Test
    public void testGetNextBoolean() throws ExecException {
        Boolean inp = r.nextBoolean();
        ce.setValue(inp);
        Result res = ce.getNext(inp);
        Boolean ret = (Boolean)res.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        res = ce.getNext(inp);
        ret  = (Boolean)res.result;
        assertEquals(null, ret);
    }

    @Test
    public void testGetNextTuple() throws ExecException {
        Tuple inp = GenRandomData.genRandSmallBagTuple(r, 10, 100);
        ce.setValue(inp);
        Result rest = ce.getNext(inp);
        Tuple ret = (Tuple)rest.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        rest = ce.getNext(inp);
        ret  = (Tuple)rest.result;
        assertEquals(null, ret);
    }

    @Test
    public void testGetNextDataBag() throws ExecException {
        DataBag inp = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
        ce.setValue(inp);
        Result res = ce.getNext(inp);
        DataBag ret = (DataBag)res.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        res = ce.getNext(inp);
        ret  = (DataBag)res.result;
        assertEquals(null, ret);
    }

}
