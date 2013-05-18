/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Map;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.Test;

public class TestConstExpr {
    Random r = new Random(42L);
    ConstantExpression ce = (ConstantExpression)GenPhyOp.exprConst();

    @Test
    public void testGetNextInteger() throws ExecException {
        Integer inp = r.nextInt();
        ce.setValue(inp);
        Result resi = ce.getNextInteger();
        Integer ret = (Integer)resi.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resi = ce.getNextInteger();
        ret = (Integer)resi.result;
        assertNull(ret);
    }

    @Test
    public void testGetNextLong() throws ExecException {
        Long inp = r.nextLong();
        ce.setValue(inp);
        Result resl = ce.getNextLong();
        Long ret = (Long)resl.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resl = ce.getNextLong();
        ret = (Long)resl.result;
        assertNull(ret);
    }

    @Test
    public void testGetNextDouble() throws ExecException {
        Double inp = r.nextDouble();
        ce.setValue(inp);
        Result resd = ce.getNextDouble();
        Double ret = (Double)resd.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resd = ce.getNextDouble();
        ret = (Double)resd.result;
        assertNull(ret);
    }

    @Test
    public void testGetNextFloat() throws ExecException {
        Float inp = r.nextFloat();
        ce.setValue(inp);
        Result resf = ce.getNextFloat();
        Float ret = (Float)resf.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resf = ce.getNextFloat();
        ret = (Float)resf.result;
        assertNull(ret);
    }

    @Test
    public void testGetNextString() throws ExecException {
        String inp = GenRandomData.genRandString(r);
        ce.setValue(inp);
        Result ress = ce.getNextString();
        String ret = (String)ress.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        ress = ce.getNextString();
        ret = (String)ress.result;
        assertNull(ret);
    }

    @Test
    public void testGetNextDataByteArray() throws ExecException {
        DataByteArray inp = GenRandomData.genRandDBA(r);
        ce.setValue(inp);
        Result resba = ce.getNextDataByteArray();
        DataByteArray ret = (DataByteArray)resba.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resba = ce.getNextDataByteArray();
        ret = (DataByteArray)resba.result;
        assertNull(ret);
    }

    @Test
    public void testGetNextMap() throws ExecException {
        Map<String, Object> inp = GenRandomData.genRandMap(r, 10);
        ce.setValue(inp);
        Result resm = ce.getNextMap();
        Map<Integer, String> ret = (Map)resm.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        resm = ce.getNextMap();
        ret = (Map)resm.result;
        assertNull(ret);
    }

    @Test
    public void testGetNextBoolean() throws ExecException {
        Boolean inp = r.nextBoolean();
        ce.setValue(inp);
        Result res = ce.getNextBoolean();
        Boolean ret = (Boolean)res.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        res = ce.getNextBoolean();
        ret = (Boolean)res.result;
        assertNull(ret);
    }

    @Test
    public void testGetNextTuple() throws ExecException {
        Tuple inp = GenRandomData.genRandSmallBagTuple(r, 10, 100);
        ce.setValue(inp);
        Result rest = ce.getNextTuple();
        Tuple ret = (Tuple)rest.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        rest = ce.getNextTuple();
        ret = (Tuple)rest.result;
        assertNull(ret);
    }

    @Test
    public void testGetNextDataBag() throws ExecException {
        DataBag inp = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
        ce.setValue(inp);
        Result res = ce.getNextDataBag();
        DataBag ret = (DataBag)res.result;
        assertEquals(inp, ret);

        // test with null input
        ce.setValue(null);
        res = ce.getNextDataBag();
        ret = (DataBag)res.result;
        assertNull(ret);
    }

}
