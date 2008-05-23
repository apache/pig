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
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.impl.physicalLayer.expressionOperators.NotEqualToExpr;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNotEqualTo extends junit.framework.TestCase {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    public static boolean test(byte type) throws ExecException {
        Random r = new Random();
        ConstantExpression lt = (ConstantExpression) GenPhyOp.exprConst();
        lt.setResultType(type);
        ConstantExpression rt = (ConstantExpression) GenPhyOp.exprConst();
        rt.setResultType(type);
        NotEqualToExpr g = (NotEqualToExpr) GenPhyOp.compNotEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);

        Object inp1;
        Object inp2;
        Result res;
        Boolean ret;
        switch (type) {
        case DataType.BAG:
            inp1 = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
            inp2 = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((DataBag) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((DataBag) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        case DataType.BOOLEAN:
            inp1 = r.nextBoolean();
            inp2 = r.nextBoolean();
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((Boolean) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((Boolean) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        case DataType.BYTEARRAY:
            inp1 = GenRandomData.genRandDBA(r);
            inp2 = GenRandomData.genRandDBA(r);
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((DataByteArray) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((DataByteArray) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        case DataType.CHARARRAY:
            inp1 = GenRandomData.genRandString(r);
            inp2 = GenRandomData.genRandString(r);
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((String) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((String) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        case DataType.DOUBLE:
            inp1 = r.nextDouble();
            inp2 = r.nextDouble();
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((Double) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((Double) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        case DataType.FLOAT:
            inp1 = r.nextFloat();
            inp2 = r.nextFloat();
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((Float) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((Float) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        case DataType.INTEGER:
            inp1 = r.nextInt();
            inp2 = r.nextInt();
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((Integer) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((Integer) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        case DataType.LONG:
            inp1 = r.nextLong();
            inp2 = r.nextLong();
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((Long) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((Long) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        case DataType.MAP:
            inp1 = GenRandomData.genRandMap(r, 10);
            inp2 = GenRandomData.genRandMap(r, 10);
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((Map) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((Map) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        case DataType.TUPLE:
            inp1 = GenRandomData.genRandSmallBagTuple(r, 10, 100);
            inp2 = GenRandomData.genRandSmallBagTuple(r, 10, 100);
            lt.setValue(inp1);
            rt.setValue(inp1);
            res = g.getNext((Tuple) inp1);
            if ((Boolean) res.result == true)
                return false;
            rt.setValue(inp2);
            res = g.getNext((Tuple) inp1);
            ret = (DataType.compare(inp1, inp2) != 0);
            if (res.result.equals(ret))
                return true;
            return false;
        }
        return true;
    }

    @Test
    public void testOperator() throws ExecException {
        int TRIALS = 10;
        byte[] types = DataType.genAllTypes();
        Map<Byte, String> map = DataType.genTypeToNameMap();
        // System.out.println("Testing Not Equal To Expression:");

        long t1 = System.currentTimeMillis();

        for (byte b : types) {
            boolean succ = true;
            // System.out.print("\t With " + map.get(b) + ": ");
            for (int i = 0; i < TRIALS; i++) {
                succ &= test(b);
            }
            assertEquals(true, succ);
            /*
             * if (succ) System.out.println("Success!!"); else
             * System.out.println("Failure");
             */
        }
    }
}
