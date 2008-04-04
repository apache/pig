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

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ConstantExpression;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GTOrEqualToExpr;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestGTOrEqual {

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
        GTOrEqualToExpr g = (GTOrEqualToExpr) GenPhyOp.compGTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);

        switch (type) {
        case DataType.BAG:
            DataBag inpdb1 = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
            DataBag inpdb2 = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
            lt.setValue(inpdb1);
            rt.setValue(inpdb2);
            Result resdb = g.getNext(inpdb1);
            if (resdb.returnStatus == POStatus.STATUS_ERR) {
                return true;
            }
            return false;
        case DataType.BOOLEAN:
            Boolean inpb1 = r.nextBoolean();
            Boolean inpb2 = r.nextBoolean();
            lt.setValue(inpb1);
            rt.setValue(inpb2);
            Result resb = g.getNext(inpb1);
            if (resb.returnStatus == POStatus.STATUS_ERR) {
                return true;
            }
            return false;
        case DataType.BYTEARRAY:
            DataByteArray inpba1 = GenRandomData.genRandDBA(r);
            DataByteArray inpba2 = GenRandomData.genRandDBA(r);
            lt.setValue(inpba1);
            rt.setValue(inpba2);
            Result resba = g.getNext(inpba1);
            boolean retba = (inpba1.compareTo(inpba2) == -1 || inpba1
                    .compareTo(inpba2) == 0);
            if ((Boolean) resba.result == retba)
                return true;
            return false;
        case DataType.CHARARRAY:
            String inps1 = GenRandomData.genRandString(r);
            String inps2 = GenRandomData.genRandString(r);
            lt.setValue(inps1);
            rt.setValue(inps2);
            Result ress = g.getNext(inps1);
            boolean rets = (inps1.compareTo(inps2) >= 0);
            if ((Boolean) ress.result == rets)
                return true;
            return false;
        case DataType.DOUBLE:
            Double inpd1 = r.nextDouble();
            Double inpd2 = r.nextDouble();
            lt.setValue(inpd1);
            rt.setValue(inpd2);
            Result resd = g.getNext(inpd1);
            boolean retd = (inpd1 >= inpd2);
            if ((Boolean) resd.result == retd)
                return true;
            return false;
        case DataType.FLOAT:
            Float inpf1 = r.nextFloat();
            Float inpf2 = r.nextFloat();
            lt.setValue(inpf1);
            rt.setValue(inpf2);
            Result resf = g.getNext(inpf1);
            boolean retf = (inpf1 >= inpf2);
            if ((Boolean) resf.result == retf)
                return true;
            return false;
        case DataType.INTEGER:
            Integer inpi1 = r.nextInt();
            Integer inpi2 = r.nextInt();
            lt.setValue(inpi1);
            rt.setValue(inpi2);
            Result resi = g.getNext(inpi1);
            boolean reti = (inpi1 >= inpi2);
            if ((Boolean) resi.result == reti)
                return true;
            return false;
        case DataType.LONG:
            Long inpl1 = r.nextLong();
            Long inpl2 = r.nextLong();
            lt.setValue(inpl1);
            rt.setValue(inpl2);
            Result resl = g.getNext(inpl1);
            boolean retl = (inpl1 >= inpl2);
            if ((Boolean) resl.result == retl)
                return true;
            return false;
        case DataType.MAP:
            Map<Integer, String> inpm1 = GenRandomData.genRandMap(r, 10);
            Map<Integer, String> inpm2 = GenRandomData.genRandMap(r, 10);
            lt.setValue(inpm1);
            rt.setValue(inpm2);
            Result resm = g.getNext(inpm1);
            if (resm.returnStatus == POStatus.STATUS_ERR) {
                return true;
            }
            return false;
        case DataType.TUPLE:
            Tuple inpt1 = GenRandomData.genRandSmallBagTuple(r, 10, 100);
            Tuple inpt2 = GenRandomData.genRandSmallBagTuple(r, 10, 100);
            lt.setValue(inpt1);
            rt.setValue(inpt2);
            Result rest = g.getNext(inpt1);
            if (rest.returnStatus == POStatus.STATUS_ERR) {
                return true;
            }
            return false;
        }
        return true;
    }

    @Test
    public void testOperator() throws ExecException {
        int TRIALS = 10;
        byte[] types = DataType.genAllTypes();
        // Map<Byte,String> map = DataType.genTypeToNameMap();
        // System.out.println("Testing Greater Than Or Equal To Expression:");
        for (byte b : types) {
            boolean succ = true;
            // System.out.print( "\t With " + map.get(b) + ": ");
            for (int i = 0; i < TRIALS; i++) {
                succ &= test(b);
            }
            assertEquals(true, succ);
            /*
             * if(succ) System.out.println("Success!!"); else
             * System.out.println("Failure");
             */
        }
    }

}
