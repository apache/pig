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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Add;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.test.utils.GenRandomData;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class TestAdd {
    Random r = new Random(42L);
    ConstantExpression lt, rt;
    Add op = new Add(new OperatorKey("", r.nextLong()));

    @Before
    public void setUp() throws Exception {
        lt = new ConstantExpression(new OperatorKey("", r.nextLong()));
        rt = new ConstantExpression(new OperatorKey("", r.nextLong()));
    }

    @Test
    public void testOperator() throws ExecException {
        // int TRIALS = 10;
        byte[] types = { DataType.BAG, DataType.BOOLEAN, DataType.BYTEARRAY, DataType.CHARARRAY,
                        DataType.DOUBLE, DataType.FLOAT, DataType.INTEGER, DataType.LONG,
                        DataType.DATETIME, DataType.MAP, DataType.TUPLE, DataType.BIGINTEGER, DataType.BIGDECIMAL };
        // Map<Byte,String> map = GenRandomData.genTypeToNameMap();
        System.out.println("Testing Add operator");
        for (byte type : types) {
            lt.setResultType(type);
            rt.setResultType(type);
            op.setLhs(lt);
            op.setRhs(rt);

            switch (type) {
            case DataType.BAG:
                DataBag inpdb1 = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
                DataBag inpdb2 = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
                lt.setValue(inpdb1);
                rt.setValue(inpdb2);
                Result resdb = op.getNextDataBag();
                assertEquals(POStatus.STATUS_ERR, resdb.returnStatus);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpdb2);
                resdb = op.getNextDataBag();
                assertEquals(POStatus.STATUS_ERR, resdb.returnStatus);
                // test with null in rhs
                lt.setValue(inpdb1);
                rt.setValue(null);
                resdb = op.getNextDataBag();
                assertEquals(POStatus.STATUS_ERR, resdb.returnStatus);
                break;
            case DataType.BOOLEAN:
                Boolean inpb1 = r.nextBoolean();
                Boolean inpb2 = r.nextBoolean();
                lt.setValue(inpb1);
                rt.setValue(inpb2);
                Result resb = op.getNextBoolean();
                assertEquals(POStatus.STATUS_ERR, resb.returnStatus);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpb2);
                resb = op.getNextBoolean();
                assertEquals(POStatus.STATUS_ERR, resb.returnStatus);
                // test with null in rhs
                lt.setValue(inpb1);
                rt.setValue(null);
                resb = op.getNextBoolean();
                assertEquals(POStatus.STATUS_ERR, resb.returnStatus);
                break;
            case DataType.BYTEARRAY: {
                DataByteArray inpba1 = GenRandomData.genRandDBA(r);
                DataByteArray inpba2 = GenRandomData.genRandDBA(r);
                lt.setValue(inpba1);
                rt.setValue(inpba2);
                Result resba = op.getNextDataByteArray();
                // DataByteArray expected = new DataByteArray(inpba1.toString() +
                // inpba2.toString());
                // assertEquals(expected, (DataByteArray)resba.result);
                assertEquals(POStatus.STATUS_ERR, resba.returnStatus);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpba2);
                resba = op.getNextDataByteArray();
                assertEquals(POStatus.STATUS_ERR, resba.returnStatus);
                // test with null in rhs
                lt.setValue(inpba1);
                rt.setValue(null);
                resba = op.getNextDataByteArray();
                assertEquals(POStatus.STATUS_ERR, resba.returnStatus);
                break;
            }
            case DataType.CHARARRAY: {
                String inps1 = GenRandomData.genRandString(r);
                String inps2 = GenRandomData.genRandString(r);
                lt.setValue(inps1);
                rt.setValue(inps2);
                Result ress = op.getNextString();
                /*
                 * String expected = new String(inps1 + inps2);
                 * assertEquals(expected, (String)ress.result);
                 */
                assertEquals(POStatus.STATUS_ERR, ress.returnStatus);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inps2);
                ress = op.getNextString();
                assertEquals(POStatus.STATUS_ERR, ress.returnStatus);
                // test with null in rhs
                lt.setValue(inps1);
                rt.setValue(null);
                ress = op.getNextString();
                assertEquals(POStatus.STATUS_ERR, ress.returnStatus);
                break;
            }
            case DataType.DOUBLE: {
                Double inpd1 = r.nextDouble();
                Double inpd2 = r.nextDouble();
                lt.setValue(inpd1);
                rt.setValue(inpd2);
                Result resd = op.getNextDouble();
                Double expected = new Double(inpd1 + inpd2);
                assertEquals(expected, (Double) resd.result);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpd2);
                resd = op.getNextDouble();
                assertNull(resd.result);
                // test with null in rhs
                lt.setValue(inpd1);
                rt.setValue(null);
                resd = op.getNextDouble();
                assertNull(resd.result);
                break;
            }
            case DataType.FLOAT: {
                Float inpf1 = r.nextFloat();
                Float inpf2 = r.nextFloat();
                lt.setValue(inpf1);
                rt.setValue(inpf2);
                Result resf = op.getNextFloat();
                Float expected = new Float(inpf1 + inpf2);
                assertEquals(expected, (Float) resf.result);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpf2);
                resf = op.getNextFloat();
                assertNull(resf.result);
                // test with null in rhs
                lt.setValue(inpf1);
                rt.setValue(null);
                resf = op.getNextFloat();
                assertNull(resf.result);
                break;
            }
            case DataType.INTEGER: {
                Integer inpi1 = r.nextInt();
                Integer inpi2 = r.nextInt();
                lt.setValue(inpi1);
                rt.setValue(inpi2);
                Result resi = op.getNextInteger();
                Integer expected = new Integer(inpi1 + inpi2);
                assertEquals(expected, (Integer) resi.result);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpi2);
                resi = op.getNextInteger();
                assertNull(resi.result);
                // test with null in rhs
                lt.setValue(inpi1);
                rt.setValue(null);
                resi = op.getNextInteger();
                assertNull(resi.result);
                break;
            }
            case DataType.LONG: {
                Long inpl1 = r.nextLong();
                Long inpl2 = r.nextLong();
                lt.setValue(inpl1);
                rt.setValue(inpl2);
                Result resl = op.getNextLong();
                Long expected = new Long(inpl1 + inpl2);
                assertEquals(expected, (Long) resl.result);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpl2);
                resl = op.getNextLong();
                assertNull(resl.result);
                // test with null in rhs
                lt.setValue(inpl1);
                rt.setValue(null);
                resl = op.getNextLong();
                assertNull(resl.result);
                break;
            }
            case DataType.DATETIME:
                DateTime inpdt1 = new DateTime(r.nextLong());
                DateTime inpdt2 = new DateTime(r.nextLong());
                lt.setValue(inpdt1);
                rt.setValue(inpdt2);
                Result resdt = op.getNextDateTime();
                assertEquals(POStatus.STATUS_ERR, resdt.returnStatus);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpdt2);
                resdt = op.getNextDateTime();
                assertEquals(POStatus.STATUS_ERR, resdt.returnStatus);
                // test with null in rhs
                lt.setValue(inpdt1);
                rt.setValue(null);
                resdt = op.getNextDateTime();
                assertEquals(POStatus.STATUS_ERR, resdt.returnStatus);
                break;
            case DataType.MAP: {
                Map<String, Object> inpm1 = GenRandomData.genRandMap(r, 10);
                Map<String, Object> inpm2 = GenRandomData.genRandMap(r, 10);
                lt.setValue(inpm1);
                rt.setValue(inpm2);
                Result resm = op.getNextMap();
                assertEquals(POStatus.STATUS_ERR, resm.returnStatus);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpm2);
                resm = op.getNextMap();
                assertEquals(POStatus.STATUS_ERR, resm.returnStatus);
                // test with null in rhs
                lt.setValue(inpm1);
                rt.setValue(null);
                resm = op.getNextMap();
                assertEquals(POStatus.STATUS_ERR, resm.returnStatus);
                break;
            }
            case DataType.TUPLE: {
                Tuple inpt1 = GenRandomData.genRandSmallBagTuple(r, 10, 100);
                Tuple inpt2 = GenRandomData.genRandSmallBagTuple(r, 10, 100);
                lt.setValue(inpt1);
                rt.setValue(inpt2);
                Result rest = op.getNextTuple();
                assertEquals(POStatus.STATUS_ERR, rest.returnStatus);

                // test with null in lhs
                lt.setValue(null);
                rt.setValue(inpt2);
                rest = op.getNextTuple();
                assertEquals(POStatus.STATUS_ERR, rest.returnStatus);
                // test with null in rhs
                lt.setValue(inpt1);
                rt.setValue(null);
                rest = op.getNextTuple();
                assertEquals(POStatus.STATUS_ERR, rest.returnStatus);
                break;
            } //TODO add case for BigInteger and BigDecimal
            }
        }
    }
}
