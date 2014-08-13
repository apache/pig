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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LTOrEqualToExpr;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.test.utils.GenPhyOp;
import org.joda.time.DateTime;
import org.junit.Test;

public class TestLTOrEqual {
    @Test
    public void testIntegerGt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Integer(1));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Integer(0));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.INTEGER);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertFalse((Boolean)r.result);
    }

    @Test
    public void testIntegerLt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Integer(0));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Integer(1));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.INTEGER);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testIntegerEq() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Integer(1));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Integer(1));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.INTEGER);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testIntegerAndNullValues() throws Exception {
        checkNullValues(DataType.INTEGER, new Integer(1));
    }

    @Test
    public void testLongGt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Long(1L));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Long(0L));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.LONG);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertFalse((Boolean)r.result);
    }

    @Test
    public void testLongLt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Long(0L));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Long(1L));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.LONG);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testLongEq() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Long(1L));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Long(1L));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.LONG);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testLongAndNullValues() throws Exception {
        checkNullValues(DataType.LONG, new Long(1L));
    }

    @Test
    public void testFloatGt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Float(1.0f));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Float(0.0f));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.FLOAT);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertFalse((Boolean)r.result);
    }

    @Test
    public void testFloatLt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Float(0.0f));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Float(1.0f));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.FLOAT);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testFloatEq() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Float(1.0f));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Float(1.0f));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.FLOAT);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testFloatAndNullValues() throws Exception {
        checkNullValues(DataType.FLOAT, new Float(1.0f));
    }

    @Test
    public void testDoubleGt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Double(1.0));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Double(0.0));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.DOUBLE);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertFalse((Boolean)r.result);
    }

    @Test
    public void testDoubleLt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Double(0.0));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Double(1.0));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.DOUBLE);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testDoubleEq() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new Double(1.0));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new Double(1.0));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.DOUBLE);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testDoubleAndNullValues() throws Exception {
        checkNullValues(DataType.DOUBLE, new Double(1.0));
    }

    @Test
    public void testDateTimeGt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new DateTime(1L));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new DateTime(0L));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.DATETIME);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertFalse((Boolean)r.result);
    }

    @Test
    public void testDateTimeLt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new DateTime(0L));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new DateTime(1L));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.DATETIME);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testDateTimeEq() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new DateTime(1L));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new DateTime(1L));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.DATETIME);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testDateTimeAndNullValues() throws Exception {
        checkNullValues(DataType.DATETIME, new DateTime(1L));
    }

    @Test
    public void testStringGt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new String("b"));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new String("a"));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.CHARARRAY);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertFalse((Boolean)r.result);
    }

    @Test
    public void testStringLt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new String("a"));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new String("b"));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.CHARARRAY);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testStringEq() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new String("b"));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new String("b"));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.CHARARRAY);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testStringAndNullValues() throws Exception {
        checkNullValues(DataType.CHARARRAY, new String("b"));
    }

    @Test
    public void testDataByteArrayGt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new DataByteArray("b"));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new DataByteArray("a"));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.BYTEARRAY);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertFalse((Boolean)r.result);
    }

    @Test
    public void testDataByteArrayLt() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new DataByteArray("a"));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new DataByteArray("b"));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.BYTEARRAY);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testDataByteArrayEq() throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        lt.setValue(new DataByteArray("b"));
        ConstantExpression rt = GenPhyOp.exprConst();
        rt.setValue(new DataByteArray("b"));
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();
        g.setLhs(lt);
        g.setRhs(rt);
        g.setOperandType(DataType.BYTEARRAY);
        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertTrue((Boolean)r.result);
    }

    @Test
    public void testDataByteArrayAndNullValues() throws Exception {
        checkNullValues(DataType.BYTEARRAY, new DataByteArray("b"));
    }

    public <U> void checkNullValues(byte operandType, U value) throws Exception {
        ConstantExpression lt = GenPhyOp.exprConst();
        ConstantExpression rt = GenPhyOp.exprConst();
        LTOrEqualToExpr g = GenPhyOp.compLTOrEqualToExpr();

        // test with null in lhs
        g.setOperandType(operandType);
        lt.setValue(null);
        rt.setValue(value);
        g.setLhs(lt);
        g.setRhs(rt);

        Result r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertNull((Boolean)r.result);

        // test with null in rhs
        g.setOperandType(operandType);
        lt.setValue(value);
        rt.setValue(null);
        g.setLhs(lt);
        g.setRhs(rt);

        r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertNull((Boolean)r.result);

        // test with null in lhs and rhs
        g.setOperandType(operandType);
        lt.setValue(null);
        rt.setValue(null);
        g.setLhs(lt);
        g.setRhs(rt);

        r = g.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, r.returnStatus);
        assertNull((Boolean)r.result);

    }
}