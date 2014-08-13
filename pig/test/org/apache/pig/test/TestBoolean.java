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

import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.BinaryExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POAnd;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONot;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POOr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.UnaryExpressionOperator;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Before;
import org.junit.Test;

public class TestBoolean {
    Random r = new Random(42L);
    ConstantExpression lt, rt;
    BinaryExpressionOperator bop;
    UnaryExpressionOperator uop;

    @Before
    public void setUp() throws Exception {
        lt = new ConstantExpression(new OperatorKey("", r.nextLong()));
        lt.setResultType(DataType.BOOLEAN);
        rt = new ConstantExpression(new OperatorKey("", r.nextLong()));
        rt.setResultType(DataType.BOOLEAN);
    }

    private void setupAnd() {
        bop = new POAnd(new OperatorKey("", r.nextLong()));
        bop.setLhs(lt);
        bop.setRhs(rt);
    }

    private void setupOr() {
        bop = new POOr(new OperatorKey("", r.nextLong()));
        bop.setLhs(lt);
        bop.setRhs(rt);
    }

    private void setupNot() {
        uop = new PONot(new OperatorKey("", r.nextLong()));
        uop.setExpr(lt);
    }

    @Test
    public void testAndNull() throws ExecException {
        setupAnd();
        Boolean[] testWith = new Boolean[] { false, true, null };

        // truth table for AND
        // t = true, n = null, f = false
        // AND t n f
        // t t n f
        // n n n f
        // f f f f

        // test with first operand set to null
        for (int i = 0; i < testWith.length; i++) {
            lt.setValue(null);
            rt.setValue(testWith[i]);
            Result res = bop.getNextBoolean();
            assertEquals(POStatus.STATUS_OK, res.returnStatus);
            if (testWith[i] != null && testWith[i] == false) {
                // if rhs is false, result is false
                assertFalse((Boolean) res.result);
            } else {
                // else result is null
                assertNull(res.result);
            }
        }

        // test with second operand set to null
        for (int i = 0; i < testWith.length; i++) {
            lt.setValue(testWith[i]);
            rt.setValue(null);
            Result res = bop.getNextBoolean();
            assertEquals(POStatus.STATUS_OK, res.returnStatus);
            if (testWith[i] != null && testWith[i] == false) {
                // if lhs is false, result is false
                assertFalse((Boolean) res.result);
            } else {
                // else result is null
                assertNull(res.result);
            }
        }
    }

    @Test
    public void testOrNull() throws ExecException {
        setupOr();
        Boolean[] testWith = new Boolean[] { false, true, null };
        // truth table for OR
        // t = true, n = null, f = false
        // OR t n f
        // t t t t
        // n t n n
        // f t n f

        // test with first operand set to null
        for (int i = 0; i < testWith.length; i++) {
            lt.setValue(null);
            rt.setValue(testWith[i]);
            Result res = bop.getNextBoolean();
            assertEquals(POStatus.STATUS_OK, res.returnStatus);
            if (testWith[i] != null && testWith[i] == true) {
                // if rhs is true, result is true
                assertTrue((Boolean) res.result);
            } else {
                // else result is null
                assertNull(res.result);
            }
        }

        // test with second operand set to null
        for (int i = 0; i < testWith.length; i++) {
            lt.setValue(testWith[i]);
            rt.setValue(null);
            Result res = bop.getNextBoolean();
            assertEquals(POStatus.STATUS_OK, res.returnStatus);
            if (testWith[i] != null && testWith[i] == true) {
                // if lhs is true, result is true
                assertTrue((Boolean) res.result);
            } else {
                // else result is null
                assertNull(res.result);
            }
        }
    }

    @Test
    public void testAndFirstFalse() throws ExecException {
        setupAnd();
        lt.setValue(new Boolean(false));
        rt.setValue(new Boolean(true));
        Result res = bop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertFalse((Boolean) res.result);
    }

    @Test
    public void testAndSecondFalse() throws ExecException {
        setupAnd();
        lt.setValue(new Boolean(true));
        rt.setValue(new Boolean(false));
        Result res = bop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertFalse((Boolean) res.result);
    }

    @Test
    public void testAndBothFalse() throws ExecException {
        setupAnd();
        lt.setValue(new Boolean(false));
        rt.setValue(new Boolean(false));
        Result res = bop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertFalse((Boolean) res.result);
    }

    @Test
    public void testAndTrue() throws ExecException {
        setupAnd();
        lt.setValue(new Boolean(true));
        rt.setValue(new Boolean(true));
        Result res = bop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertTrue((Boolean) res.result);
    }

    @Test
    public void testOrFirstFalse() throws ExecException {
        setupOr();
        lt.setValue(new Boolean(false));
        rt.setValue(new Boolean(true));
        Result res = bop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertTrue((Boolean) res.result);
    }

    @Test
    public void testOrSecondFalse() throws ExecException {
        setupOr();
        lt.setValue(new Boolean(true));
        rt.setValue(new Boolean(false));
        Result res = bop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertTrue((Boolean) res.result);
    }

    @Test
    public void testOrBothFalse() throws ExecException {
        setupOr();
        lt.setValue(new Boolean(false));
        rt.setValue(new Boolean(false));
        Result res = bop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertFalse((Boolean) res.result);
    }

    @Test
    public void testOrTrue() throws ExecException {
        setupOr();
        lt.setValue(new Boolean(true));
        rt.setValue(new Boolean(true));
        Result res = bop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertTrue((Boolean) res.result);
    }

    @Test
    public void testNotTrue() throws ExecException {
        setupNot();
        lt.setValue(new Boolean(true));
        Result res = uop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertFalse((Boolean) res.result);
    }

    @Test
    public void testNotFalse() throws ExecException {
        setupNot();
        lt.setValue(new Boolean(false));
        Result res = uop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertTrue((Boolean) res.result);
    }

    @Test
    public void testNotNull() throws ExecException {
        setupNot();
        lt.setValue(null);
        Result res = uop.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertNull(res.result);
    }
}