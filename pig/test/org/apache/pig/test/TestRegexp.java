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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.BinaryComparisonOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORegexp;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Before;
import org.junit.Test;

public class TestRegexp {

    Random r = new Random();
    ConstantExpression lt, rt;
    BinaryComparisonOperator op;

    @Before
    public void setUp() throws Exception {
        lt = new ConstantExpression(new OperatorKey("", r.nextLong()));
        lt.setResultType(DataType.CHARARRAY);
        rt = new ConstantExpression(new OperatorKey("", r.nextLong()));
        rt.setResultType(DataType.CHARARRAY);
        op = new PORegexp(new OperatorKey("", r.nextLong()));
        op.setLhs(lt);
        op.setRhs(rt);
        op.setOperandType(DataType.CHARARRAY);
    }

    @Test
    public void testMatches() throws ExecException {
        lt.setValue(new String(
                "The quick sly fox jumped over the lazy brown dog"));
        rt.setValue(".*s.y.*");
        Result res = op.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertTrue((Boolean)res.result);

        // test with null in lhs
        lt.setValue(null);
        rt.setValue(".*s.y.*");
        res = op.getNextBoolean();
        assertNull(res.result);

        // test with null in rhs
        lt.setValue(new String(
                "The quick sly fox jumped over the lazy brown dog"));
        rt.setValue(null);
        res = op.getNextBoolean();
        assertNull(res.result);
    }

    @Test
    public void testDoesntMatch() throws ExecException {
        lt.setValue(new String(
                "The quick sly fox jumped over the lazy brown dog"));
        rt.setValue(new String("zzz"));
        Result res = op.getNextBoolean();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertFalse((Boolean)res.result);
    }
}
