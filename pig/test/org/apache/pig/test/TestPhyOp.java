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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.Before;
import org.junit.Test;

public class TestPhyOp {
    PhysicalOperator op;

    PhysicalOperator inpOp;

    Tuple t;

    @Before
    public void setUp() throws Exception {
        op = GenPhyOp.topFilterOp();
        inpOp = GenPhyOp.topFilterOpWithExPlan(25, 10);
        t = GenRandomData.genRandSmallBagTuple(new Random(), 10, 100);
    }

    @Test
    public void testProcessInput() throws ExecException {
        // Stand-alone tests
        Result res = op.processInput();
        assertEquals(POStatus.STATUS_EOP, res.returnStatus);
        op.attachInput(t);
        res = op.processInput();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertEquals(t, res.result);
        op.detachInput();
        res = op.processInput();
        assertEquals(POStatus.STATUS_EOP, res.returnStatus);

        // With input operator
        List<PhysicalOperator> inp = new ArrayList<PhysicalOperator>();
        inp.add(inpOp);
        op.setInputs(inp);
        op.processInput();
        assertEquals(POStatus.STATUS_EOP, res.returnStatus);

        inpOp.attachInput(t);
        res = op.processInput();
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertEquals(t, res.result);
        inpOp.detachInput();
        res = op.processInput();
        assertEquals(POStatus.STATUS_EOP, res.returnStatus);
    }
}
