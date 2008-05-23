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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.impl.physicalLayer.expressionOperators.POProject;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFilter extends junit.framework.TestCase {
    POFilter pass;

    POFilter fail;

    Tuple t;

    DataBag inp;

    POFilter projFil;

    @Before
    public void setUp() throws Exception {
        Random r = new Random();
        pass = GenPhyOp.topFilterOpWithExPlan(50, 25);
        fail = GenPhyOp.topFilterOpWithExPlan(25, 50);
        inp = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
        t = GenRandomData.genRandSmallBagTuple(r, 10, 100);
        projFil = GenPhyOp.topFilterOpWithProj(1, 50);
        POProject inpPrj = GenPhyOp.exprProject();
        Tuple tmpTpl = new DefaultTuple();
        tmpTpl.append(inp);
        inpPrj.setColumn(0);
        inpPrj.setResultType(DataType.TUPLE);
        inpPrj.setOverloaded(true);
        List<PhysicalOperator> inputs = new ArrayList<PhysicalOperator>();
        inputs.add(inpPrj);
        projFil.setInputs(inputs);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetNextTuple() throws ExecException, IOException {
        pass.attachInput(t);
        Result res = pass.getNext(t);
        assertEquals(t, res.result);
        fail.attachInput(t);
        res = fail.getNext(t);
        assertEquals(res.returnStatus, POStatus.STATUS_EOP);

        while (true) {
            res = projFil.getNext(t);
            if (res.returnStatus == POStatus.STATUS_EOP)
                break;
            assertEquals(POStatus.STATUS_OK, res.returnStatus);
            Tuple output = (Tuple) res.result;
            assertEquals(true, TestHelper.bagContains(inp, output));
            assertEquals(true, (Integer) ((Tuple) res.result).get(1) > 50);
        }
    }

}
