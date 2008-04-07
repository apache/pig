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
import java.util.Iterator;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.POProject;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestProject {
    Random r;

    Tuple t;

    Result res;

    POProject proj;

    @Before
    public void setUp() throws Exception {
        r = new Random();
        t = GenRandomData.genRandSmallBagTuple(r, 10, 100);
        res = new Result();
        proj = GenPhyOp.exprProject();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetNext() throws ExecException, IOException {
        proj.attachInput(t);
        for (int j = 0; j < t.size(); j++) {
            proj.attachInput(t);
            proj.setColumn(j);

            res = proj.getNext();
            assertEquals(POStatus.STATUS_OK, res.returnStatus);
            assertEquals(t.get(j), res.result);
        }
    }

    @Test
    public void testGetNextTuple() throws IOException, ExecException {
        proj.attachInput(t);
        proj.setColumn(0);
        proj.setOverloaded(true);
        DataBag inpBag = (DataBag) t.get(0);
        int cntr = 0;
        boolean contains = true;
        while (true) {
            res = proj.getNext(t);
            if (res.returnStatus == POStatus.STATUS_EOP)
                break;
            if (!TestHelper.bagContains(inpBag, (Tuple) res.result)) {
                contains = false;
                break;
            }
            ++cntr;
        }
        assertEquals((float) (inpBag).size(), (float) cntr, 0.01f);
        assertEquals(true, contains);

        proj.attachInput(t);
        proj.setColumn(9);
        proj.setOverloaded(false);
        res = proj.getNext(t);
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertEquals(t.get(9), res.result);
    }

}
