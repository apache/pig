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

import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFilter {
    POFilter pass;
    POFilter fail;
    Tuple t;
    
    @Before
    public void setUp() throws Exception {
        pass = GenPhyOp.topFilterOpWithExPlan(50, 25);
        fail = GenPhyOp.topFilterOpWithExPlan(25, 50);
        
        t = GenRandomData.genRandSmallBagTuple(new Random(), 10, 100);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetNextTuple() throws ExecException {
        pass.attachInput(t);
        Result res = pass.getNext(t);
        assertEquals(t, res.result);
        fail.attachInput(t);
        res = fail.getNext(t);
        assertEquals(res.returnStatus, POStatus.STATUS_EOP);
    }

}
