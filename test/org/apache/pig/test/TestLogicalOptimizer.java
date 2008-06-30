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

import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.optimizer.*;
import org.apache.pig.test.utils.LogicalPlanTester;

import org.junit.Test;
import org.junit.Before;

/**
 * Test the logical optimizer.
 */

public class TestLogicalOptimizer extends junit.framework.TestCase {

    final String FILE_BASE_LOCATION = "test/org/apache/pig/test/data/DotFiles/" ;

    LogicalPlanTester planTester = new LogicalPlanTester() ;

    @Before
    public void setUp() {
        planTester.reset();
    }

    @Test
    public void testTypeCastInsertion() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (p:int, q:long, r:float, "
            + "s:double, t:map [], u:tuple (x:int, y:int), " + 
            "v:bag {x:tuple(z:int)});");
        LogicalPlan plan = planTester.buildPlan("B = order A by p;");
        planTester.typeCheckAgainstDotFile(plan, FILE_BASE_LOCATION +
            "optplan1.dot", true);
    }
}

