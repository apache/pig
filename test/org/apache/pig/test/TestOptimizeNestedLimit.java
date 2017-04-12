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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.NestedLimitOptimizer;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOptimizeNestedLimit {

    LogicalPlan lp;
    PhysicalPlan php;
    MROperPlan mrp;

    @Before
    public void setup() throws Exception {

        PigContext pc = new PigContext( ExecType.LOCAL, new Properties());
        PigServer pigServer = new PigServer(pc);
        String query = "A = load 'myfile';" +
                "B = group A by $0;" +
                "C = foreach B {" +
                "C1 = order A by $1;" +
                "C2 = limit C1 5;" +
                "generate C2;}" +
                "store C into 'empty';";
        lp = optimizePlan(Util.buildLp(pigServer, query));
        php = Util.buildPp(pigServer, query);
        mrp = Util.buildMRPlanWithOptimizer(php, pc);
    }

    @After
    public void tearDown() {

    }

    @Test
    // Test logical plan
    public void testLogicalPlan() throws Exception {

        Iterator<Operator> it = lp.getOperators();

        LOForEach forEach = null;
        while(it.hasNext()) {
            Operator op = it.next();
            if (op instanceof LOForEach) {
                assertNull("There should be only one LOForEach", forEach);
                forEach = (LOForEach) op;
            }
        }
        assertNotNull("ForEach is missing", forEach);

        it = forEach.getInnerPlan().getOperators();
        LOSort sort = null;
        while(it.hasNext()) {
            Operator op = it.next();
            if (op instanceof LOLimit) {
                fail("There should be no LOLimit");
            } else if (op instanceof LOSort) {
                assertNull("There should be only one LOSort", sort);
                sort = (LOSort) op;
            }
        }
        assertNotNull("LOSort is missing", sort);
        assertEquals(sort.getLimit(), 5);
    }

    @Test
    // test physical plan
    public void testPhysicalPlan() throws Exception {

        Iterator<PhysicalOperator> it = php.iterator();

        POForEach forEach = null;
        while (it.hasNext()) {
            PhysicalOperator op = it.next();
            if (op instanceof POForEach) {
                assertNull("There should be only 1 POForEach", forEach);
                forEach = (POForEach) op;
            }
        }
        assertNotNull("POForEach is missing", forEach);

        List<PhysicalPlan> inps = forEach.getInputPlans();
        assertEquals(inps.size(), 1);

        it = inps.get(0).iterator();
        POSort sort = null;
        while(it.hasNext()) {
            PhysicalOperator op = it.next();
            if (op instanceof POLimit) {
                fail("There should be no POLimit");
            } else if (op instanceof POSort) {
                assertNull("There should be only 1 POSort", sort);
                sort = (POSort) op;
            }
        }
        assertNotNull("POSort is missing", sort);
        assertEquals(sort.getLimit(), 5);
    }

    @Test
    // test MR plan
    public void testMRPlan() throws Exception {

        List<MapReduceOper> ops = mrp.getLeaves();
        assertEquals(ops.size(), 1);

        PhysicalPlan plan = ops.get(0).reducePlan;
        Iterator<PhysicalOperator> it = plan.iterator();

        POForEach forEach = null;
        while(it.hasNext()) {
            PhysicalOperator op = it.next();
            if (op instanceof POForEach) {
                assertNull("There should be only 1 POForEach", forEach);
                forEach = (POForEach) op;
            }
        }
        assertNotNull("POForEach is missing", forEach);

        List<PhysicalPlan> inps = forEach.getInputPlans();
        assertEquals(inps.size(), 1);

        it = inps.get(0).iterator();
        POSort sort = null;
        while(it.hasNext()) {
            PhysicalOperator op = it.next();
            if (op instanceof POLimit) {
                fail("There should be no POLimit");
            } else if (op instanceof POSort) {
                assertNull("There should be only 1 POSort", sort);
                sort = (POSort) op;
            }
        }
        assertNotNull("POSort is missing", sort);
        assertEquals(sort.getLimit(), 5);
    }

    public class MyPlanOptimizer extends LogicalPlanOptimizer {
        protected MyPlanOptimizer(OperatorPlan p,  int iterations) {
            super( p, iterations, new HashSet<String>() );
        }

        @Override
        protected List<Set<Rule>> buildRuleSets() {
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();

            Set<Rule> s = null;
            Rule r = null;

            s = new HashSet<Rule>();
            r = new NestedLimitOptimizer("OptimizeNestedLimit");
            s.add(r);
            ls.add(s);

            return ls;
        }
    }

    private LogicalPlan optimizePlan(LogicalPlan plan) throws IOException {
        PlanOptimizer optimizer = new MyPlanOptimizer( plan, 3 );
        optimizer.optimize();
        return plan;
    }
}
