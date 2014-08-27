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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.FilterConstantCalculator;
import org.apache.pig.newplan.logical.rules.ForEachConstantCalculator;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.junit.Before;
import org.junit.Test;

public class TestConstantCalculator {
    PigContext pc = null;
    PigServer pigServer = null;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL, new Properties());
        pc = pigServer.getPigContext();
        SchemaTupleBackend.initialize(ConfigurationUtil.toConfiguration(pc.getProperties(), true),
                pc);
    }
    @Test
    public void test() throws Exception {
        // pure simple constant
        assertQuerySame("b = filter (load 'd.txt' as (id:int, v1, v2)) by id > 3+5;" +
                       "store b into 'empty';",
                       "b = filter (load 'd.txt' as (id:int, v1, v2)) by id > 8;" +
                       "store b into 'empty';");

        // simple constant mixed with column
        assertQuerySame("b = foreach (load 'd.txt' as (id:int, v1, v2)) generate id * (3 * 5);" +
                "store b into 'empty';",
                "b = foreach (load 'd.txt' as (id:int, v1, v2)) generate id * 15;" +
                "store b into 'empty';");

        // boolean constant calculation
        assertQuerySame("b = filter (load 'd.txt' as (id:int, v1, v2)) by 5 + 1 > 3 and -1 < 4;" +
                "store b into 'empty';",
                "b = filter (load 'd.txt' as (id:int, v1, v2)) by true;" +
                "store b into 'empty';");

        // simple cast
        assertQuerySame("b = foreach (load 'd.txt' as (d:double)) generate (int)5.0;" +
                "store b into 'empty';",
                "b = foreach (load 'd.txt' as (d:double)) generate 5;" +
                "store b into 'empty';");

        // more calculation
        assertQuerySame("b = foreach (load 'd.txt' as (d:double)) generate ((int)15.89 + 5) * 5 / 25 % 3;" +
                "store b into 'empty';",
                "b = foreach (load 'd.txt' as (d:double)) generate 1;" +
                "store b into 'empty';");

        // udf calculation
        assertQuerySame("b = foreach (load 'd.txt' as (v1)) generate UPPER(CONCAT('a', CONCAT('b', 'c'), 'd'));" +
                "store b into 'empty';",
                "b = foreach (load 'd.txt' as (v1)) generate 'ABCD';" +
                "store b into 'empty';");

        // udf not eligible for optimize
        assertQuerySame("b = foreach (load 'd.txt' as (v1)) generate " + NoCalc.class.getName() + "('2');" +
                "store b into 'empty';",
                "b = foreach (load 'd.txt' as (v1)) generate " + NoCalc.class.getName() + "('2');" +
                "store b into 'empty';");

        // solo udf
        assertQuerySame("b = foreach (load 'd.txt' as (v1)) generate " + StringSpecified.class.getName() + "();" +
                "store b into 'empty';",
                "b = foreach (load 'd.txt' as (v1)) generate '1';" +
                "store b into 'empty';");
    }

    public static class NoCalc extends EvalFunc<String> {
        @Override
        public String exec(Tuple input) throws IOException {
            return "1";
        }
    }

    public static class StringSpecified extends EvalFunc<String> {
        @Override
        public String exec(Tuple input) throws IOException {
            return "1";
        }
        @Override
        public boolean allowCompileTimeCalculation() {
            return true;
        }
    }

    private void assertQuerySame(String origQuery, String optimizedQuery) throws Exception {
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, origQuery);

        SchemaTupleBackend.initialize(ConfigurationUtil.toConfiguration(pc.getProperties(), true),
                pc);
        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        LogicalPlan expected = Util.buildLp(pigServer, optimizedQuery);

        assertTrue(expected.isEqual(newLogicalPlan));

    }

    public class MyPlanOptimizer extends LogicalPlanOptimizer {

        protected MyPlanOptimizer(OperatorPlan p, int iterations) {
            super(p, iterations, null);
        }

        protected List<Set<Rule>> buildRuleSets() {
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();

            // Logical expression simplifier
            Set <Rule> s = new HashSet<Rule>();
            // add constant calculator rule
            Rule r = new FilterConstantCalculator("FilterConstantCalculator", pc);
            s.add(r);
            r = new ForEachConstantCalculator("ForEachConstantCalculator", pc);
            s.add(r);
            ls.add(s);

            return ls;
        }
    }
}
