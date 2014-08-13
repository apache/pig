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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.FilterFunc;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.LogicalExpressionSimplifier;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.junit.Before;
import org.junit.Test;

public class TestFilterSimplification {

    LogicalPlan plan = null;
    PigContext pc = null;
    PigServer pigServer = null;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL, new Properties());
        pc = pigServer.getPigContext();
    }

    @Test
    public void test1() throws Exception {
        // case 1: simple and implication
        String query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3) AND (id > 5);" +
                       "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 5);" +
                "store b into 'empty';";
        LogicalPlan expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 2: simple or implication
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3) OR (id > 5);" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 3: constant expression eval
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3+4*2);" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 11);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 4: simple NOT
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by NOT(NOT(NOT(id > 3)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id <= 3);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 5: redundant NOT
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by NOT(NOT(id > 3));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 6: negative
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3) AND (v1 is null);" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3) AND (v1 is null);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 7: is not null
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by NOT(v1 is null);" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (v1 is not null);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 8: combo I
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by NOT((id > 1) OR ((v1 is null) AND (id > 5)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id <= 1);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 9: combo II: lhs <-> rhs
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by NOT(((id > 5) AND (v1 is null)) OR (id > 1));"+
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id <= 1);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 10: complementary OR
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((id < 1) OR (id >= 1));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = load 'd.txt' as (id:int, v1, v2);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 11: OR Equality elimination
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((id < 1) OR (id < 1));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id < 1);" +
        "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 12: AND Equality elimination
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((id < 1) AND (id < 1));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id < 1);" + "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 13: negative case
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((id < 1) AND (v1 is null));" + "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((id < 1) AND (v1 is NULL));" + "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 14: combo III
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by NOT((id > 1) OR ((v1 is null) AND (id > 1+2*2)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id <= 1);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 15: combo III: negative
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((id > 5) OR (v1 < 3)) AND ((id > 4) OR (v1 > 5)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((id > 5) OR (v1 < 3)) AND ((id > 4) OR (v1 > 5)));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 15: combo III: negative
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((id > 5) OR (v1 > 3)) AND ((id > 4) OR (v1 > 5)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((id > 5) OR (v1 > 3)) AND ((id > 4) OR (v1 > 5)));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 16: conflicting OR
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((id < 1) OR (id > 1));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((id < 1) OR (id > 1));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 17: conflicting AND: negtive case for now
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((id < 1) AND (id > 1));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((id < 1) AND (id > 1));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 18: combo IV: negative
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((id > 5) OR (v1 > 3)) AND ((id < 8) OR (v1 > 5)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((id > 5) OR (v1 > 3)) AND ((id < 8) OR (v1 > 5)));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 19: negative AND
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3) AND (id < 5);" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3) AND (id < 5);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 20: negative OR
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3) OR (id < 5);" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3) OR (id < 5);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 20: combo V: negative
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((v1 > 3) OR (id > 5)) AND ((id < 8) OR (v1 > 5)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((v1 > 3) OR (id > 5)) AND ((id < 8) OR (v1 > 5)));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 22: combo V: negative
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((v1 > 3) OR (id > 5)) AND ((v1 > 5) OR (id < 8)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((v1 > 3) OR (id > 5)) AND ((v1 > 5) OR (id < 8)));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 23: combo VI: extremely degenerate
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by ((((id > 1) OR (id > 2)) AND ((id > 3) OR (id > 4))) AND (((id > 5) OR (id > 6)) AND ((id > 7) OR (id > 8))));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (id > 7);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 24: combo VII: extremely degenerate
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by ((((id > 1) OR (id > 2)) AND ((id > 3) OR (id > 4))) AND (id > 7));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (id > 7);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 25: combo VII: extremely degenerate
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by ((((id > 1) OR (id > 2)) AND ((id > 3) OR (id > 4))) AND (((id > 5) AND (id > 7))));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (id > 7);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 26: combo VIII: lhs<->rhs for case 25
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by ((((id > 7) AND (id > 5))) AND (((id > 4) OR (id > 3)) AND ((id > 2) OR (id > 1))));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (id > 7);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 27: combo VII: rhs<->lhs for case 24
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by ((id > 7) AND (((id > 4) OR (id > 3)) AND ((id > 2) OR (id > 1))));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (id > 7);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 28: complex equality
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((id > 4) OR (id > 3)) AND ((id > 3) OR (id > 4)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (id > 3);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 29: complex equality
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((id > 4) OR (v1 > 3)) AND ((v1 > 3) OR (id > 4)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by ((id > 4) OR (v1 > 3));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 30: complex equality
        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by (((id > 4) OR (v1 > 3)) OR ((v1 > 3) OR (id > 4)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1:int, v2)) by ((id > 4) OR (v1 > 3));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // case 31: See PIG-2067
        query = "A = load 'a.dat' as (cookie);" +
                "B = load 'b.dat' as (cookie);" +
                "C = cogroup A by cookie, B by cookie;" +
                "E = filter C by COUNT(B)>0 AND COUNT(A)>0;" +
                "store E into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        // Make sure in this case, we don't optimize
        query = "A = load 'a.dat' as (cookie);" +
            "B = load 'b.dat' as (cookie);" +
            "C = cogroup A by cookie, B by cookie;" +
            "E = filter C by COUNT(B)>0 AND COUNT(A)>0;" +
            "store E into 'empty';";

        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

    }

    @Test
    public void test2() throws Exception {
        String query = "b = filter (load 'd.txt' as (name, age, gpa)) by age >= 50 or name > 'fred' and gpa <= 3.0 or name >= 'bob';" +
                       "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (name, age, gpa)) by age >= 50 or name >= 'bob';" +
                "store b into 'empty';";
        LogicalPlan expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // Regex filtering
        query = "b = filter (load 'd.txt' as (name:chararray, age:int, registration, contributions:double)) by (name matches '^fred.*' and (chararray)registration matches '^dem.*');" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (name:chararray, age:int, registration, contributions:double)) by (name matches '^fred.*' and (chararray)registration matches '^dem.*');" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // NOT Regex filtering
        query = "b = filter (load 'd.txt') by (not $0 matches '^fred.*');" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt') by (not $0 matches '^fred.*');" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // naiive filtering
        query = "b = filter (load 'd.txt') by 1==1;" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = load 'd.txt';" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));
    }

    @Test
    public void test3() throws Exception {
        // boolean constant elimination: AND
        String query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((v1 is not null) AND (id == 1) AND (1 == 1));" +
                       "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((v1 is not null) AND (id == 1));" +
                "store b into 'empty';";
        LogicalPlan expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // boolean constant elimination: OR
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by (((v1 is not null) AND (id == 1)) OR (1 == 0));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((v1 is not null) AND (id == 1));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // the mirror case of the above
        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((1 == 0) OR ((v1 is not null) AND (id == 1)));" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (id:int, v1, v2)) by ((v1 is not null) AND (id == 1));" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));
    }

    @Test
    public void test4() throws Exception {
        String query = "b = filter (load 'd.txt' as (a:chararray, b:long, c:map[], d:chararray, e:chararray)) by a == 'v' and b == 117L and c#'p1' == 'h' and c#'p2' == 'to' and ((d is not null and d != '') or (e is not null and e != ''));" +
                       "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (a:chararray, b:long, c:map[], d:chararray, e:chararray)) by a == 'v' and b == 117L and c#'p1' == 'h' and c#'p2' == 'to' and ((d is not null and d != '') or (e is not null and e != ''));" +
                "store b into 'empty';";
        LogicalPlan expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // mirror of the above
        query = "b = filter (load 'd.txt' as (a:chararray, b:long, c:map[], d:chararray, e:chararray)) by ((d is not null and d != '') or (e is not null and e != '')) and a == 'v' and b == 117L and c#'p1' == 'h' and c#'p2' == 'to';" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (a:chararray, b:long, c:map[], d:chararray, e:chararray)) by ((d is not null and d != '') or (e is not null and e != '')) and a == 'v' and b == 117L and c#'p1' == 'h' and c#'p2' == 'to';" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

    }

    @Test
    public void test5() throws Exception {
        // 2-level combo: 8 possibilities
        boolean[] booleans = {false, true};
        for (boolean b1 : booleans)
        for (boolean b2 : booleans)
        for (boolean b3 : booleans)
            comboRunner2(b1, b2, b3);
    }

    private void comboRunner2(boolean b1, boolean b2, boolean b3) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("b = filter (load 'd.txt' as (a:int, b:int, c:int, d:int)) by (((a < 1) " + (b1 ? "and" : "or") + " (b < 2)) " + (b2 ? "and" : "or") + " ((c < 3) " + (b3 ? "and" : "or") + " (d < 4)));");
        String query = sb.toString() + "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        LogicalPlan expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));
    }

    @Test
    public void test6() throws Exception {
        // 3-level combo: 128 possibilities
        boolean[] booleans = {false, true};
        for (boolean b1 : booleans)
        for (boolean b2 : booleans)
        for (boolean b3 : booleans)
        for (boolean b4 : booleans)
        for (boolean b5 : booleans)
        for (boolean b6 : booleans)
        for (boolean b7 : booleans)
            comboRunner3(b1, b2, b3, b4, b5, b6, b7);
    }

    private void comboRunner3(boolean b1, boolean b2, boolean b3, boolean b4, boolean b5, boolean b6, boolean b7) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("b = filter (load 'd.txt' as (a:int, b:int, c:int, d:int, e:int, f:int, g:int, h:int)) by ((((a < 1) " + (b1 ? "and" : "or") + " (b < 2)) " + (b2 ? "and" : "or") + " ((c < 3) " + (b3 ? "and" : "or") + " (d < 4))) " + (b4 ? "and" : "or") + " (((e < 5) " + (b5 ? "and" : "or") + " (f < 6)) " + (b6 ? "and" : "or") + " ((g < 7) " + (b7 ? "and" : "or") + " (h < 8))));");
        String query = sb.toString() + "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        LogicalPlan expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));
    }

    @Test
    public void test7() throws Exception {
        String query = "b = filter (load 'd.txt' as (k1, k2, k3, v1, v2, v3)) by k2#'f1'#'f' is not null and (v2#'f'#'f1' is not null or v2#'f'#'f2' is not null);" +
                       "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (k1, k2, k3, v1, v2, v3)) by k2#'f1'#'f' is not null and (v2#'f'#'f1' is not null or v2#'f'#'f2' is not null);" +
                "store b into 'empty';";
        LogicalPlan expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));

        query = "b = filter (load 'd.txt' as (k1, k2, k3, v1, v2, v3)) by k2#'f1'#'f' is not null and (v2#'f1'#'f' is not null or v2#'f2'#'f' is not null);" +
                "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (k1, k2, k3, v1, v2, v3)) by k2#'f1'#'f' is not null and (v2#'f1'#'f' is not null or v2#'f2'#'f' is not null);" +
                "store b into 'empty';";
        expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));
    }

    @Test
    // PIG-1820
    public void test8() throws Exception {
        String query = "b = filter (load 'd.txt' as (a0, a1)) by (a0 is not null or a1 is not null) and IsEmpty(a0);" +
                       "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        query = "b = filter (load 'd.txt' as (a0, a1)) by (a0 is not null or a1 is not null) and IsEmpty(a0);" +
                "store b into 'empty';";
        LogicalPlan expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));
    }


    @Test
    // PIG-2137
    public void testSimiplificationNonDeterministicUdf() throws Exception {
        String query = "b = filter (load 'd.txt' as (a0, a1)) by RANDOM() > 0.1 and RANDOM() > 0.1;" +
                       "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();
        LOFilter optimizedFilt = (LOFilter) newLogicalPlan.getSuccessors(newLogicalPlan.getSources().get(0)).get(0);

        //expected query is same as original query, optimizer should not combine
        // conditions involving non deterministic udfs
        query = "b = filter (load 'd.txt' as (a0, a1)) by RANDOM() > 0.1 and RANDOM() > 0.1;" +
            "store b into 'empty';";
        LogicalPlan expected = Util.buildLp(pigServer, query);
        LOFilter expectedFilt = (LOFilter) expected.getSuccessors(expected.getSources().get(0)).get(0);
        assertEquals(
                "size of filter expression plans",
                optimizedFilt.getFilterPlan().size(),
                expectedFilt.getFilterPlan().size()
        );

    }

    static public class MyFilterFunc extends FilterFunc {

        @Override
        public Boolean exec(Tuple input) {
            return true;
        }
    }

    //PIG-2144
    @Test
    public void testNotConversionUdfArg() throws Exception{
        //udf arg should not be changed
        String query = "b = filter (load 'd.txt' as (a0, a1)) by " +
        " NOT IsEmpty( " +  MyFilterFunc.class.getName() + "(a0,a1));" +
        "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        //expected plan is same as original plan
        LogicalPlan expected = Util.buildLp(pigServer, query);

        assertTrue(expected.isEqual(newLogicalPlan));
    }

    // PIG-2316
    @Test
    public void testEqualNotEqualWithSameValue() throws Exception {
        String query = "b = filter (load 'd.txt' as (a0:int, a1:int)) "
                       + "by ((a0 == 1) or (a0 != 1));"
                       + "store b into 'empty';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        //expected plan is same as original plan
        LogicalPlan expected = Util.buildLp(pigServer, query);
        assertTrue(expected.isEqual(newLogicalPlan));

        //swapping == and !=
        query = "b = filter (load 'd.txt' as (a0:int, a1:int)) "
                       + "by ((a0 != 1) or (a0 == 1));"
                       + "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);
        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        //expected plan is same as original plan
        expected = Util.buildLp(pigServer, query);
        assertTrue(expected.isEqual(newLogicalPlan));

        //more realistic test case which created incorrect output
        query = "b = filter (load 'd.txt' as (a0:int, a1:int)) "
                       + "by ((a0 == 1 and a1 == 3) or (a0 != 1));"
                       + "store b into 'empty';";
        newLogicalPlan = Util.buildLp(pigServer, query);
        optimizer = new MyPlanOptimizer(newLogicalPlan, 10);
        optimizer.optimize();

        //expected plan is same as original plan
        expected = Util.buildLp(pigServer, query);
        assertTrue(expected.isEqual(newLogicalPlan));
    }

    public class MyPlanOptimizer extends LogicalPlanOptimizer {

        protected MyPlanOptimizer(OperatorPlan p, int iterations) {
            super(p, iterations, null);
        }

        protected List<Set<Rule>> buildRuleSets() {
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();

            Rule r = new LogicalExpressionSimplifier("LogicalPlanSimplifier");
            Set<Rule> s = new HashSet<Rule>();
            s.add(r);
            ls.add(s);

            return ls;
        }
    }
}