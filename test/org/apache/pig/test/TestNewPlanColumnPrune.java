/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License" + you may not use this file except in compliance
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.rules.AddForEach;
import org.apache.pig.newplan.logical.rules.ColumnMapKeyPrune;
import org.apache.pig.newplan.logical.rules.MapKeysPruneHelper;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.junit.Test;

public class TestNewPlanColumnPrune {
    LogicalPlan plan = null;
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());

    private LogicalPlan buildPlan(String query) throws Exception{
        PigServer pigServer = new PigServer( pc );
        return Util.buildLp(pigServer, query);
    }

    @Test
    public void testNoPrune() throws Exception  {
        // no foreach
        String query = "a = load 'd.txt' as (id, v1, v2);" +
        "b = filter a by v1==NULL;" +
        "store b into 'empty';";
        LogicalPlan newLogicalPlan = buildPlan(query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query  = "a = load 'd.txt' as (id, v1, v2);" +
        "b = filter a by v1==NULL;"  +
        "store b into 'empty';";
        LogicalPlan expected = buildPlan(query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // no schema
        query = "a = load 'd.txt';" +
        "b = foreach a generate $0, $1;" +
        "store b into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a = load 'd.txt';"+
        "b = foreach a generate $0, $1;"+
        "store b into 'empty';";
        expected = buildPlan(query);
        assertTrue(expected.isEqual(newLogicalPlan));
    }

    @Test
    public void testPrune() throws Exception  {
        // only foreach
        String query = "a = load 'd.txt' as (id, v1, v2);" +
        "b = foreach a generate id;"+
        "store b into 'empty';";
        LogicalPlan newLogicalPlan = buildPlan(query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a = load 'd.txt' as (id);" +
        "b = foreach a generate id;"+
        "store b into 'empty';";
        LogicalPlan expected = buildPlan(query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // with filter
        query = "a = load 'd.txt' as (id, v1, v5, v3, v4, v2);"+
        "b = filter a by v1 != NULL AND (v2+v3)<100;"+
        "c = foreach b generate id;"+
        "store c into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a = load 'd.txt' as (id, v1, v3, v2);" +
        "b = filter a by v1 != NULL AND (v2+v3)<100;" +
        "c = foreach b generate id;" +
        "store c into 'empty';";
        expected = buildPlan(query);
        assertTrue(expected.isEqual(newLogicalPlan));

        // with 2 foreach
        query = "a = load 'd.txt' as (id, v1, v5, v3, v4, v2);" +
        "b = foreach a generate v2, v5, v4;" +
        "c = foreach b generate v5, v4;" +
        "store c into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a = load 'd.txt' as (v5, v4);" +
        "b = foreach a generate v5, v4;" +
        "c = foreach b generate v5, v4;" +
        "store c into 'empty';";
        expected = buildPlan(query);
        assertTrue(expected.isEqual(newLogicalPlan));

        // with 2 foreach
        query = "a = load 'd.txt' as (id, v1, v5, v3, v4, v2);" +
        "b = foreach a generate id, v1, v5, v3, v4;" +
        "c = foreach b generate v5, v4;" +
        "store c into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a = load 'd.txt' as (v5, v4);" +
        "b = foreach a generate v5, v4;" +
        "c = foreach b generate v5, v4;" +
        "store c into 'empty';";
        expected = buildPlan(query);
        assertTrue(expected.isEqual(newLogicalPlan));

        // with 2 foreach and filter in between
        query = "a =load 'd.txt' as (id, v1, v5, v3, v4, v2);" +
        "b = foreach a generate v2, v5, v4;" +
        "c = filter b by v2 != NULL;" +
        "d = foreach c generate v5, v4;" +
        "store d into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' as (v5, v4, v2);" +
        "b = foreach a generate v2, v5, v4;" +
        "c = filter b by v2 != NULL;" +
        "d = foreach c generate v5, v4;" +
        "store d into 'empty';";
        expected = buildPlan(query);
        assertTrue(expected.isEqual(newLogicalPlan));

        // with 2 foreach after join
        query = "a =load 'd.txt' as (id, v1, v2, v3);" +
        "b = load 'c.txt' as (id, v4, v5, v6);" +
        "c = join a by id, b by id;" +
        "d = foreach c generate a::id, v5, v3, v4;" +
        "store d into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' as (id, v3);" +
        "b = load 'c.txt' as (id, v4, v5);" +
        "c = join a by id, b by id;" +
        "d = foreach c generate a::id, v5, v3, v4;" +
        "store d into 'empty';";
        expected = buildPlan(query);
        assertTrue(expected.isEqual(newLogicalPlan));

        // with BinStorage, insert foreach after load
        query = "a =load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);" +
        "c = filter a by v2 != NULL;" +
        "d = foreach c generate v5, v4;" +
        "store d into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);" +
        "b = foreach a generate v5, v4, v2;" +
        "c = filter b by v2 != NULL;" +
        "d = foreach c generate v5, v4;" +
        "store d into 'empty';";
        expected = buildPlan(query);
        assertTrue(expected.isEqual(newLogicalPlan));

       // with BinStorage, not to insert foreach after load if there is already one
        query = "a =load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);" +
        "b = foreach a generate v5, v4, v2;" +
        "c = filter b by v2 != NULL;" +
        "d = foreach c generate v5;" +
        "store d into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);" +
        "b = foreach a generate v5, v2;" +
        "c = filter b by v2 != NULL;" +
        "d = foreach c generate v5;" +
        "store d into 'empty';";
        expected = buildPlan(query);
        assertTrue(expected.isEqual(newLogicalPlan));

       // with BinStorage, not to insert foreach after load if there is already one
        query = "a =load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);" +
        "b = foreach a generate v5, v4, v2, 10;" +
        "c = filter b by v2 != NULL;" +
        "d = foreach c generate v5;" +
        "store d into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);" +
        "b = foreach a generate v5, v2, 10;" +
        "c = filter b by v2 != NULL;" +
        "d = foreach c generate v5;" +
        "store d into 'empty';";
        expected = buildPlan(query);
        assertTrue(expected.isEqual(newLogicalPlan));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPruneWithMapKey() throws Exception {
         // only foreach
        String query = "a =load 'd.txt' as (id, v1, m:map[]);" +
        "b = foreach a generate id, m#'path';" +
        "store b into 'empty';";
        LogicalPlan newLogicalPlan = buildPlan(query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' as (id, m:map[]);" +
        "b = foreach a generate id, m#'path';" +
        "store b into 'empty';";
        LogicalPlan expected = buildPlan(query);

        assertTrue(expected.isEqual(newLogicalPlan));

        LOLoad op = (LOLoad)newLogicalPlan.getSources().get(0);
        Map<Integer,Set<String>> annotation =
                (Map<Integer, Set<String>>) op.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
        assertEquals(1, annotation.size());
        Set<String> s = new HashSet<String>();
        s.add("path");
        assertEquals(annotation.get(2), s);

        // foreach with join
        query = "a =load 'd.txt' as (id, v1, m:map[]);" +
        "b = load 'd.txt' as (id, v1, m:map[]);" +
        "c = join a by id, b by id;" +
        "d = filter c by a::m#'path' != NULL;" +
        "e = foreach d generate a::id, b::id, b::m#'path', a::m;" +
        "store e into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' as (id, m:map[]);" +
        "b = load 'd.txt' as (id, m:map[]);" +
        "c = join a by id, b by id;" +
        "d = filter c by a::m#'path' != NULL;" +
        "e = foreach d generate a::id, b::id, b::m#'path', a::m;" +
        "store e into 'empty';";
        expected = buildPlan(query);

        assertTrue(expected.isEqual(newLogicalPlan));

        List<Operator> ll = newLogicalPlan.getSources();
        assertEquals(2, ll.size());
        LOLoad loada = null;
        LOLoad loadb = null;
        for(Operator opp: ll) {
            if (((LogicalRelationalOperator)opp).getAlias().equals("a")) {
                loada = (LOLoad)opp;
                continue;
            }

            if (((LogicalRelationalOperator)opp).getAlias().equals("b")) {
                loadb = (LOLoad)opp;
                continue;
            }
        }

        annotation =
                (Map<Integer, Set<String>>) loada.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
        assertNull(annotation);

        annotation =
            (Map<Integer, Set<String>>) loadb.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
        assertEquals(1, annotation.size());

        s = new HashSet<String>();
        s.add("path");
        assertEquals(annotation.get(2), s);
    }

    @Test
    public void testPruneWithBag() throws Exception  {
        // filter above foreach
        String query = "a =load 'd.txt' as (id, v:bag{t:(s1,s2,s3)});" +
        "b = filter a by id>10;" +
        "c = foreach b generate id, FLATTEN(v);" +
        "d = foreach c generate id, v::s2;" +
        "store d into 'empty';";
        LogicalPlan newLogicalPlan = buildPlan(query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' as (id, v:bag{t:(s1,s2,s3)});" +
        "b = filter a by id>10;" +
        "c = foreach b generate id, FLATTEN(v);" +
        "d = foreach c generate id, v::s2;" +
        "store d into 'empty';";
        LogicalPlan expected = buildPlan(query);

        assertTrue(expected.isEqual(newLogicalPlan));
    }

    @Test
    public void testAddForeach() throws Exception  {
        // filter above foreach
        String query = "a =load 'd.txt' as (id, v1, v2);" +
        "b = filter a by v1>10;" +
        "c = foreach b generate id;" +
        "store c into 'empty';";
        LogicalPlan newLogicalPlan = buildPlan(query);

        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' as (id, v1);" +
        "b = filter a by v1>10;" +
        "c = foreach b generate id;" +
        "store c into 'empty';";
        LogicalPlan expected = buildPlan(query);

        assertTrue(expected.isEqual(newLogicalPlan));

        // join with foreach
        query = "a =load 'd.txt' as (id, v1, v2);" +
        "b = load 'd.txt' as (id, v1, v2);" +
        "c = join a by id, b by id;" +
        "d = filter c by a::v1>b::v1;" +
        "e = foreach d generate a::id;" +
        "store e into 'empty';";
        newLogicalPlan = buildPlan(query);

        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();

        query = "a =load 'd.txt' as (id, v1);" +
        "b = load 'd.txt' as (id, v1);" +
        "c = join a by id, b by id;" +
        "d = foreach c generate a::id, a::v1, b::v1;" +
        "e = filter d by a::v1>b::v1;" +
        "f = foreach e generate a::id;" +
        "store f into 'empty';";
        expected = buildPlan(query);

        assertTrue(expected.isEqual(newLogicalPlan));
    }

    @Test
    public void testPruneSubTreeForEach() throws Exception  {
        String query = "a =load 'd.txt' as (id, v1);" +
            "b = group a by id;" +
            "c = foreach b { d = a.v1; " +
                          " e = distinct d; " +
                          " generate group, e; };" +
            "f = foreach c generate group ;" +
            "store f into 'empty';";
        LogicalPlan newLogicalPlan = buildPlan(query);
        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        try {
            optimizer.optimize();
        } catch (Exception e) {
            //PIG-2968 throws ConcurrentModificationException
            e.printStackTrace();
            fail("Unexpected Exception: " + e);
        }
    }

    @Test
    public void testDistinct() throws Exception {
        //Test for bug where distinct wasn't being pruned properly causing union 
        //to fail to get a schema since the distinct relation had an incompatible schema
        //with the other relation being unioned.
        
        String testQuery = 
                "a = load 'd.txt' as (id, v1, v2);" +
                "b = load 'd.txt' as (id, v1, v2);" +
                "c = distinct a;" +
                "d = union b, c;" +
                "e = foreach d generate id, v1;" +
                "store e into 'empty';";
        
        //Generate optimized plan.
        LogicalPlan newLogicalPlan = buildPlan(testQuery);
        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        Iterator<Operator> iter = newLogicalPlan.getOperators();
        while (iter.hasNext()) {
            Operator o = iter.next();
            LogicalRelationalOperator lro = (LogicalRelationalOperator)o;
            if (lro == null || lro.getAlias() == null) continue;
            if (lro.getAlias().equals("d")) {
                assertNotNull(lro.getSchema());
            }
        }
    }

    public class MyPlanOptimizer extends LogicalPlanOptimizer {

        protected MyPlanOptimizer(OperatorPlan p,  int iterations) {
            super(p, iterations, null);
        }

        protected List<Set<Rule>> buildRuleSets() {
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();

            Rule r = new ColumnMapKeyPrune("ColumnMapKeyPrune");
            Set<Rule> s = new HashSet<Rule>();
            s.add(r);
            ls.add(s);

            r = new AddForEach("AddForEach");
            s = new HashSet<Rule>();
            s.add(r);
            ls.add(s);

            return ls;
        }
    }
}

