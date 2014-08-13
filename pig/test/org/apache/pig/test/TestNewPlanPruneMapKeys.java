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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanPrinter;
import org.apache.pig.newplan.logical.optimizer.ProjectionPatcher;
import org.apache.pig.newplan.logical.optimizer.SchemaPatcher;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.rules.ColumnMapKeyPrune;
import org.apache.pig.newplan.logical.rules.MapKeysPruneHelper;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.PlanTransformListener;
import org.apache.pig.newplan.optimizer.Rule;
import org.junit.Test;

public class TestNewPlanPruneMapKeys {
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());

    private org.apache.pig.newplan.logical.relational.LogicalPlan migratePlan(String query) throws Exception{
        PigServer pigServer = new PigServer( pc );
        org.apache.pig.newplan.logical.relational.LogicalPlan newPlan =
        Util.buildLp(pigServer, query);

        // run filter rule
        Set<Rule> s = new HashSet<Rule>();
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        // Add the PruneMap Filter
        Rule r = new ColumnMapKeyPrune("PruneMapKeys");
        s.add(r);

        printPlan((org.apache.pig.newplan.logical.relational.LogicalPlan)newPlan);

        // Run the optimizer
        MyPlanOptimizer optimizer = new MyPlanOptimizer(newPlan, ls, 3);
        optimizer.addPlanTransformListener(new ProjectionPatcher());
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.optimize();

        return newPlan;
    }

    public class MyPlanOptimizer extends PlanOptimizer {

        protected MyPlanOptimizer(OperatorPlan p, List<Set<Rule>> rs,
                int iterations) {
            super(p, rs, iterations);
        }

        public void addPlanTransformListener(PlanTransformListener listener) {
            super.addPlanTransformListener(listener);
        }

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePlan() throws Exception {
        String query = "a =load 'd.txt' as (a:map[], b:int, c:float);" +
        "b = filter a by a#'name' == 'hello';" +
        "store b into 'empty';";

        org.apache.pig.newplan.logical.relational.LogicalPlan newLogicalPlan = migratePlan(query);

        List<Operator> sources = newLogicalPlan.getSources();
        assertEquals( 1, sources.size() );
        for( Operator source : sources ) {
            Map<Long,Set<String>> annotation =
                (Map<Long, Set<String>>) source.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
            assertTrue(annotation == null || annotation.isEmpty() );
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePlan2() throws Exception {
        String query = "a =load 'd.txt' as (a:map[], b:int, c:float);" +
        "b = filter a by a#'name' == 'hello';" +
        "c = foreach b generate b,c;" +
        "store c into 'empty';";

        org.apache.pig.newplan.logical.relational.LogicalPlan newLogicalPlan = migratePlan(query);

        assertEquals( 1, newLogicalPlan.getSources().size() );
        LOLoad load = (LOLoad) newLogicalPlan.getSources().get(0);
        Map<Long,Set<String>> annotation =
            (Map<Long, Set<String>>) load.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
        assertNotNull(annotation);
        assertEquals( 1, annotation.keySet().size() );
        Integer[] keySet = annotation.keySet().toArray( new Integer[0] );
        assertEquals( new Integer(0), keySet[0] );
        Set<String> keys = annotation.get(0);
        assertEquals( 1, keys.size() );
        assertEquals( "name", keys.toArray( new String[0] )[0] );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePlan3() throws Exception {
        String query = "a =load 'd.txt' as (a:map[], b:int, c:float);" +
        "b = filter a by a#'name' == 'hello';" +
        "c = foreach b generate a#'age',b,c;" +
        "store c into 'empty';";

        org.apache.pig.newplan.logical.relational.LogicalPlan newLogicalPlan = migratePlan(query);

        assertEquals( 1, newLogicalPlan.getSources().size() );
        LOLoad load = (LOLoad) newLogicalPlan.getSources().get(0);
        Map<Long,Set<String>> annotation =
            (Map<Long, Set<String>>) load.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
        assertNotNull(annotation);
        assertEquals( 1, annotation.keySet().size() );
        Integer[] keySet = annotation.keySet().toArray( new Integer[0] );
        assertEquals( new Integer(0), keySet[0] );
        Set<String> keys = annotation.get(0);
        assertEquals( 2, keys.size() );
        assertTrue( keys.contains("name") );
        assertTrue( keys.contains("age"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePlan4() throws Exception {
        String query = "a =load 'd.txt' as (a:map[], b:int, c:float);" +
        "b = filter a by a#'name' == 'hello';" +
        "c = foreach b generate a#'age',a,b,c;" +
        "store c into 'empty';";

        org.apache.pig.newplan.logical.relational.LogicalPlan newLogicalPlan = migratePlan(query);

        List<Operator> sources = newLogicalPlan.getSources();
        assertEquals( 1, sources.size() );
        for( Operator source : sources ) {
            Map<Long,Set<String>> annotation =
                (Map<Long, Set<String>>) source.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
            assertTrue(annotation == null || annotation.isEmpty() );
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePlan5() throws Exception {
        String query = "a =load 'd.txt' as (a:chararray, b:int, c:float);" +
        "b = filter a by a == 'hello';" +
        "c = foreach b generate a,b,c;" +
        "store c into 'empty';";

        org.apache.pig.newplan.logical.relational.LogicalPlan newLogicalPlan = migratePlan(query);

        List<Operator> sources = newLogicalPlan.getSources();
        assertEquals( 1, sources.size() );
        for( Operator source : sources ) {
            Map<Long,Set<String>> annotation =
                (Map<Long, Set<String>>) source.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
            assertNull(annotation);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePlan6() throws Exception {
        String query = "a =load 'd.txt';" +
        "b = filter a by $0 == 'hello';" +
        "c = foreach b generate $0,$1,$2;" +
        "store c into 'empty';";

        org.apache.pig.newplan.logical.relational.LogicalPlan newLogicalPlan = migratePlan(query);

        List<Operator> sources = newLogicalPlan.getSources();
        assertEquals( 1, sources.size() );
        for( Operator source : sources ) {
            Map<Long,Set<String>> annotation =
                (Map<Long, Set<String>>) source.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
            assertTrue(annotation == null );
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePlan7() throws Exception {
        String query = "a =load 'd.txt';" +
        "a1 = load 'b.txt' as (a:map[],b:int, c:float);" +
        "b = join a by $0, a1 by a#'name';" +
        "c = foreach b generate $0,$1,$2;" +
        "store c into 'empty';";

        org.apache.pig.newplan.logical.relational.LogicalPlan newLogicalPlan = migratePlan(query);
        printPlan( newLogicalPlan );

        List<Operator> sources = newLogicalPlan.getSources();
        assertEquals( 2, sources.size() );
        for( Operator source : sources ) {
            Map<Long,Set<String>> annotation =
                (Map<Long, Set<String>>) source.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
            assertTrue( annotation == null || annotation.isEmpty() );
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimplePlan8() throws Exception {
        String query = "a =load 'd.txt';" +
        "a1 = load 'b.txt' as (a:chararray,b:int, c:float);" +
        "b = join a by $0, a1 by a;" +
        "c = foreach b generate $0,$1,$2;" +
        "store c into 'empty';";

        org.apache.pig.newplan.logical.relational.LogicalPlan newLogicalPlan = migratePlan(query);
        printPlan(newLogicalPlan);

        List<Operator> sources = newLogicalPlan.getSources();
        assertEquals( 2, sources.size() );
        for( Operator source : sources ) {
            Map<Long,Set<String>> annotation =
                (Map<Long, Set<String>>) source.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
            assertTrue( annotation == null || annotation.isEmpty() );
        }
    }

    public void printPlan(org.apache.pig.newplan.logical.relational.LogicalPlan logicalPlan ) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(out);
        LogicalPlanPrinter pp = new LogicalPlanPrinter(logicalPlan,ps);
        pp.visit();
        System.err.println(out.toString());
    }

    public void printPlan(PhysicalPlan physicalPlan) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(out);
        physicalPlan.explain(ps, "text", true);
        System.err.println(out.toString());
    }
}
