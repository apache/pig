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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.LogicalPlanMigrationVistor;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.rules.AddForEach;
import org.apache.pig.newplan.logical.rules.ColumnMapKeyPrune;
import org.apache.pig.newplan.logical.rules.MapKeysPruneHelper;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.test.utils.LogicalPlanTester;

public class TestNewPlanColumnPrune extends TestCase {

    LogicalPlan plan = null;
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
  
    private LogicalPlan migratePlan(org.apache.pig.impl.logicalLayer.LogicalPlan lp) throws FrontendException{
        LogicalPlanMigrationVistor visitor = new LogicalPlanMigrationVistor(lp);        
        visitor.visit();
        org.apache.pig.newplan.logical.relational.LogicalPlan newPlan = visitor.getNewLogicalPlan();
        
        SchemaResetter schemaResetter = new SchemaResetter(newPlan);
        schemaResetter.visit();
        
        return newPlan;
    }
    
   
    public void testNoPrune() throws Exception  {
        // no foreach
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v2);");
        lpt.buildPlan("b = filter a by v1==NULL;");        
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan("store b into 'empty';");  
        LogicalPlan newLogicalPlan = migratePlan(plan);
               
        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v2);");
        lpt.buildPlan("b = filter a by v1==NULL;");        
        plan = lpt.buildPlan("store b into 'empty';");  
        LogicalPlan expected = migratePlan(plan);
        
        assertTrue(expected.isEqual(newLogicalPlan));
        
        // no schema
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt';");
        lpt.buildPlan("b = foreach a generate $0, $1;");
        plan = lpt.buildPlan("store b into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt';");
        lpt.buildPlan("b = foreach a generate $0, $1;");
        plan = lpt.buildPlan("store b into 'empty';");  
        expected = migratePlan(plan);
        assertTrue(expected.isEqual(newLogicalPlan));
    }
       
    public void testPrune() throws Exception  {
        // only foreach
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v2);");
        lpt.buildPlan("b = foreach a generate id;");        
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan("store b into 'empty';");  
        LogicalPlan newLogicalPlan = migratePlan(plan);
               
        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id);");
        lpt.buildPlan("b = foreach a generate id;");        
        plan = lpt.buildPlan("store b into 'empty';");  
        LogicalPlan expected = migratePlan(plan);
        
        assertTrue(expected.isEqual(newLogicalPlan));
        
        // with filter
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v5, v3, v4, v2);");
        lpt.buildPlan("b = filter a by v1 != NULL AND (v2+v3)<100;");
        lpt.buildPlan("c = foreach b generate id;");
        plan = lpt.buildPlan("store c into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v3, v2);");
        lpt.buildPlan("b = filter a by v1 != NULL AND (v2+v3)<100;");
        lpt.buildPlan("c = foreach b generate id;");
        plan = lpt.buildPlan("store c into 'empty';"); 
        expected = migratePlan(plan);
        assertTrue(expected.isEqual(newLogicalPlan));
        
        // with 2 foreach
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v5, v3, v4, v2);");
        lpt.buildPlan("b = foreach a generate v2, v5, v4;");
        lpt.buildPlan("c = foreach b generate v5, v4;");
        plan = lpt.buildPlan("store c into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (v5, v4);");
        lpt.buildPlan("b = foreach a generate v5, v4;");
        lpt.buildPlan("c = foreach b generate v5, v4;");
        plan = lpt.buildPlan("store c into 'empty';"); 
        expected = migratePlan(plan);
        assertTrue(expected.isEqual(newLogicalPlan));
        
        // with 2 foreach
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v5, v3, v4, v2);");
        lpt.buildPlan("b = foreach a generate id, v1, v5, v3, v4;");
        lpt.buildPlan("c = foreach b generate v5, v4;");
        plan = lpt.buildPlan("store c into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (v5, v4);");
        lpt.buildPlan("b = foreach a generate v5, v4;");
        lpt.buildPlan("c = foreach b generate v5, v4;");
        plan = lpt.buildPlan("store c into 'empty';"); 
        expected = migratePlan(plan);
        assertTrue(expected.isEqual(newLogicalPlan));
        
        // with 2 foreach and filter in between
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v5, v3, v4, v2);");
        lpt.buildPlan("b = foreach a generate v2, v5, v4;");
        lpt.buildPlan("c = filter b by v2 != NULL;");
        lpt.buildPlan("d = foreach c generate v5, v4;");
        plan = lpt.buildPlan("store d into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (v5, v4, v2);");
        lpt.buildPlan("b = foreach a generate v2, v5, v4;");
        lpt.buildPlan("c = filter b by v2 != NULL;");
        lpt.buildPlan("d = foreach c generate v5, v4;");
        plan = lpt.buildPlan("store d into 'empty';");  
        expected = migratePlan(plan);
        assertTrue(expected.isEqual(newLogicalPlan));
        
        // with 2 foreach after join
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v2, v3);");
        lpt.buildPlan("b = load 'c.txt' as (id, v4, v5, v6);");
        lpt.buildPlan("c = join a by id, b by id;");       
        lpt.buildPlan("d = foreach c generate a::id, v5, v3, v4;");
        plan = lpt.buildPlan("store d into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v3);");
        lpt.buildPlan("b = load 'c.txt' as (id, v4, v5);");
        lpt.buildPlan("c = join a by id, b by id;");  
        lpt.buildPlan("d = foreach c generate a::id, v5, v3, v4;");
        plan = lpt.buildPlan("store d into 'empty';");  
        expected = migratePlan(plan);
        assertTrue(expected.isEqual(newLogicalPlan));
        
        // with BinStorage, insert foreach after load
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);");        
        lpt.buildPlan("c = filter a by v2 != NULL;");
        lpt.buildPlan("d = foreach c generate v5, v4;");
        plan = lpt.buildPlan("store d into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);");
        lpt.buildPlan("b = foreach a generate v5, v4, v2;");
        lpt.buildPlan("c = filter b by v2 != NULL;");
        lpt.buildPlan("d = foreach c generate v5, v4;");
        plan = lpt.buildPlan("store d into 'empty';");  
        expected = migratePlan(plan);
        assertTrue(expected.isEqual(newLogicalPlan));
        
       // with BinStorage, not to insert foreach after load if there is already one
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);");    
        lpt.buildPlan("b = foreach a generate v5, v4, v2;");
        lpt.buildPlan("c = filter b by v2 != NULL;");
        lpt.buildPlan("d = foreach c generate v5;");
        plan = lpt.buildPlan("store d into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);");
        lpt.buildPlan("b = foreach a generate v5, v2;");
        lpt.buildPlan("c = filter b by v2 != NULL;");
        lpt.buildPlan("d = foreach c generate v5;");
        plan = lpt.buildPlan("store d into 'empty';");  
        expected = migratePlan(plan);
        assertTrue(expected.isEqual(newLogicalPlan));
        
       // with BinStorage, not to insert foreach after load if there is already one
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);");    
        lpt.buildPlan("b = foreach a generate v5, v4, v2, 10;");
        lpt.buildPlan("c = filter b by v2 != NULL;");
        lpt.buildPlan("d = foreach c generate v5;");
        plan = lpt.buildPlan("store d into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' using BinStorage() as (id, v1, v5, v3, v4, v2);");
        lpt.buildPlan("b = foreach a generate v5, v2, 10;");
        lpt.buildPlan("c = filter b by v2 != NULL;");
        lpt.buildPlan("d = foreach c generate v5;");
        plan = lpt.buildPlan("store d into 'empty';");  
        expected = migratePlan(plan);
        assertTrue(expected.isEqual(newLogicalPlan));
    }
    
    @SuppressWarnings("unchecked")
    public void testPruneWithMapKey() throws Exception {
         // only foreach
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, m:map[]);");
        lpt.buildPlan("b = foreach a generate id, m#'path';");        
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan("store b into 'empty';");  
        LogicalPlan newLogicalPlan = migratePlan(plan);
               
        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, m:map[]);");
        lpt.buildPlan("b = foreach a generate id, m#'path';");        
        plan = lpt.buildPlan("store b into 'empty';");  
        LogicalPlan expected = migratePlan(plan);
        
        assertTrue(expected.isEqual(newLogicalPlan));
        
        LOLoad op = (LOLoad)newLogicalPlan.getSources().get(0);
        Map<Integer,Set<String>> annotation = 
                (Map<Integer, Set<String>>) op.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
        assertEquals(annotation.size(), 1);
        Set<String> s = new HashSet<String>();
        s.add("path");
        assertEquals(annotation.get(2), s);
        
        // foreach with join
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, m:map[]);");
        lpt.buildPlan("b = load 'd.txt' as (id, v1, m:map[]);");
        lpt.buildPlan("c = join a by id, b by id;");
        lpt.buildPlan("d = filter c by a::m#'path' != NULL;");
        lpt.buildPlan("e = foreach d generate a::id, b::id, b::m#'path', a::m;");        
        plan = lpt.buildPlan("store e into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, m:map[]);");
        lpt.buildPlan("b = load 'd.txt' as (id, m:map[]);");
        lpt.buildPlan("c = join a by id, b by id;");
        lpt.buildPlan("d = filter c by a::m#'path' != NULL;");
        lpt.buildPlan("e = foreach d generate a::id, b::id, b::m#'path', a::m;");        
        plan = lpt.buildPlan("store e into 'empty';");  
        expected = migratePlan(plan);
        
        assertTrue(expected.isEqual(newLogicalPlan));
        
        List<Operator> ll = newLogicalPlan.getSources();
        assertEquals(ll.size(), 2);
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
        assertEquals(annotation.size(), 1);
    
        s = new HashSet<String>();
        s.add("path");
        assertEquals(annotation.get(2), s);
    }
    
    public void testPruneWithBag() throws Exception  {
        // filter above foreach
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v:bag{t:(s1,s2,s3)});");
        lpt.buildPlan("b = filter a by id>10;");
        lpt.buildPlan("c = foreach b generate id, FLATTEN(v);");    
        lpt.buildPlan("d = foreach c generate id, v::s2;");    
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan("store d into 'empty';");  
        LogicalPlan newLogicalPlan = migratePlan(plan);
               
        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v:bag{t:(s1,s2,s3)});");
        lpt.buildPlan("b = filter a by id>10;");
        lpt.buildPlan("c = foreach b generate id, FLATTEN(v);");    
        lpt.buildPlan("d = foreach c generate id, v::s2;");    
        plan = lpt.buildPlan("store d into 'empty';");
        LogicalPlan expected = migratePlan(plan);
        
        assertTrue(expected.isEqual(newLogicalPlan));
    }
    
    public void testAddForeach() throws Exception  {
        // filter above foreach
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v2);");
        lpt.buildPlan("b = filter a by v1>10;");
        lpt.buildPlan("c = foreach b generate id;");        
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan("store c into 'empty';");  
        LogicalPlan newLogicalPlan = migratePlan(plan);
               
        PlanOptimizer optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1);");
        lpt.buildPlan("b = filter a by v1>10;"); 
        lpt.buildPlan("c = foreach b generate id;");      
        plan = lpt.buildPlan("store c into 'empty';");  
        LogicalPlan expected = migratePlan(plan);
        
        assertTrue(expected.isEqual(newLogicalPlan));
        
        // join with foreach
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1, v2);");
        lpt.buildPlan("b = load 'd.txt' as (id, v1, v2);");
        lpt.buildPlan("c = join a by id, b by id;");
        lpt.buildPlan("d = filter c by a::v1>b::v1;");
        lpt.buildPlan("e = foreach d generate a::id;");        
        plan = lpt.buildPlan("store e into 'empty';");  
        newLogicalPlan = migratePlan(plan);
               
        optimizer = new MyPlanOptimizer(newLogicalPlan, 3);
        optimizer.optimize();
        
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, v1);");
        lpt.buildPlan("b = load 'd.txt' as (id, v1);");
        lpt.buildPlan("c = join a by id, b by id;");
        lpt.buildPlan("d = foreach c generate a::id, a::v1, b::v1;");        
        lpt.buildPlan("e = filter d by a::v1>b::v1;");  
        lpt.buildPlan("f = foreach e generate a::id;");        
        plan = lpt.buildPlan("store f into 'empty';");  
        expected = migratePlan(plan);
        
        assertTrue(expected.isEqual(newLogicalPlan));
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
