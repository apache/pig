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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.rules.AddForEach;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.MergeForEach;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMergeForEachOptimization {
    LogicalPlan plan = null;
    PigContext pc = new PigContext( ExecType.LOCAL, new Properties() );
    PigServer pigServer = null;
  
    @Before
    public void setup() throws ExecException {
        pigServer = new PigServer( pc );
    }
    
    @After
    public void tearDown() {
        
    }
    
    /**
     * Basic test case. Two simple FOREACH statements can be merged to one.
     * @throws Exception 
     */
    @Test   
    public void testSimple() throws Exception  {
        String query = "A = load 'file.txt' as (a, b, c);" +
         "B = foreach A generate a+b, c-b;" +
         "C = foreach B generate $0+5, $1;" +
         "store C into 'empty';";  
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        
        int forEachCount1 = getForEachOperatorCount( newLogicalPlan );
        int outputExprCount1 = getOutputExprCount( newLogicalPlan );
        LOForEach foreach1 = getForEachOperator( newLogicalPlan );
        Assert.assertTrue( foreach1.getAlias().equals( "C" ) );
               
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        
        int forEachCount2 = getForEachOperatorCount( newLogicalPlan );
        Assert.assertEquals( 1, forEachCount1 - forEachCount2 );
        int outputExprCount2 = getOutputExprCount( newLogicalPlan );
        Assert.assertTrue( outputExprCount1 == outputExprCount2 );
        LOForEach foreach2 = getForEachOperator( newLogicalPlan );
        Assert.assertTrue( foreach2.getAlias().equals( "C" ) );
    }
    
    /**
     * Test more complex case where the first for each in the script has inner plan.
     * @throws Exception 
     */
    @Test
    public void testComplex() throws Exception {
        String query = "A = load 'file.txt' as (a:int, b, c:bag{t:tuple(c0:int,c1:int)});" +
         "B = foreach A { S = ORDER c BY $0; generate $0, COUNT(S), SIZE(S); };" +
         "C = foreach B generate $2+5 as x, $0-$1/2 as y;" + "store C into 'empty';" ;  
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        
        int forEachCount1 = getForEachOperatorCount( newLogicalPlan );
        int outputExprCount1 = getOutputExprCount( newLogicalPlan );
        LOForEach foreach1 = getForEachOperator( newLogicalPlan );
        Assert.assertTrue( foreach1.getAlias().equals( "C" ) );
               
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        
        int forEachCount2 = getForEachOperatorCount( newLogicalPlan );
        // The number of FOREACHes didn't change because one is genereated because of type cast and
        // one is reduced because of the merge.
        Assert.assertEquals( 0, forEachCount1 - forEachCount2 );
        int outputExprCount2 = getOutputExprCount( newLogicalPlan );
        Assert.assertTrue( outputExprCount1 == outputExprCount2 );
        LOForEach foreach2 = getForEachOperator( newLogicalPlan );
        Assert.assertTrue( foreach2.getAlias().equals( "C" ) );
        LogicalSchema newSchema = foreach2.getSchema();
        Assert.assertTrue(newSchema.getField(0).alias.equals("x"));
        Assert.assertTrue(newSchema.getField(1).alias.equals("y"));
    }
    
    /**
     * One output of first foreach was referred more than once in the second foreach
     * @throws Exception 
     */
    @Test
    public void testDuplicateInputs() throws Exception {
        String query = "A = load 'file.txt' as (a0:int, a1:double);" +
         "A1 = foreach A generate (int)a0 as a0, (double)a1 as a1;" +
         "B = group A1 all;" +
         "C = foreach B generate A1;" +
         "D = foreach C generate SUM(A1.a0), AVG(A1.a1);" + "store D into 'empty';" ;  
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        
        Operator store = newLogicalPlan.getSinks().get(0);
        int forEachCount1 = getForEachOperatorCount( newLogicalPlan );
        LOForEach foreach1 = (LOForEach)newLogicalPlan.getPredecessors(store).get(0);
        Assert.assertTrue( foreach1.getAlias().equals( "D" ) );
               
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        
        int forEachCount2 = getForEachOperatorCount( newLogicalPlan );
        // The number of FOREACHes didn't change because one is genereated because of type cast and
        // one is reduced because of the merge.
        Assert.assertEquals( 1, forEachCount1 - forEachCount2 );
        
        LOForEach foreach2 = (LOForEach)newLogicalPlan.getPredecessors(store).get(0);
        Assert.assertTrue( foreach2.getAlias().equals( "D" ) );
    }
    
    /**
     * Not all consecutive FOREACHes can be merged. In this case, the second FOREACH statment
     * has inner plan, which cannot be merged with one before it.
     * @throws Exception 
     */
    @Test
    public void testNegative1() throws Exception {
        String query = "A = LOAD 'file.txt' as (a, b, c, d:bag{t:tuple(c0:int,c1:int)});" +
         "B = FOREACH A GENERATE a+5 AS u, b-c/2 AS v, d AS w;" +
         "C = FOREACH B { S = ORDER w BY $0; GENERATE $0 as x, COUNT(S) as y; };" +
         "store C into 'empty';";  
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        
        int forEachCount1 = getForEachOperatorCount( newLogicalPlan );
               
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        int forEachCount2 = getForEachOperatorCount( newLogicalPlan );
        
        // Actually MergeForEach optimization is happening here. A new foreach will be inserted after A because
        // of typ casting. The inserted one and the one in B can be merged due to this optimization. However, 
        // the plan cannot be further optimized because C has inner plan.
        Assert.assertEquals( forEachCount1, forEachCount2 );
    }
    
    /**
     * MergeForEach Optimization is off if the first statement has a FLATTEN operator.
     * @throws Exception 
     */
    @Test
    public void testNegative2() throws Exception {
        String query = "A = LOAD 'file.txt' as (a, b, c);" +
         "B = FOREACH A GENERATE FLATTEN(a), b, c;" +
         "C = FOREACH B GENERATE $0, $1+$2;" + "store C into 'empty';" ;  
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        
        int forEachCount1 = getForEachOperatorCount( newLogicalPlan );
               
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        
        int forEachCount2 = getForEachOperatorCount( newLogicalPlan );
        Assert.assertEquals( 2, forEachCount1 );
        Assert.assertEquals( 2, forEachCount2 );
    }
    
    
    /**
     * Ensure that join input order does not get reversed (PIG-1672)
     * @throws Exception 
     */
    @Test   
    public void testJoinInputOrder() throws Exception  {
        String query = "l1 = load 'y' as (a);" +
         "l2 = load 'z' as (a1,b1,c1,d1);" +
         "f1 = foreach l2 generate a1, b1, c1, d1;" +
         "f2 = foreach f1 generate a1, b1, c1;" +
         "j1 = join f2 by a1, l1 by a using 'replicated';" + "store j1 into 'empty';" ;  
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);

        int forEachCount1 = getForEachOperatorCount( newLogicalPlan );
        List<Operator> loads = newLogicalPlan.getSources();
        Operator l2 = null;
        for (Operator load : loads) {
            if (((LogicalRelationalOperator)load).getAlias().equals("l2"))
                l2 = load;
        }
        LOForEach foreachL2 = (LOForEach)newLogicalPlan.getSuccessors(l2).get(0);
        foreachL2 = (LOForEach)newLogicalPlan.getSuccessors(foreachL2).get(0);
        
        int outputExprCount1 = ((LOGenerate)foreachL2.getInnerPlan().getSinks().get(0)).getOutputPlans().size();
               
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        
        int forEachCount2 = getForEachOperatorCount( newLogicalPlan );
        Assert.assertEquals( 1, forEachCount1 - forEachCount2 );
        
        loads = newLogicalPlan.getSources();
        l2 = null;
        for (Operator load : loads) {
            if (((LogicalRelationalOperator)load).getAlias().equals("l2"))
                l2 = load;
        }
        foreachL2 = (LOForEach)newLogicalPlan.getSuccessors(l2).get(0);
        
        int outputExprCount2 = ((LOGenerate)foreachL2.getInnerPlan().getSinks().get(0)).getOutputPlans().size();
        
        Assert.assertTrue( outputExprCount1 == outputExprCount2 );
        Assert.assertTrue( foreachL2.getAlias().equals( "f2" ) );
        
        LOJoin join = (LOJoin)getOperator(newLogicalPlan, LOJoin.class);
        LogicalRelationalOperator leftInp =
            (LogicalRelationalOperator)newLogicalPlan.getPredecessors(join).get(0);
        assertEquals("join child left", leftInp.getAlias(), "f2"); 
        
        LogicalRelationalOperator rightInp =
            (LogicalRelationalOperator)newLogicalPlan.getPredecessors(join).get(1);
        assertEquals("join child right", rightInp.getAlias(), "l1"); 
        
    }

    private int getForEachOperatorCount(LogicalPlan plan) {
        Iterator<Operator> ops = plan.getOperators();
        int count = 0;
        while( ops.hasNext() ) {
            Operator op = ops.next();
            if( op instanceof LOForEach )
                count++;
        }
        return count;
    }
       
    private int getOutputExprCount(LogicalPlan plan) throws IOException {
        LOForEach foreach = getForEachOperator( plan );
        LogicalPlan inner = foreach.getInnerPlan();
        List<Operator> ops = inner.getSinks();
        LOGenerate gen = (LOGenerate)ops.get( 0 );
        return gen.getOutputPlans().size();
    }
    
    private LOForEach getForEachOperator(LogicalPlan plan) throws IOException {
        Iterator<Operator> ops = plan.getOperators();
        while( ops.hasNext() ) {
            Operator op = ops.next();
            if( op instanceof LOForEach ) {
                LOForEach foreach = (LOForEach)op;
                Operator succ = plan.getSuccessors( foreach ).get( 0 );
                if( !(succ instanceof LOForEach ) )
                    return foreach;
            }
        }
        return null;
    }
    
    /**
     * returns first operator that is an instance of given class c
     * @param plan
     * @param c
     * @return instance of class c if any, or null
     * @throws IOException
     */
    private Operator getOperator(LogicalPlan plan, Class<? extends Operator> c) throws IOException {
        Iterator<Operator> ops = plan.getOperators();
        while( ops.hasNext() ) {
            Operator op = ops.next();
            if( op.getClass().equals(c)) {
                return op;          
            }
        }
        return null;
    }
    

    public class MyPlanOptimizer extends LogicalPlanOptimizer {
        protected MyPlanOptimizer(OperatorPlan p,  int iterations) {
            super(p, iterations, new HashSet<String>());
        }
        
        protected List<Set<Rule>> buildRuleSets() {            
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
            
            Set<Rule> s = new HashSet<Rule>();
            // add split filter rule
            Rule r = new LoadTypeCastInserter( "TypeCastInserter" );
            s.add(r);
            ls.add(s);
             
            // Split Set
            // This set of rules does splitting of operators only.
            // It does not move operators
            s = new HashSet<Rule>();
            r = new AddForEach( "AddForEach" );
            s.add(r);            
            ls.add(s);
            
            s = new HashSet<Rule>();
            r = new MergeForEach("MergeForEach");
            s.add(r);            
            ls.add(s);

            return ls;
        }
    }    
}
