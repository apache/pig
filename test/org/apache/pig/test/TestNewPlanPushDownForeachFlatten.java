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


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.utils.Identity;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.LogicalPlanMigrationVistor;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.OptimizerUtils;
import org.apache.pig.newplan.logical.rules.PushDownForEachFlatten;
import org.apache.pig.newplan.logical.rules.TypeCastInserter;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

/**
 * Test the logical optimizer.
 */
public class TestNewPlanPushDownForeachFlatten {
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    LogicalPlanTester planTester = new LogicalPlanTester(pc) ;
    
    @Before
    public void tearDown() {
        planTester.reset();
    }

    /**
     * 
     * A simple filter UDF for testing
     *
     */
    static public class MyFilterFunc extends FilterFunc {
        
        @Override
        public Boolean exec(Tuple input) {
            return false;
        }
    }
    
    /**
     * Old plan is empty, so is the optimized new plan.
     */
    @Test
    public void testErrorEmptyInput() throws Exception {
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            new org.apache.pig.impl.logicalLayer.LogicalPlan();
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Assert.assertTrue( newLogicalPlan.getOperators().hasNext() ==  false );
    }

    /**
     * No foreach in the plan, no effect.
     */
    @Test
    public void testErrorNonForeachInput() throws Exception {
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        List<Operator> nexts = newLogicalPlan.getSuccessors( load );
        Assert.assertTrue( nexts == null || nexts.size() == 0 );
}
    
    @Test
    public void testForeachNoFlatten() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, $2;");
        planTester.buildPlan("C = order B by $0, $1;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan( "D = store C into 'dummy';" );
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    @Test
    public void testForeachNoSuccessors() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("B = foreach A generate flatten($1);");
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
    }
    
    @Test
    public void testForeachStreaming() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate flatten($1);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("C = stream B through `" + "pc -l" + "`;");
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
    }
    
    @Test
    public void testForeachDistinct() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate flatten($1);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("C = distinct B;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
    }
    
    @Test
    public void testForeachForeach() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");        
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("C = foreach B generate $0;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }
    

    @Test
    public void testForeachFilter() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");        
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("C = filter B by $1 < 18;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }

    @Test
    public void testForeachSplitOutput() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("split B into C if $1 < 18, D if $1 >= 18;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }

    @Test
    public void testForeachLimit() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("B = limit B 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }

    @Test
    public void testForeachUnion() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("D = union B, C;");        
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator load = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            load = loads.get( 0 );
        else
            load = loads.get( 1 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }
    
    @Test
    public void testForeachCogroup() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("D = cogroup B by $0, C by $0;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator load = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            load = loads.get( 0 );
        else
            load = loads.get( 1 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }
    
    @Test
    public void testForeachGroupBy() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("C = group B by $0;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }
    
    @Test
    public void testForeachSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = order B by $0, $1;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan( "D = store C into 'dummy';" );
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator sort = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
        Operator foreach = newLogicalPlan.getSuccessors( sort ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
    }
    
    /**
     * Non-pure-projection, not optimizable.
     */
    @Test
    public void testForeachSortNegative1() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0 + 5, $1, flatten($2);");
        planTester.buildPlan("C = order B by $0, $1;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan( "D = store C into 'dummy';" );
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    
    /**
     * If the flattened field is referenced in the sort condition, then no optimization can be done.
     */
    @Test
    public void testForeachSortNegative2() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:tuple(x,y));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = order B by $0, $3;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan( "D = store C into 'dummy';" );
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator foreach1 = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach1 instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach1 ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }

    @Test
    public void testForeachFlattenAddedColumnSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("C = order B by $0, $1;");
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    @Test
    public void testForeachUDFSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, " + Identity.class.getName() + "($2) ;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("C = order B by $0, $1;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    @Test
    public void testForeachCastSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate (chararray)$0, $1, flatten($2);");        
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("C = order B by $0, $1;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    @Test
    public void testForeachCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("D = cross B, C;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOCross );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    @Test
    public void testForeachCross1() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("C = foreach B generate $0, $1, flatten($2);");
        planTester.buildPlan("D = cross A, C;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "B" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOCross );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    // TODO
    // The following test case testForeachCross2 has multiple foreach flatten
    // A new rule should optimize this case
    @Test
    public void testForeachCross2() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = foreach C generate $0, $1, flatten($2);");
        planTester.buildPlan("E = cross B, D;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("F = limit E 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        // No optimization about foreach flatten.
        Assert.assertTrue( newLogicalPlan.getPredecessors( newLogicalPlan.getSinks().get( 0 ) ).get( 0 ) instanceof LOCross );
    }
    
    /**
     * This actually is a valid case, even though the optimization may not provide any performance benefit. However, detecting 
     * such a case requires more coding. Thus, we allow optimization to go thru in this case.
     */
    @Test
    public void testForeachFlattenAddedColumnCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = cross B, C;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOCross );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    /**
     * This is a valid, positive test case. Optimization should go thru.
     */
    @Test
    public void testForeachUDFCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, flatten($1), " + Identity.class.getName() + "($2) ;");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = cross B, C;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOCross );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }
    
    /**
     * Cast should NOT matter to cross. This is a valid positive test case.
     */
    @Test
    public void testForeachCastCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, (int)$1, flatten( $2 );");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = cross B, C;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOCross );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }
    
    @Test
    public void testForeachFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("D = join B by $0, C by $0 using \"replicated\";");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    @Test
    public void testForeachFRJoin1() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("C = foreach B generate $0, $1, flatten($2);");
        planTester.buildPlan("D = join A by $0, C by $0 using \"replicated\";");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "B" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    // TODO
    // The following test case testForeachFRJoin2 has multiple foreach flatten
    // A new rule should optimize this case
    @Test
    public void testForeachFRJoin2() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = foreach C generate $0, $1, flatten($2);");
        planTester.buildPlan("E = join B by $0, D by $0 using \"replicated\";");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("F = limit E 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        // No optimization about foreach flatten.
        Assert.assertTrue( newLogicalPlan.getPredecessors( newLogicalPlan.getSinks().get( 0 ) ).get( 0 ) instanceof LOJoin );
    }
    
    /**
     * Valid positive test case, even though the benefit from the optimization is questionable. However, putting in additinal check for
     * this condition requires extra coding.
     */
    @Test
    public void testForeachFlattenAddedColumnFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0 using \"replicated\";");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    /**
     * This is actually a valid, positive test case. UDF doesn't prevent optimization.
     * @throws Exception
     */
    @Test
    public void testForeachUDFFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, flatten($1), " + Identity.class.getName() + "($2) ;");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0 using \"replicated\";");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    /**
     * This is actually a valid, positive test case. Cast doesn't prevent optimization.
     * @throws Exception
     */
    @Test
    public void testForeachCastFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, (int)$1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0 using \"replicated\";");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    @Test
    public void testForeachInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }
    
    @Test
    public void testForeachInnerJoin1() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("C = foreach B generate $0, $1, flatten($2);");
        planTester.buildPlan("D = join A by $0, C by $0;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "B" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    // TODO
    // The following test case testForeachInnerJoin2 has multiple foreach flatten
    // A new rule should optimize this case
    @Test
    public void testForeachInnerJoin2() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = foreach C generate $0, $1, flatten($2);");
        planTester.buildPlan("E = join B by $0, D by $0;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("F = limit E 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        // No optimization about foreach flatten.
        Assert.assertTrue( newLogicalPlan.getPredecessors( newLogicalPlan.getSinks().get( 0 ) ).get( 0 ) instanceof LOJoin );
    }
    
    /**
     * This is actually a valid positive test case, even though the benefit of such optimization is questionable. However, 
     * checking for such condition requires additional coding effort.
     */
    @Test
    public void testForeachFlattenAddedColumnInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    /**
     * UDF doesn't prevent optimization.
     */
    @Test
    public void testForeachUDFInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, flatten($1), " + Identity.class.getName() + "($2) ;");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    /**
     * Cast doesn't prevent optimization.
     */
    @Test
    public void testForeachCastInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, (int)$1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) 
            op = loads.get( 0 );
        else
            op = loads.get( 1 );

        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)op ) );
        op = newLogicalPlan.getSuccessors( op ).get( 0 );
        Assert.assertTrue( op instanceof LOLimit );
    }

    // See PIG-1172
    @Test
    public void testForeachJoinRequiredField() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (bg:bag{t:tuple(a0,a1)});");
        planTester.buildPlan("B = FOREACH A generate flatten($0);");
        planTester.buildPlan("C = load '3.txt' AS (c0, c1);");
        planTester.buildPlan("D = JOIN B by a1, C by c1;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        // No optimization about foreach flatten.
        Assert.assertTrue( newLogicalPlan.getPredecessors( newLogicalPlan.getSinks().get( 0 ) ).get( 0 ) instanceof LOJoin );
    }
    
    // See PIG-1374
    @Test
    public void testForeachRequiredField() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (b{t(a0:chararray,a1:int)});");
        planTester.buildPlan("B = foreach A generate flatten($0);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("C = order B by $1 desc;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator foreach1 = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach1 instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach1 ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    // See PIG-1706
    @Test
    public void testForeachWithUserDefinedSchema() throws Exception {
        planTester.buildPlan("a = load '1.txt' as (a0:int, a1, a2:bag{t:(i1:int, i2:int)});");
        planTester.buildPlan("b = load '2.txt' as (b0:int, b1);");
        planTester.buildPlan("c = foreach a generate a0, flatten(a2) as (q1, q2);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = planTester.buildPlan("d = join c by a0, b by b0;");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        LOForEach foreach = (LOForEach)newLogicalPlan.getSinks().get( 0 );
        Assert.assertTrue(foreach.getSchema().getField(1).alias.equals("q1"));
        Assert.assertTrue(foreach.getSchema().getField(2).alias.equals("q2"));
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
             
            s = new HashSet<Rule>();
            r = new PushDownForEachFlatten( "PushDownForEachFlatten" );
            s.add(r);            
            ls.add(s);
            
            return ls;
        }
    }    

    private LogicalPlan migrateAndOptimizePlan(org.apache.pig.impl.logicalLayer.LogicalPlan plan) throws IOException {
        LogicalPlan newLogicalPlan = migratePlan( plan );
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
    }

    private LogicalPlan migratePlan(org.apache.pig.impl.logicalLayer.LogicalPlan lp) throws VisitorException{
        LogicalPlanMigrationVistor visitor = new LogicalPlanMigrationVistor(lp);        
        visitor.visit();
        org.apache.pig.newplan.logical.relational.LogicalPlan newPlan = visitor.getNewLogicalPlan();
        return newPlan;
    }
}

