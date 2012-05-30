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
import java.util.Properties;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.FilterFunc;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.utils.Identity;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.logical.optimizer.ProjectionPatcher;
import org.apache.pig.newplan.logical.optimizer.SchemaPatcher;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.ColumnMapKeyPrune;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.OptimizerUtils;
import org.apache.pig.newplan.logical.rules.PushDownForEachFlatten;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

/**
 * Test the logical optimizer.
 */
public class TestNewPlanPushDownForeachFlatten {
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    
    @Before
    public void tearDown() {
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
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( "" );
        
        Assert.assertTrue( newLogicalPlan.getOperators().hasNext() ==  false );
    }

    /**
     * No foreach in the plan, no effect.
     */
    @Test
    public void testErrorNonForeachInput() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
                       "store A into 'output';";
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        List<Operator> nexts = newLogicalPlan.getSuccessors( load );
        Assert.assertTrue( nexts != null && nexts.size() == 1 );
}
    
    @Test
    public void testForeachNoFlatten() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, $2;" +
        "C = order B by $0, $1;" +
         "D = store C into 'dummy';";
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    @Test
    public void testForeachNoSuccessors() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
                       "B = foreach A generate flatten($1);" +
                       "Store B into 'output';";
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
    }
    
    @Test
    public void testForeachStreaming() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate flatten($1);" +
        "C = stream B through `" + "pc -l" + "`;" +
        "Store C into 'output';";
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
    }
    
    @Test
    public void testForeachDistinct() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate flatten($1);" +
        "C = distinct B;" +
        "store C into 'output';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
    }
    
    @Test
    public void testForeachForeach() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, flatten(1);" +        
        "C = foreach B generate $0;" +
        "store C into 'output';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( !OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }
    

    @Test
    public void testForeachFilter() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, flatten($2);" +        
        "C = filter B by $1 < 18;" +
        "store C into 'output';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }

    @Test
    public void testForeachSplitOutput() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "split B into C if $1 < 18, D if $1 >= 18;" +
        "store C into 'output1';" + 
        "store D into 'output2';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }

    @Test
    public void testForeachLimit() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = limit B 10;" +
        "store C into 'output';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }

    @Test
    public void testForeachUnion() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference);" +
        "D = union B, C;" +
        "store D into 'output';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }
    
    @Test
    public void testForeachCogroup() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference);" +
        "D = cogroup B by $0, C by $0;" +
        "store D into 'output';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

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
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }
    
    @Test
    public void testForeachGroupBy() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = group B by $0;" +
        "store C into 'output';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Assert.assertTrue( OptimizerUtils.hasFlatten( (LOForEach)foreach ) );
    }
    
    @Test
    public void testForeachSort() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = order B by $0, $1;" +
        "D = store C into 'dummy';";
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
        foreach = newLogicalPlan.getSuccessors( sort ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
    }
    
    /**
     * Non-pure-projection, not optimizable.
     */
    @Test
    public void testForeachSortNegative1() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0 + 5, $1, flatten($2);" +
        "C = order B by $0, $1;" +
         "D = store C into 'dummy';";
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    
    /**
     * If the flattened field is referenced in the sort condition, then no optimization can be done.
     */
    @Test
    public void testForeachSortNegative2() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa:tuple(x,y));" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = order B by $0, $3;" +
        "D = store C into 'dummy';";
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, flatten(1);" +
        "C = order B by $0, $1;" +
        "store C into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    @Test
    public void testForeachUDFSort() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, $1, " + Identity.class.getName() + "($2) ;" +
        "C = order B by $0, $1;" +
        "store C into 'output';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    @Test
    public void testForeachCastSort() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate (chararray)$0, $1, flatten($2);" +        
        "C = order B by $0, $1;" +
        "store C into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
    }
    
    @Test
    public void testForeachCross() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference);" +
        "D = cross B, C;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "C = foreach B generate $0, $1, flatten($2);" +
        "D = cross A, C;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = foreach C generate $0, $1, flatten($2);" +
        "E = cross B, D;" +
        "F = limit E 10;" +
        "store F into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        // No optimization about foreach flatten.
        Operator store = newLogicalPlan.getSinks().get( 0 );
        Operator limit = newLogicalPlan.getPredecessors(store).get(0);
        Operator cross = newLogicalPlan.getPredecessors(limit).get(0);
        Assert.assertTrue( cross instanceof LOCross );
    }
    
    /**
     * This actually is a valid case, even though the optimization may not provide any performance benefit. However, detecting 
     * such a case requires more coding. Thus, we allow optimization to go thru in this case.
     */
    @Test
    public void testForeachFlattenAddedColumnCross() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, $1, flatten(1);" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = cross B, C;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, flatten($1), " + Identity.class.getName() + "($2) ;" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = cross B, C;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, (int)$1, flatten( $2 );" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = cross B, C;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference);" +
        "D = join B by $0, C by $0 using 'replicated';" +
        "E = limit D 10;" +
        "store E into 'output';";

        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "C = foreach B generate $0, $1, flatten($2);" +
        "D = join A by $0, C by $0 using 'replicated';" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = foreach C generate $0, $1, flatten($2);" +
        "E = join B by $0, D by $0 using 'replicated';" +
        "F = limit E 10;" +
        "store F into 'output';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        // No optimization about foreach flatten.
        Operator store = newLogicalPlan.getSinks().get( 0 );
        Operator limit = newLogicalPlan.getPredecessors( store ).get( 0 );
        Operator join = newLogicalPlan.getPredecessors( limit ).get( 0 );
        Assert.assertTrue( join instanceof LOJoin );
    }
    
    /**
     * Valid positive test case, even though the benefit from the optimization is questionable. However, putting in additinal check for
     * this condition requires extra coding.
     */
    @Test
    public void testForeachFlattenAddedColumnFRJoin() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, $1, flatten(1);" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = join B by $0, C by $0 using 'replicated';" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, flatten($1), " + Identity.class.getName() + "($2) ;" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = join B by $0, C by $0 using 'replicated';" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, (int)$1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = join B by $0, C by $0 using 'replicated';" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = join B by $0, C by $0;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "C = foreach B generate $0, $1, flatten($2);" +
        "D = join A by $0, C by $0;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, $1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = foreach C generate $0, $1, flatten($2);" +
        "E = join B by $0, D by $0;" +
        "F = limit E 10;" +
        "store F into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        // No optimization about foreach flatten.
        Operator store = newLogicalPlan.getSinks().get( 0 );
        Operator limit = newLogicalPlan.getPredecessors( store ).get( 0 );
        Operator join = newLogicalPlan.getPredecessors( limit ).get( 0 );
        Assert.assertTrue( join instanceof LOJoin );
    }
    
    /**
     * This is actually a valid positive test case, even though the benefit of such optimization is questionable. However, 
     * checking for such condition requires additional coding effort.
     */
    @Test
    public void testForeachFlattenAddedColumnInnerJoin() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, $1, flatten(1);" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = join B by $0, C by $0;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, flatten($1), " + Identity.class.getName() + "($2) ;" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = join B by $0, C by $0;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));" +
        "B = foreach A generate $0, (int)$1, flatten($2);" +
        "C = load 'anotherfile' as (name, age, preference:(course_name, instructor));" +
        "D = join B by $0, C by $0;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "A = load 'myfile' as (bg:bag{t:tuple(a0,a1)});" +
        "B = FOREACH A generate flatten($0);" +
        "C = load '3.txt' AS (c0, c1);" +
        "D = JOIN B by a1, C by c1;" +
        "E = limit D 10;" +
        "store E into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        // No optimization about foreach flatten.
        Operator store = newLogicalPlan.getSinks().get( 0 );
        Operator limit = newLogicalPlan.getPredecessors( store ).get( 0 );
        Operator join = newLogicalPlan.getPredecessors( limit ).get( 0 );
        Assert.assertTrue( join instanceof LOJoin );
    }
    
    // See PIG-1374
    @Test
    public void testForeachRequiredField() throws Exception {
        String query = "A = load 'myfile' as (b:bag{t:tuple(a0:chararray,a1:int)});" +
        "B = foreach A generate flatten($0);" +
        "C = order B by $1 desc;" +
        "store C into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
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
        String query = "a = load '1.txt' as (a0:int, a1, a2:bag{t:(i1:int, i2:int)});" +
        "b = load '2.txt' as (b0:int, b1);" +
        "c = foreach a generate a0, flatten(a2) as (q1, q2);" +
        "d = join c by a0, b by b0;" +
        "store d into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator store = newLogicalPlan.getSinks().get( 0 );
        LOForEach foreach = (LOForEach)newLogicalPlan.getPredecessors(store).get(0);
        Assert.assertTrue(foreach.getSchema().getField(1).alias.equals("q1"));
        Assert.assertTrue(foreach.getSchema().getField(2).alias.equals("q2"));
    }
    
    // See PIG-1751
    @Test
    public void testForeachWithUserDefinedSchema2() throws Exception {
        String query = "a = load '1.txt' as (a0:chararray);" +
        "b = load '2.txt' as (b0:chararray);" +
        "c = foreach b generate flatten(STRSPLIT(b0)) as c0;" +
        "d = join c by (chararray)c0, a by a0;" +
        "store d into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator store = newLogicalPlan.getSinks().get( 0 );
        Operator op = newLogicalPlan.getPredecessors(store).get(0);
        Assert.assertTrue(op instanceof LOJoin);
    }

    // See PIG-2721
    @Test
    public void testForeachSortWithUserDefinedSchema() throws Exception {
        String query =
        "a = load '1.txt' as (a0:int, a1:bag{t:(i1:int, i2:int)});" +
        "b = foreach a generate a0, flatten(a1) as (q1, q2);" +
        "c = order b by a0;" +
        "store c into 'output';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlanWithPruning( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Assert.assertTrue( "Field \"a1\" is dropped by ColumnMapKeyPrune" + 
                  "even though it should be stored",
                  ((LOLoad)load).getSchema().getField("a1") != null );
    }

    public class MyPlanOptimizerWithPruning extends LogicalPlanOptimizer {
        protected MyPlanOptimizerWithPruning (OperatorPlan p,  int iterations) {
            super(p, iterations, new HashSet<String>());
            addPlanTransformListener(new SchemaPatcher());
            addPlanTransformListener(new ProjectionPatcher());
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

            s = new HashSet<Rule>();
            r = new ColumnMapKeyPrune( "ColumnMapKeyPrune" );
            s.add(r);
            ls.add(s);

            return ls;
        }
    }

    private LogicalPlan migrateAndOptimizePlanWithPruning(String query) throws Exception {
        PigServer pigServer = new PigServer( pc );
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        PlanOptimizer optimizer = new MyPlanOptimizerWithPruning( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
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

    private LogicalPlan migrateAndOptimizePlan(String query) throws Exception {
    	PigServer pigServer = new PigServer( pc );
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
    }

    @Test
    public void testNonDeterministicUdf() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $0, RANDOM(), flatten($2);" +
        "C = order B by $0, $1;" +
        "D = store C into 'dummy';";
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator foreach = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        foreach = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( foreach instanceof LOForEach );
        Operator sort = newLogicalPlan.getSuccessors( foreach ).get( 0 );
        Assert.assertTrue( sort instanceof LOSort );
        
    }
}

