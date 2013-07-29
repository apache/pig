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
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.PushUpFilter;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.PlanTransformListener;
import org.apache.pig.newplan.optimizer.Rule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the logical optimizer.
 */
public class TestNewPlanPushUpFilter {
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());

    @Before
    public void tearDown() {
    }

    /**
     * A simple filter UDF for testing
     */
    static public class MyFilterFunc extends FilterFunc {
        
        @Override
        public Boolean exec(Tuple input) {
            return false;
        }
    }
    
    @Test
    // Empty plan, nothing to update
    public void testErrorEmptyInput() throws Exception {
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( "" );
        
        Assert.assertTrue( !newLogicalPlan.getOperators().hasNext() );
    }

    @Test
    //Test to ensure that the right exception is thrown when the input list is empty
    public void testErrorNonFilterInput() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);store A into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        Operator op = newLogicalPlan.getSources().get(0);
        Assert.assertTrue( op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue( op instanceof LOStore );
        Assert.assertTrue( newLogicalPlan.getSuccessors(op) == null );
    }
    
    @Test
    public void testFilterLoad() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = filter A by $1 < 18;" +
            "store B into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get(0);
        Assert.assertTrue( op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue( op instanceof LOStore );
    }
    
    @Test
    public void testFilterStreaming() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = stream A through `" + "ps -u" + "`;" +
            "C = filter B by $1 < 18;" +
            "D = STORE C into 'dummy';";        
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStream );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
    }
    
    @Test
    public void testFilterSort() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = order A by $1, $2;" +
            "C = filter B by $1 < 18;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOSort );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    @Test
    public void testFilterConstantConditionSort() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = order A by $1, $2;" +
            "C = filter B by 1 == 1;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOSort );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    @Test
    public void testFilterUDFSort() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = order A by $1, $2;" +
            "C = filter B by " + MyFilterFunc.class.getName() + "($1) ;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOSort );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    @Test
    public void testFilterDistinct() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = distinct A;" +
            "C = filter B by $1 < 18;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LODistinct );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    @Test
    public void testFilterConstantConditionDistinct() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = distinct A;" +
            "C = filter B by 1 == 1;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LODistinct );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    @Test
    public void testFilterUDFDistinct() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = distinct A;" +
            "C = filter B by " + MyFilterFunc.class.getName() + "($1) ;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LODistinct );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    @Test
    public void testFilterFilter() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = filter A by $0 != 'name';" +
            "C = filter B by $1 < 18;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        Assert.assertTrue( ((LOFilter)op).getAlias().equals( "B" ) );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        Assert.assertTrue( ((LOFilter)op).getAlias().equals( "C" ) );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    @Test
    public void testFilterSplitOutput() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "split A into B if $1 < 18, C if $1 >= 18;" +
            "C = filter B by $1 < 10;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOSplit );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOSplitOutput );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    @Test
    public void testFilterLimit() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = limit A 10;" +
            "C = filter B by $1 < 18;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOLimit );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    @Test
    public void testFilterUnion() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = union A, B;" +
            "D = filter C by $1 < 18;" +
            "E = STORE D into'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) )  {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator filterA = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  filterA instanceof LOFilter );
        Operator filterB = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  filterB instanceof LOFilter );
        
        Operator unionA = newLogicalPlan.getSuccessors( filterA ).get( 0 );
        Assert.assertTrue(  unionA instanceof LOUnion );
        Operator unionB = newLogicalPlan.getSuccessors( filterB ).get( 0 );
        Assert.assertTrue(  unionB instanceof LOUnion );
        Assert.assertTrue(  unionB == unionA );
        
        Operator store = newLogicalPlan.getSuccessors(unionA).get( 0 );
        Assert.assertTrue(  store instanceof LOStore );
    }
    
    @Test
    public void testFilterConstantConditionUnion() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = union A, B;" +
            "D = filter C by 1 == 1;" +
            "E = STORE D into'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) )  {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator filterA = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  filterA instanceof LOFilter );
        Operator filterB = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  filterB instanceof LOFilter );
        
        Operator unionA = newLogicalPlan.getSuccessors( filterA ).get( 0 );
        Assert.assertTrue(  unionA instanceof LOUnion );
        Operator unionB = newLogicalPlan.getSuccessors( filterB ).get( 0 );
        Assert.assertTrue(  unionB instanceof LOUnion );
        Assert.assertTrue(  unionB == unionA );
        
        Operator store = newLogicalPlan.getSuccessors(unionA).get( 0 );
        Assert.assertTrue(  store instanceof LOStore );
    }
    
    @Test
    public void testFilterUDFUnion() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = union A, B;" +
            "D = filter C by " + MyFilterFunc.class.getName() + "() ;" +
            "E = STORE D into'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) )  {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator filterA = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  filterA instanceof LOFilter );
        Operator filterB = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  filterB instanceof LOFilter );
        
        Operator unionA = newLogicalPlan.getSuccessors( filterA ).get( 0 );
        Assert.assertTrue(  unionA instanceof LOUnion );
        Operator unionB = newLogicalPlan.getSuccessors( filterB ).get( 0 );
        Assert.assertTrue(  unionB instanceof LOUnion );
        Assert.assertTrue(  unionB == unionA );
        
        Operator store = newLogicalPlan.getSuccessors(unionA).get( 0 );
        Assert.assertTrue(  store instanceof LOStore );
    }
    
    @Test
    public void testFilterCross() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cross A, B;" +
            "D = filter C by $5 < 18;" +
            "E = limit D 10;" +
            "F = STORE E into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCross );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOLimit );
    }

    @Test
    public void testFilterCross1() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cross A, B;" +
            "D = filter C by $1 < 18;" +
            "E = STORE D into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCross );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    
    @Test
    public void testFilterCross2() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cross A, B;" +
            "D = filter C by $1 < 18 and $5 < 18;" +
            "E = STORE D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator op = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  op instanceof LOCross );
        op = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  op instanceof LOCross );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
    }
    
    @Test
    public void testFilterConstantConditionCross() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cross A, B;" +
            "D = filter C by 1 == 1;" +
            "E = STORE D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) ) {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }
        Operator op = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCross );
    }
    
    @Test
    public void testFilterUDFCross() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cross A, B;" +
            "D = filter C by " + MyFilterFunc.class.getName() + "($0) ;" +
            "E = STORE D into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCross );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    @Test
    public void testFilterCogroup() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cogroup A by $0, B by $0;" +
            "D = filter C by $0 < 'name';" +
            "E = STORE D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) )  {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator filterA = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  filterA instanceof LOFilter );
        
        Operator filterB = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  filterB instanceof LOFilter );
        
        Operator cogrpA = newLogicalPlan.getSuccessors( filterA ).get( 0 );
        Assert.assertTrue(  cogrpA instanceof LOCogroup );
        Operator cogrpB = newLogicalPlan.getSuccessors( filterB ).get( 0 );
        Assert.assertTrue(  cogrpB instanceof LOCogroup );
        Assert.assertTrue(  cogrpB == cogrpA );
        
        Operator store = newLogicalPlan.getSuccessors(cogrpA).get( 0 );
        Assert.assertTrue(  store instanceof LOStore );
    }
    
    @Test
    public void testFilterConstantConditionCogroup() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cogroup A by $0, B by $0;" +
            "D = filter C by 1 == 1;" +
            "E = STORE D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) )  {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator filterA = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  filterA instanceof LOFilter );
        
        Operator filterB = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  filterB instanceof LOFilter );
        
        Operator cogrpA = newLogicalPlan.getSuccessors( filterA ).get( 0 );
        Assert.assertTrue(  cogrpA instanceof LOCogroup );
        Operator cogrpB = newLogicalPlan.getSuccessors( filterB ).get( 0 );
        Assert.assertTrue(  cogrpB instanceof LOCogroup );
        Assert.assertTrue(  cogrpB == cogrpA );
        
        Operator store = newLogicalPlan.getSuccessors(cogrpA).get( 0 );
        Assert.assertTrue(  store instanceof LOStore );
    }
    
    
    @Test
    public void testFilterUDFCogroup() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cogroup A by $0, B by $0;" +
            "D = filter C by " + MyFilterFunc.class.getName() + "($1) ;" +
            "E = STORE D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) )  {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator cogroupA = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  cogroupA instanceof LOCogroup );
        
        Operator cogroupB = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  cogroupB instanceof LOCogroup );
        
        Operator filter = newLogicalPlan.getSuccessors( cogroupA ).get( 0 );
        Assert.assertTrue(  filter instanceof LOFilter );
        filter = newLogicalPlan.getSuccessors( cogroupB ).get( 0 );
        Assert.assertTrue(  cogroupB instanceof LOCogroup );
        Assert.assertTrue(  cogroupB == cogroupA );
        
        Operator store = newLogicalPlan.getSuccessors(filter).get( 0 );
        Assert.assertTrue(  store instanceof LOStore );
    }

    @Test
    public void testFilterCogroupOuter() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cogroup A by $0, B by $0 outer;" +
            "D = filter C by $0 < 'name';" +
            "E = STORE D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) )  {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator filterA = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  filterA instanceof LOFilter );
        
        Operator filterB = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  filterB instanceof LOFilter );
        
        Operator cogrpA = newLogicalPlan.getSuccessors( filterA ).get( 0 );
        Assert.assertTrue(  cogrpA instanceof LOCogroup );
        Operator cogrpB = newLogicalPlan.getSuccessors( filterB ).get( 0 );
        Assert.assertTrue(  cogrpB instanceof LOCogroup );
        Assert.assertTrue(  cogrpB == cogrpA );
        
        Operator store = newLogicalPlan.getSuccessors(cogrpA).get( 0 );
        Assert.assertTrue(  store instanceof LOStore );
    }
    
    @Test
    public void testFilterConstantConditionCogroupOuter() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cogroup A by $0, B by $0 outer;" +
            "D = filter C by 1 == 1;" +
            "E = STORE D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) )  {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator filterA = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  filterA instanceof LOFilter );
        
        Operator filterB = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  filterB instanceof LOFilter );
        
        Operator cogrpA = newLogicalPlan.getSuccessors( filterA ).get( 0 );
        Assert.assertTrue(  cogrpA instanceof LOCogroup );
        Operator cogrpB = newLogicalPlan.getSuccessors( filterB ).get( 0 );
        Assert.assertTrue(  cogrpB instanceof LOCogroup );
        Assert.assertTrue(  cogrpB == cogrpA );
        
        Operator store = newLogicalPlan.getSuccessors(cogrpA).get( 0 );
        Assert.assertTrue(  store instanceof LOStore );
    }
    
    @Test
    public void testFilterUDFCogroupOuter() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = cogroup A by $0, B by $0 outer;" +
            "D = filter C by " + MyFilterFunc.class.getName() + "() ;" +
            "E = STORE D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator loadA = null;
        Operator loadB = null;
        if( ((LOLoad)loads.get( 0 )).getAlias().equals( "A" ) )  {
            loadA = loads.get( 0 );
            loadB = loads.get( 1 );
        } else {
            loadA = loads.get( 1 );
            loadB = loads.get( 0 );
        }

        Operator cogroupA = newLogicalPlan.getSuccessors(loadA).get( 0 );
        Assert.assertTrue(  cogroupA instanceof LOCogroup );
        
        Operator cogroupB = newLogicalPlan.getSuccessors(loadB).get( 0 );
        Assert.assertTrue(  cogroupB instanceof LOCogroup );
        
        Operator filter = newLogicalPlan.getSuccessors( cogroupA ).get( 0 );
        Assert.assertTrue(  filter instanceof LOFilter );
        filter = newLogicalPlan.getSuccessors( cogroupB ).get( 0 );
        Assert.assertTrue(  filter instanceof LOFilter );
        Assert.assertTrue(  cogroupB == cogroupA );
        
        Operator store = newLogicalPlan.getSuccessors(filter).get( 0 );
        Assert.assertTrue(  store instanceof LOStore );
    }
    
    @Test
    public void testFilterGroupBy() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = group A by $0;" +
            "C = filter B by $0 < 'name';" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCogroup );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    @Test
    public void testFilterConstantConditionGroupBy() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = group A by $0;" +
            "C = filter B by 1 == 1;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCogroup );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    @Test
    public void testFilterUDFGroupBy() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = group A by $0;" +
            "C = filter B by " + MyFilterFunc.class.getName() + "($1) ;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCogroup );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    @Test
    public void testFilterGroupByOuter() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = group A by $0 outer;" +
            "C = filter B by $0 < 'name';" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCogroup );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    @Test
    public void testFilterConstantConditionGroupByOuter() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = group A by $0 outer;" +
            "C = filter B by 1 == 1;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCogroup );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    @Test
    public void testFilterUDFGroupByOuter() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = group A by $0 outer;" +
            "C = filter B by " + MyFilterFunc.class.getName() + "($1) ;" +
            "D = STORE C into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator op = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue(  op instanceof LOLoad );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOCogroup );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }

    @Test
    public void testFilterFRJoin() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = join A by $0, B by $0 using 'replicated';" +
            "D = filter C by $0 < 'name';" +
            "E = limit D 10;" +
            "F = STORE E into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOLimit );
    }
    
    @Test
    public void testFilterFRJoin1() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = join A by $0, B by $0 using 'replicated';" +
            "D = filter C by $4 < 'name';" +
            "E = limit D 10;" +
            "F = STORE E into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOLimit );
    }
    
    @Test
    // Constant filter condition, the filter will be pushed up to the first branch of join.
    public void testFilterConstantConditionFRJoin() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = join A by $0, B by $0 using 'replicated';" +
            "D = filter C by 1 == 1;" +
            "E = STORE D into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    @Test
    // UDF takes on argument, so it's constant. As a result, filter will pushed up to the first branch of the join.
    public void testFilterUDFFRJoin() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = join A by $0, B by $0 using 'replicated';" +
            "D = filter C by " + MyFilterFunc.class.getName() + "();" +
            "E = STORE D into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    @Test
    // UDF takes all input, so filter connot be pushed up.
    public void testFilterUDFFRJoin1() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = join A by $0, B by $0 using 'replicated';" +
            "D = filter C by TupleSize(*) > 5;" +
            "E = STORE D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );
        Operator op = newLogicalPlan.getSuccessors( loads.get( 0 ) ).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        
        op = newLogicalPlan.getSuccessors( loads.get( 1 ) ).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
    }

    @Test
    public void testFilterInnerJoin() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = join A by $0, B by $0;" +
            "D = filter C by $0 < 'name';" +
            "E = limit D 10;" +
            "F = STORE E into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOLimit );
    }

    @Test
    public void testFilterInnerJoin1() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = join A by $0, B by $0;" +
            "D = filter C by $4 < 'name';" +
            "E = limit D 10;" +
            "F = STORE E into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOLimit );
    }

    @Test
    public void testFilterInnerJoin2() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +  
            "C = join A by $0, B by $0;" +
            "D = filter C by $0 < 'jonh' OR $1 > 50;" +
            "E = limit D 10;" +
            "F = STORE E into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOLimit );
    }

    @Test
    public void testFilterInnerJoinNegative() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = join A by $0, B by $0;" +
            "D = filter C by $4 < 'name' AND $0 == 'joe';" +
            "E = limit D 10;" +
            "F = STORE E into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        
        List<Operator> loads = newLogicalPlan.getSources();
        Assert.assertTrue( loads.size() == 2 );
        Assert.assertTrue( loads.get( 0 ) instanceof LOLoad );
        Assert.assertTrue( loads.get( 1 ) instanceof LOLoad );

        Operator op = newLogicalPlan.getSuccessors( loads.get( 0 ) ).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors( loads.get( 1 ) ).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
    }

    @Test
    public void testFilterUDFInnerJoin() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name, age, preference);" +
            "C = join A by $0, B by $0;" +
            "D = filter C by " + MyFilterFunc.class.getName() + "() ;" +
            "E = STORE D into 'dummy';";
        
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

        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOJoin );
        op = newLogicalPlan.getSuccessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOStore );
    }
    
    // See PIG-1289
    @Test
    public void testOutJoin() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
            "B = load 'anotherfile' as (name);" +
            "C = join A by name LEFT OUTER, B by name;" +
            "D = filter C by B::name is null;" +
            "store D into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan(query);
        
        Operator op = newLogicalPlan.getSinks().get(0);
        Assert.assertTrue( op instanceof LOStore );
        op = newLogicalPlan.getPredecessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
    }
    
    // See PIG-1507
    @Test
    public void testFullOutJoin() throws Exception {
        String query = "A = load 'myfile' as (d1:int);" +
            "B = load 'anotherfile' as (d2:int);" +
            "c = join A by d1 full outer, B by d2;" +
            "d = filter c by d2 is null;" +
            "store d into 'dummy';";
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan(query);
        
        Operator op = newLogicalPlan.getSinks().get(0);
        Assert.assertTrue( op instanceof LOStore );
        op = newLogicalPlan.getPredecessors(op).get( 0 );
        Assert.assertTrue(  op instanceof LOFilter );
    }

    /**
     * Cascading joins. First one has outer, but filter should be able to push up above the second one, but below
     * the first one.
     */
    @Test
    public void testFullOutJoin1() throws Exception {
        String query = "A = load 'myfile' as (d1:int);" +
            "B = load 'anotherfile' as (d2:int);" +
            "C = join A by d1 full outer, B by d2;" +
            "D = load 'xxx' as (d3:int);" +
            "E = join C by d1, D by d3;" +
            "F = filter E by d1 > 5;" +
            "G = store F into 'dummy';";
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan(query);
        
        List<Operator> ops = newLogicalPlan.getSinks();
        Assert.assertTrue( ops.size() == 1 );
        Operator op = ops.get( 0 );
        Assert.assertTrue( op instanceof LOStore );
        Operator join = newLogicalPlan.getPredecessors(op).get( 0 );
        Assert.assertTrue( join instanceof LOJoin );
        ops = newLogicalPlan.getPredecessors(join);
        Assert.assertEquals( 2, ops.size() );
        Assert.assertTrue( ops.get( 0 ) instanceof LOFilter || ops.get( 1 ) instanceof LOFilter );
    }

    private LogicalPlan migrateAndOptimizePlan(String query) throws Exception {
        PigServer pigServer = new PigServer( pc );
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
    }
    
    public class MyPlanOptimizer extends LogicalPlanOptimizer {
        protected MyPlanOptimizer(OperatorPlan p,  int iterations) {
            super(p, iterations, new HashSet<String>());
        }
        
        public void addPlanTransformListener(PlanTransformListener listener) {
            super.addPlanTransformListener(listener);
        }
        
       protected List<Set<Rule>> buildRuleSets() {            
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
            
            Set<Rule> s = new HashSet<Rule>();
            // add split filter rule
            Rule r = new LoadTypeCastInserter( "TypeCastInserter" );
            s.add(r);
            ls.add(s);
             
            s = new HashSet<Rule>();
            r = new PushUpFilter( "PushUpFilter" );
            s.add(r);    
            ls.add(s);
            
            return ls;
        }
    }    
}

