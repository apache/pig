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
import java.util.List;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GreaterThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LessThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.LogicalPlanMigrationVistor;
import org.apache.pig.experimental.logical.expression.LogicalExpression;
import org.apache.pig.experimental.logical.optimizer.UidStamper;
import org.apache.pig.experimental.logical.relational.LogToPhyTranslationVisitor;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.test.utils.LogicalPlanTester;

import junit.framework.TestCase;

public class TestExperimentalLogToPhyTranslationVisitor extends TestCase {

    private PhysicalPlan translatePlan(OperatorPlan plan) throws IOException {
        LogToPhyTranslationVisitor visitor = new LogToPhyTranslationVisitor(plan);
        visitor.visit();
        return visitor.getPhysicalPlan();
    }
    
    private org.apache.pig.experimental.logical.relational.LogicalPlan migratePlan(LogicalPlan lp) throws VisitorException{
        LogicalPlanMigrationVistor visitor = new LogicalPlanMigrationVistor(lp);        
        visitor.visit();
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = visitor.getNewLogicalPlan();
        
        try {
            UidStamper stamper = new UidStamper(newPlan);
            stamper.visit();
            
            return newPlan;
        }catch(Exception e) {
            throw new VisitorException(e);
        }
    }
    
    protected void setUp() throws Exception {    
        LogicalExpression.resetNextUid();
    }
    
    public void testSimplePlan() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'd.txt';");
        lpt.buildPlan("b = filter a by $0==NULL;");        
        LogicalPlan plan = lpt.buildPlan("store b into 'empty';");  
        
        org.apache.pig.experimental.logical.relational.LogicalPlan newLogicalPlan = migratePlan(plan);
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        assertEquals( 3, phyPlan.size() );
        assertEquals( 1, phyPlan.getRoots().size() );
        assertEquals( 1, phyPlan.getLeaves().size() );
        
        PhysicalOperator load = phyPlan.getRoots().get(0);
        assertEquals( POLoad.class, load.getClass() );
        assertTrue(  ((POLoad)load).getLFile().getFileName().contains("d.txt") );
        
        // Check for Filter
        PhysicalOperator fil = phyPlan.getSuccessors(load).get(0);
        assertEquals( POFilter.class, fil.getClass() );
        PhysicalPlan filPlan = ((POFilter)fil).getPlan();
        assertEquals( 2, filPlan.getRoots().size() );
        assertEquals( 1, filPlan.getLeaves().size() );
        
        PhysicalOperator eq = filPlan.getLeaves().get(0);
        assertEquals( EqualToExpr.class, eq.getClass() );
        
        PhysicalOperator prj1 = filPlan.getRoots().get(0);
        assertEquals( POProject.class, prj1.getClass() );
        assertEquals( 0, ((POProject)prj1).getColumn() );
        PhysicalOperator constExp = filPlan.getRoots().get(1);
        assertEquals( ConstantExpression.class, constExp.getClass() );
        assertEquals( null, ((ConstantExpression)constExp).getValue() );
        
        // Check for Store
        PhysicalOperator stor = phyPlan.getSuccessors(fil).get(0);
        assertEquals( POStore.class, stor.getClass() );
        assertTrue(  ((POStore)stor).getSFile().getFileName().contains("empty"));
    }
    
    public void testJoinPlan() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'd1.txt' as (id, c);");
        lpt.buildPlan("b = load 'd2.txt'as (id, c);");
        lpt.buildPlan("c = join a by id, b by c;");
        lpt.buildPlan("d = filter c by a::id==NULL AND b::c==NULL;");        
        LogicalPlan plan = lpt.buildPlan("store d into 'empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        PhysicalPlan physicalPlan = translatePlan(newPlan);
        assertEquals(9, physicalPlan.size());
        assertEquals(physicalPlan.getRoots().size(), 2);
        
        // Check Load and LocalRearrange and GlobalRearrange
        PhysicalOperator LoR = (PhysicalOperator)physicalPlan.getSuccessors(physicalPlan.getRoots().get(0)).get(0);
        assertEquals( POLocalRearrange.class, LoR.getClass() );
        POLocalRearrange Lor = (POLocalRearrange) LoR;
        PhysicalOperator prj3 = Lor.getPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj3.getClass() );
        assertEquals(0, ((POProject)prj3).getColumn() );
        PhysicalOperator inp1 = Lor.getInputs().get(0);
        assertEquals( POLoad.class, inp1.getClass() );
        assertTrue(  ((POLoad)inp1).getLFile().getFileName().contains("d1.txt") );
                
        PhysicalOperator LoR1 = (PhysicalOperator)physicalPlan.getSuccessors(physicalPlan.getRoots().get(1)).get(0);
        assertEquals( POLocalRearrange.class, LoR1.getClass() );
        POLocalRearrange Lor1 = (POLocalRearrange) LoR1;
        PhysicalOperator prj4 = Lor1.getPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj4.getClass() );
        assertEquals(1, ((POProject)prj4).getColumn() );
        PhysicalOperator inp2 = Lor1.getInputs().get(0);
        assertEquals( POLoad.class, inp2.getClass() );
        assertTrue(  ((POLoad)inp2).getLFile().getFileName().contains("d2.txt") );
        
        PhysicalOperator GoR = (PhysicalOperator)physicalPlan.getSuccessors(LoR).get(0);
        assertEquals( POGlobalRearrange.class, GoR.getClass() );
        
        PhysicalOperator Pack = (PhysicalOperator)physicalPlan.getSuccessors(GoR).get(0);
        assertEquals( POPackage.class, Pack.getClass() );

        // Check for ForEach
        PhysicalOperator ForE = (PhysicalOperator)physicalPlan.getSuccessors(Pack).get(0);
        assertEquals( POForEach.class, ForE.getClass() );
        PhysicalOperator prj5 = ((POForEach)ForE).getInputPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj5.getClass() );
        assertEquals( 1, ((POProject)prj5).getColumn() ); 
        PhysicalOperator prj6 = ((POForEach)ForE).getInputPlans().get(1).getLeaves().get(0);
        assertEquals( POProject.class, prj6.getClass() );
        assertEquals( 2, ((POProject)prj6).getColumn() );
        
        // Filter Operator
        PhysicalOperator fil = (PhysicalOperator)physicalPlan.getSuccessors(ForE).get(0);
        assertEquals( POFilter.class, fil.getClass() );        
        
        PhysicalPlan filPlan = ((POFilter)fil).getPlan();
        List<PhysicalOperator> filRoots = filPlan.getRoots();
        
        assertEquals( ConstantExpression.class, filRoots.get(1).getClass() );
        ConstantExpression ce1 = (ConstantExpression) filRoots.get(1);
        assertEquals( null, ce1.getValue() ); 
        assertEquals( ConstantExpression.class, filRoots.get(3).getClass() );
        ConstantExpression ce2 = (ConstantExpression) filRoots.get(3);
        assertEquals( null, ce2.getValue() );
        assertEquals( POProject.class, filRoots.get(0).getClass() );
        POProject prj1 = (POProject) filRoots.get(0);
        assertEquals( 3, prj1.getColumn() );
        assertEquals( POProject.class, filRoots.get(2).getClass() );
        POProject prj2 = (POProject) filRoots.get(2);
        assertEquals( 0, prj2.getColumn() );


        // Check Store Operator
        PhysicalOperator stor = (PhysicalOperator)physicalPlan.getSuccessors(fil).get(0);
        assertEquals( POStore.class, stor.getClass() );
        assertTrue(  ((POStore)stor).getSFile().getFileName().contains("empty") );
    }
    
    public void testMultiStore() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'd1.txt' as (id, c);");
        lpt.buildPlan("b = load 'd2.txt'as (id, c);");
        lpt.buildPlan("c = load 'd3.txt' as (id, c);");
        lpt.buildPlan("d = join a by id, b by c;");        
        lpt.buildPlan("e = filter d by a::id==NULL AND b::c==NULL;");
        lpt.buildPlan("f = join e by b::c, c by id;");
        lpt.buildPlan("g = filter f by b::id==NULL AND c::c==NULL;");
        LogicalPlan plan = lpt.buildPlan("store g into 'empty2';");        
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newLogicalPlan = migratePlan(plan);
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        assertEquals(16, phyPlan.size());
        assertEquals(phyPlan.getRoots().size(), 3);
        assertEquals(phyPlan.getLeaves().size(), 1 );

        // Check Load and LocalRearrange and GlobalRearrange
        PhysicalOperator LoR = (PhysicalOperator)phyPlan.getSuccessors(phyPlan.getRoots().get(0)).get(0);
        assertEquals( POLocalRearrange.class, LoR.getClass() );
        POLocalRearrange Lor = (POLocalRearrange) LoR;
        PhysicalOperator prj1 = Lor.getPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj1.getClass() );
        assertEquals(0, ((POProject)prj1).getColumn() );
        PhysicalOperator inp1 = Lor.getInputs().get(0);
        assertEquals( POLoad.class, inp1.getClass() );
        assertTrue(  ((POLoad)inp1).getLFile().getFileName().contains("d3.txt") );

        PhysicalOperator LoR1 = (PhysicalOperator)phyPlan.getSuccessors(phyPlan.getRoots().get(1)).get(0);
        assertEquals( POLocalRearrange.class, LoR1.getClass() );
        POLocalRearrange Lor1 = (POLocalRearrange) LoR1;
        PhysicalOperator prj2 = Lor1.getPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj2.getClass() );
        assertEquals(1, ((POProject)prj2).getColumn() );
        PhysicalOperator inp2 = Lor1.getInputs().get(0);
        assertEquals( POLoad.class, inp2.getClass() );
        assertTrue(  ((POLoad)inp2).getLFile().getFileName().contains("d2.txt") );
        
        PhysicalOperator GoR = (PhysicalOperator)phyPlan.getSuccessors(LoR).get(0);
        assertEquals( POGlobalRearrange.class, GoR.getClass() );
        
        PhysicalOperator Pack = (PhysicalOperator)phyPlan.getSuccessors(GoR).get(0);
        assertEquals( POPackage.class, Pack.getClass() );
        
        PhysicalOperator LoR2 = (PhysicalOperator)phyPlan.getSuccessors(phyPlan.getRoots().get(2)).get(0);
        assertEquals( POLocalRearrange.class, LoR2.getClass() );
        POLocalRearrange Lor2 = (POLocalRearrange) LoR2;
        PhysicalOperator prj3 = Lor2.getPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj3.getClass() );
        assertEquals(0, ((POProject)prj3).getColumn() );
        PhysicalOperator inp3 = Lor2.getInputs().get(0);
        assertEquals( POLoad.class, inp3.getClass() );
        assertTrue(  ((POLoad)inp3).getLFile().getFileName().contains("d1.txt") );
        
        PhysicalOperator GoR2 = (PhysicalOperator)phyPlan.getSuccessors(LoR2).get(0);
        assertEquals( POGlobalRearrange.class, GoR2.getClass() );
        
        PhysicalOperator Pack2 = (PhysicalOperator)phyPlan.getSuccessors(GoR2).get(0);
        assertEquals( POPackage.class, Pack2.getClass() );
        
        // Check for ForEach
        PhysicalOperator ForE = (PhysicalOperator)phyPlan.getSuccessors(Pack).get(0);
        assertEquals( POForEach.class, ForE.getClass() );
        PhysicalOperator prj4 = ((POForEach)ForE).getInputPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj4.getClass() );
        assertEquals( 1, ((POProject)prj4).getColumn() ); 
        PhysicalOperator prj5 = ((POForEach)ForE).getInputPlans().get(1).getLeaves().get(0);
        assertEquals( POProject.class, prj5.getClass() );
        assertEquals( 2, ((POProject)prj5).getColumn() );
        
        PhysicalOperator ForE2 = (PhysicalOperator)phyPlan.getSuccessors(Pack2).get(0);
        assertEquals( POForEach.class, ForE2.getClass() );
        PhysicalOperator prj6 = ((POForEach)ForE2).getInputPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj6.getClass() );
        assertEquals( 1, ((POProject)prj6).getColumn() ); 
        PhysicalOperator prj7 = ((POForEach)ForE2).getInputPlans().get(1).getLeaves().get(0);
        assertEquals( POProject.class, prj7.getClass() );
        assertEquals( 2, ((POProject)prj7).getColumn() );
        
        // Check Filter Operator
        PhysicalOperator fil = (PhysicalOperator)phyPlan.getSuccessors(ForE).get(0);
        assertEquals( POFilter.class, fil.getClass() );
        
        PhysicalPlan filPlan = ((POFilter)fil).getPlan();
        List<PhysicalOperator> filRoots = filPlan.getRoots();
        
        assertEquals( ConstantExpression.class, filRoots.get(0).getClass() );
        ConstantExpression ce1 = (ConstantExpression) filRoots.get(0);
        assertEquals( null, ce1.getValue() ); 
        assertEquals( ConstantExpression.class, filRoots.get(2).getClass() );
        ConstantExpression ce2 = (ConstantExpression) filRoots.get(2);
        assertEquals( null, ce2.getValue() );
        assertEquals( POProject.class, filRoots.get(1).getClass() );
        POProject prj8 = (POProject) filRoots.get(1);
        assertEquals( 5, prj8.getColumn() );
        assertEquals( POProject.class, filRoots.get(3).getClass() );
        POProject prj9 = (POProject) filRoots.get(3);
        assertEquals( 2, prj9.getColumn() );
        
        
        PhysicalOperator fil2 = (PhysicalOperator)phyPlan.getSuccessors(ForE2).get(0);
        assertEquals( POFilter.class, fil2.getClass() );
        
        PhysicalOperator LoR3 = (PhysicalOperator)phyPlan.getSuccessors(fil2).get(0);
        assertEquals( POLocalRearrange.class, LoR3.getClass() );
        POLocalRearrange Lor3 = (POLocalRearrange) LoR3;
        PhysicalOperator prj12 = Lor3.getPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj12.getClass() );
        assertEquals(3, ((POProject)prj12).getColumn() );
        
        PhysicalPlan filPlan2 = ((POFilter)fil2).getPlan();
        List<PhysicalOperator> filRoots2 = filPlan2.getRoots();
        
        assertEquals( ConstantExpression.class, filRoots2.get(0).getClass() );
        ConstantExpression ce3 = (ConstantExpression) filRoots2.get(0);
        assertEquals( null, ce3.getValue() ); 
        assertEquals( ConstantExpression.class, filRoots2.get(2).getClass() );
        ConstantExpression ce4 = (ConstantExpression) filRoots2.get(2);
        assertEquals( null, ce4.getValue() );
        assertEquals( POProject.class, filRoots2.get(1).getClass() );
        POProject prj10 = (POProject) filRoots2.get(1);
        assertEquals( 3, prj10.getColumn() );
        assertEquals( POProject.class, filRoots2.get(3).getClass() );
        POProject prj11 = (POProject) filRoots2.get(3);
        assertEquals( 0, prj11.getColumn() );
        
        // Check Store Operator
        PhysicalOperator stor = (PhysicalOperator)phyPlan.getLeaves().get(0);
        assertEquals( stor, phyPlan.getSuccessors(fil).get(0));
        assertEquals( POStore.class, stor.getClass() );
        assertTrue(  ((POStore)stor).getSFile().getFileName().contains("empty") );
    }
    
    public void testPlanWithCast() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'd.txt' as (id, c);");
        lpt.buildPlan("b = filter a by (int)id==10;");        
        LogicalPlan plan = lpt.buildPlan("store b into 'empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newLogicalPlan = migratePlan(plan);
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        assertEquals(3, phyPlan.size());
        assertEquals(phyPlan.getRoots().size(), 1);
        assertEquals(phyPlan.getLeaves().size(), 1 );
        
        PhysicalOperator load = phyPlan.getRoots().get(0);
        assertEquals( POLoad.class, load.getClass() );
        assertTrue(  ((POLoad)load).getLFile().getFileName().contains("d.txt"));
        
        PhysicalOperator fil = phyPlan.getSuccessors(load).get(0);
        assertEquals( POFilter.class, fil.getClass() );
        PhysicalPlan filPlan = ((POFilter)fil).getPlan();
        
        PhysicalOperator equal = filPlan.getLeaves().get(0);
        assertEquals( EqualToExpr.class, equal.getClass() );
        assertEquals( DataType.BOOLEAN, ((EqualToExpr)equal).getResultType() );
        
        PhysicalOperator constExpr = ((EqualToExpr)equal).getRhs();
        assertEquals( ConstantExpression.class, constExpr.getClass() );
        assertEquals( 10, ((ConstantExpression)constExpr).getValue() );
        
        PhysicalOperator castExpr = ((EqualToExpr)equal).getLhs();
        assertEquals( POCast.class, castExpr.getClass() );
        assertEquals( DataType.INTEGER, ((POCast)castExpr).getResultType() );
        
        PhysicalOperator prj = ((POCast)castExpr).getInputs().get(0);
        assertEquals( POProject.class, prj.getClass() );
        assertEquals( 0, ((POProject)prj).getColumn() );
        
        PhysicalOperator stor = phyPlan.getLeaves().get(0);
        assertEquals( POStore.class, stor.getClass() );
        assertTrue( ((POStore)stor).getSFile().getFileName().contains( "empty" ) );        
    }
    
    public void testPlanWithGreaterThan() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'd.txt' as (id, c);");
        lpt.buildPlan("b = filter a by (int)id>10;");        
        LogicalPlan plan = lpt.buildPlan("store b into 'empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newLogicalPlan = migratePlan(plan);
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        assertEquals(3, phyPlan.size());
        assertEquals(phyPlan.getRoots().size(), 1);
        assertEquals(phyPlan.getLeaves().size(), 1 );
        
        PhysicalOperator load = phyPlan.getRoots().get(0);
        assertEquals( POLoad.class, load.getClass() );
        assertTrue(  ((POLoad)load).getLFile().getFileName().contains("d.txt"));
        
        PhysicalOperator fil = phyPlan.getSuccessors(load).get(0);
        assertEquals( POFilter.class, fil.getClass() );
        PhysicalPlan filPlan = ((POFilter)fil).getPlan();
        
        PhysicalOperator greaterThan = filPlan.getLeaves().get(0);
        assertEquals( GreaterThanExpr.class, greaterThan.getClass() );
        assertEquals( DataType.BOOLEAN, ((GreaterThanExpr)greaterThan).getResultType() );
        
        PhysicalOperator constExpr = ((GreaterThanExpr)greaterThan).getRhs();
        assertEquals( ConstantExpression.class, constExpr.getClass() );
        assertEquals( 10, ((ConstantExpression)constExpr).getValue() );
        
        PhysicalOperator castExpr = ((GreaterThanExpr)greaterThan).getLhs();
        assertEquals( POCast.class, castExpr.getClass() );
        assertEquals( DataType.INTEGER, ((POCast)castExpr).getResultType() );
        
        PhysicalOperator prj = ((POCast)castExpr).getInputs().get(0);
        assertEquals( POProject.class, prj.getClass() );
        assertEquals( 0, ((POProject)prj).getColumn() );
        
        PhysicalOperator stor = phyPlan.getLeaves().get(0);
        assertEquals( POStore.class, stor.getClass() );
        assertTrue( ((POStore)stor).getSFile().getFileName().contains( "empty" ) );  
    }
    
    public void testPlanWithLessThan() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'd.txt' as (id, c);");
        lpt.buildPlan("b = filter a by (int)id<10;");
        LogicalPlan plan = lpt.buildPlan("store b into 'empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newLogicalPlan = migratePlan(plan);
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        assertEquals(3, phyPlan.size());
        assertEquals(phyPlan.getRoots().size(), 1);
        assertEquals(phyPlan.getLeaves().size(), 1 );
        
        PhysicalOperator load = phyPlan.getRoots().get(0);
        assertEquals( POLoad.class, load.getClass() );
        assertTrue(  ((POLoad)load).getLFile().getFileName().contains("d.txt"));
        
        PhysicalOperator fil = phyPlan.getSuccessors(load).get(0);
        assertEquals( POFilter.class, fil.getClass() );
        PhysicalPlan filPlan = ((POFilter)fil).getPlan();
        
        PhysicalOperator lessThan = filPlan.getLeaves().get(0);
        assertEquals( LessThanExpr.class, lessThan.getClass() );
        assertEquals( DataType.BOOLEAN, ((LessThanExpr)lessThan).getResultType() );
        
        PhysicalOperator constExpr = ((LessThanExpr)lessThan).getRhs();
        assertEquals( ConstantExpression.class, constExpr.getClass() );
        assertEquals( 10, ((ConstantExpression)constExpr).getValue() );
        
        PhysicalOperator castExpr = ((LessThanExpr)lessThan).getLhs();
        assertEquals( POCast.class, castExpr.getClass() );
        assertEquals( DataType.INTEGER, ((POCast)castExpr).getResultType() );
        
        PhysicalOperator prj = ((POCast)castExpr).getInputs().get(0);
        assertEquals( POProject.class, prj.getClass() );
        assertEquals( 0, ((POProject)prj).getColumn() );
        
        PhysicalOperator stor = phyPlan.getLeaves().get(0);
        assertEquals( POStore.class, stor.getClass() );
        assertTrue( ((POStore)stor).getSFile().getFileName().contains( "empty" ) );  
    }

    public void testForeachPlan() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'd.txt' as (id, c);");
        lpt.buildPlan("b = foreach a generate id, c;");        
        LogicalPlan plan = lpt.buildPlan("store b into 'empty';");  
        
        org.apache.pig.experimental.logical.relational.LogicalPlan newLogicalPlan = migratePlan(plan);
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        assertEquals(phyPlan.size(), 3);
        POLoad load = (POLoad)phyPlan.getRoots().get(0);        
        assertEquals(phyPlan.getLeaves().get(0).getClass(), POStore.class);
        POForEach foreach = (POForEach)phyPlan.getSuccessors(phyPlan.getRoots().get(0)).get(0);
        
        assertEquals(foreach.getInputPlans().size(), 2);
        
        PhysicalPlan inner = foreach.getInputPlans().get(0);
        assertEquals(inner.size(), 1);
        POProject prj = (POProject)inner.getRoots().get(0);
        assertEquals(prj.getColumn(), 0);
        assertEquals(prj.getInputs().get(0), load);
        
        inner = foreach.getInputPlans().get(1);
        assertEquals(inner.size(), 1);
        prj = (POProject)inner.getRoots().get(0);
        assertEquals(prj.getColumn(), 1);
        assertEquals(prj.getInputs().get(0), load);
        Boolean[] flat = foreach.getToBeFlattened().toArray(new Boolean[0]);
        assertFalse(flat[0]);
        assertFalse(flat[1]);
    }
    
    public void testForeachPlan2() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester();
        lpt.buildPlan("a = load 'd.txt' as (id, c:bag{t:(s,v)});");
        lpt.buildPlan("b = foreach a generate id, flatten(c);");        
        LogicalPlan plan = lpt.buildPlan("store b into 'empty';");  
        
        org.apache.pig.experimental.logical.relational.LogicalPlan newLogicalPlan = migratePlan(plan);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        LogicalRelationalOperator fe = (LogicalRelationalOperator)newLogicalPlan.getSuccessors(ld).get(0);
        LogicalSchema ls = fe.getSchema();
        assertEquals(1, ls.getField(0).uid);
        assertEquals(4, ls.getField(1).uid);
        assertEquals(5, ls.getField(2).uid);
        
        LogicalSchema expected = new LogicalSchema();
        expected.addField(new LogicalFieldSchema("id", null, DataType.BYTEARRAY));
        expected.addField(new LogicalFieldSchema("s", null, DataType.BYTEARRAY));
        expected.addField(new LogicalFieldSchema("v", null, DataType.BYTEARRAY));
        assertTrue(expected.isEqual(ls));
        
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        assertEquals(phyPlan.size(), 3);
        POLoad load = (POLoad)phyPlan.getRoots().get(0);        
        assertEquals(phyPlan.getLeaves().get(0).getClass(), POStore.class);
        POForEach foreach = (POForEach)phyPlan.getSuccessors(phyPlan.getRoots().get(0)).get(0);
        
        assertEquals(foreach.getInputPlans().size(), 2);
        
        PhysicalPlan inner = foreach.getInputPlans().get(0);
        assertEquals(inner.size(), 1);
        POProject prj = (POProject)inner.getRoots().get(0);
        assertEquals(prj.getColumn(), 0);
        assertEquals(prj.getInputs().get(0), load);
        
        inner = foreach.getInputPlans().get(1);
        assertEquals(inner.size(), 1);
        prj = (POProject)inner.getRoots().get(0);
        assertEquals(prj.getColumn(), 1);
        assertEquals(prj.getInputs().get(0), load);
        Boolean[] flat = foreach.getToBeFlattened().toArray(new Boolean[0]);
        assertFalse(flat[0]);
        assertTrue(flat[1]);
    }
    
}
