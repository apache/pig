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
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Add;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Divide;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GreaterThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LessThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Mod;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Multiply;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONegative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Subtract;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class TestNewPlanLogToPhyTranslationVisitor extends TestCase {

    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    private PhysicalPlan translatePlan(OperatorPlan plan) throws IOException {
        LogToPhyTranslationVisitor visitor = new LogToPhyTranslationVisitor(plan);
        visitor.visit();
        return visitor.getPhysicalPlan();
    }
    
    private LogicalPlan buildPlan(String query)
    throws Exception{
        PigServer pigServer = new PigServer( pc );
    	return Util.buildLp(pigServer, query);
    }
    
    protected void setUp() throws Exception {    
        LogicalExpression.resetNextUid();
    }
    
    public void testSimplePlan() throws Exception {
        String query = ("a = load 'd.txt';" +
        "b = filter a by $0==NULL;" +        
        "store b into 'empty';");  
        
        org.apache.pig.newplan.logical.relational.LogicalPlan newLogicalPlan = buildPlan(query);
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
        
        PhysicalOperator prj1 = filPlan.getRoots().get(1);
        assertEquals( POProject.class, prj1.getClass() );
        assertEquals( 0, ((POProject)prj1).getColumn() );
        PhysicalOperator constExp = filPlan.getRoots().get(0);
        assertEquals( ConstantExpression.class, constExp.getClass() );
        assertEquals( null, ((ConstantExpression)constExp).getValue() );
        
        // Check for Store
        PhysicalOperator stor = phyPlan.getSuccessors(fil).get(0);
        assertEquals( POStore.class, stor.getClass() );
        assertTrue(  ((POStore)stor).getSFile().getFileName().contains("empty"));
    }
    
    public void testJoinPlan() throws Exception {
        String query = ("a = load 'd1.txt' as (id, c);" +
        "b = load 'd2.txt'as (id, c);" +
        "c = join a by id, b by c;" +
        "d = filter c by a::id==NULL AND b::c==NULL;" +        
        "store d into 'empty';");
        
        // check basics
        LogicalPlan newPlan = buildPlan(query);
        PhysicalPlan physicalPlan = translatePlan(newPlan);
        assertEquals(9, physicalPlan.size());
        assertEquals(physicalPlan.getRoots().size(), 2);
        
        // Check Load and LocalRearrange and GlobalRearrange
        PhysicalOperator LoR = (PhysicalOperator)physicalPlan.getSuccessors(physicalPlan.getRoots().get(1)).get(0);
        assertEquals( POLocalRearrange.class, LoR.getClass() );
        POLocalRearrange Lor = (POLocalRearrange) LoR;
        PhysicalOperator prj3 = Lor.getPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj3.getClass() );
        assertEquals(0, ((POProject)prj3).getColumn() );
        PhysicalOperator inp1 = Lor.getInputs().get(0);
        assertEquals( POLoad.class, inp1.getClass() );
        assertTrue(  ((POLoad)inp1).getLFile().getFileName().contains("d1.txt") );
                
        PhysicalOperator LoR1 = (PhysicalOperator)physicalPlan.getSuccessors(physicalPlan.getRoots().get(0)).get(0);
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
        
        assertEquals( ConstantExpression.class, filRoots.get(2).getClass() );
        ConstantExpression ce1 = (ConstantExpression) filRoots.get(2);
        assertEquals( null, ce1.getValue() ); 
        assertEquals( ConstantExpression.class, filRoots.get(3).getClass() );
        ConstantExpression ce2 = (ConstantExpression) filRoots.get(3);
        assertEquals( null, ce2.getValue() );
        assertEquals( POProject.class, filRoots.get(0).getClass() );
        POProject prj1 = (POProject) filRoots.get(0);
        assertEquals( 0, prj1.getColumn() );
        assertEquals( POProject.class, filRoots.get(1).getClass() );
        POProject prj2 = (POProject) filRoots.get(1);
        assertEquals( 3, prj2.getColumn() );


        // Check Store Operator
        PhysicalOperator stor = (PhysicalOperator)physicalPlan.getSuccessors(fil).get(0);
        assertEquals( POStore.class, stor.getClass() );
        assertTrue(  ((POStore)stor).getSFile().getFileName().contains("empty") );
    }
    
    public void testMultiStore() throws Exception {
        String query = ("a = load 'd1.txt' as (id, c);" +
        "b = load 'd2.txt'as (id, c);" +
        "c = load 'd3.txt' as (id, c);" +
        "d = join a by id, b by c;" +        
        "e = filter d by a::id==NULL AND b::c==NULL;" +
        "f = join e by b::c, c by id;" +
        "g = filter f by b::id==NULL AND c::c==NULL;" +
        "store g into 'empty2';");        
        
        // check basics
        LogicalPlan newLogicalPlan = buildPlan(query);
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
        assertEquals(0, ((POProject)prj2).getColumn() );
        PhysicalOperator inp2 = Lor1.getInputs().get(0);
        assertEquals( POLoad.class, inp2.getClass() );
        assertTrue(  ((POLoad)inp2).getLFile().getFileName().contains("d1.txt") );
        
        PhysicalOperator GoR = (PhysicalOperator)phyPlan.getSuccessors(LoR).get(0);
        assertEquals( POGlobalRearrange.class, GoR.getClass() );
        
        PhysicalOperator Pack = (PhysicalOperator)phyPlan.getSuccessors(GoR).get(0);
        assertEquals( POPackage.class, Pack.getClass() );
        
        PhysicalOperator LoR2 = (PhysicalOperator)phyPlan.getSuccessors(phyPlan.getRoots().get(2)).get(0);
        assertEquals( POLocalRearrange.class, LoR2.getClass() );
        POLocalRearrange Lor2 = (POLocalRearrange) LoR2;
        PhysicalOperator prj3 = Lor2.getPlans().get(0).getLeaves().get(0);
        assertEquals( POProject.class, prj3.getClass() );
        assertEquals(1, ((POProject)prj3).getColumn() );
        PhysicalOperator inp3 = Lor2.getInputs().get(0);
        assertEquals( POLoad.class, inp3.getClass() );
        assertTrue(  ((POLoad)inp3).getLFile().getFileName().contains("d2.txt") );
        
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
        String query = ("a = load 'd.txt' as (id, c);" +
        "b = filter a by (int)id==10;" +        
        "store b into 'empty';");
        
        // check basics
        LogicalPlan newLogicalPlan = buildPlan(query);
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
        String query = ("a = load 'd.txt' as (id, c);" +
        "b = filter a by (int)id>10;" +        
        "store b into 'empty';");
        
        // check basics
        LogicalPlan newLogicalPlan = buildPlan(query);
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
        String query = ("a = load 'd.txt' as (id, c);" +
        "b = filter a by (int)id<10;" +
        "store b into 'empty';");
        
        // check basics
        LogicalPlan newLogicalPlan = buildPlan(query);
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
        String query = ("a = load 'd.txt' as (id, c);" +
        "b = foreach a generate id, c;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        assertEquals(phyPlan.size(), 3);
        assertEquals(phyPlan.getLeaves().get(0).getClass(), POStore.class);
        POForEach foreach = (POForEach)phyPlan.getSuccessors(phyPlan.getRoots().get(0)).get(0);
        
        assertEquals(foreach.getInputPlans().size(), 2);
        
        PhysicalPlan inner = foreach.getInputPlans().get(0);
        assertEquals(inner.size(), 1);
        POProject prj = (POProject)inner.getRoots().get(0);
        assertEquals(prj.getColumn(), 0);
        assertEquals(prj.getInputs(), null);

        inner = foreach.getInputPlans().get(1);
        assertEquals(inner.size(), 1);
        prj = (POProject)inner.getRoots().get(0);
        assertEquals(prj.getColumn(), 1);
        assertEquals(prj.getInputs(), null);
        Boolean[] flat = foreach.getToBeFlattened().toArray(new Boolean[0]);
        assertFalse(flat[0]);
        assertFalse(flat[1]);
    }
    
    public void testForeachPlan2() throws Exception {
        String query = ("a = load 'd.txt' as (id, c:bag{t:(s,v)});" +
        "b = foreach a generate id, flatten(c);" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
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
        assertEquals(phyPlan.getLeaves().get(0).getClass(), POStore.class);
        POForEach foreach = (POForEach)phyPlan.getSuccessors(phyPlan.getRoots().get(0)).get(0);
        
        assertEquals(foreach.getInputPlans().size(), 2);
        
        PhysicalPlan inner = foreach.getInputPlans().get(0);
        assertEquals(inner.size(), 1);
        POProject prj = (POProject)inner.getRoots().get(0);
        assertEquals(prj.getColumn(), 0);
        assertEquals(prj.getInputs(), null);
        
        inner = foreach.getInputPlans().get(1);
        assertEquals(inner.size(), 1);
        prj = (POProject)inner.getRoots().get(0);
        assertEquals(prj.getColumn(), 1);
        assertEquals(prj.getInputs(), null);
        Boolean[] flat = foreach.getToBeFlattened().toArray(new Boolean[0]);
        assertFalse(flat[0]);
        assertTrue(flat[1]);
    }
    
    public void testPlanwithPlus() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:int);" +
        "b = foreach a generate a+b;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        PhysicalOperator pFE = phyPlan.getSuccessors( phyPlan.getRoots().get(0) ).get(0);
        assertEquals( POForEach.class, pFE.getClass() );
        POForEach pForEach = (POForEach)pFE;
        PhysicalPlan inputPln = pForEach.getInputPlans().get(0);
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
        assertEquals( LOForEach.class, fe.getClass() );
        LOForEach forEach = (LOForEach)fe;
        
        LogicalPlan innerPlan = 
            forEach.getInnerPlan();
        
        assertEquals( 1, innerPlan.getSinks().size() );        
        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        assertEquals( 1, gen.getOutputPlans().size() );
        LogicalExpressionPlan genExp = gen.getOutputPlans().get(0);
        
        assertEquals( 1, genExp.getSources().size() );
        
        
        // Main Tests start here
        assertEquals( AddExpression.class, genExp.getSources().get(0).getClass() );
        AddExpression add = (AddExpression) genExp.getSources().get(0);
        assertEquals( ls.getField(0).uid, add.getLhs().getFieldSchema().uid );
        assertEquals( ls.getField(1).uid, add.getRhs().getFieldSchema().uid );
        assertTrue( ls.getField(0).uid != add.getFieldSchema().uid );
        assertTrue( ls.getField(1).uid != add.getFieldSchema().uid );
        
        assertEquals( 1, inputPln.getLeaves().size() );
        assertEquals( Add.class, inputPln.getLeaves().get(0).getClass() );
        Add pAdd = (Add) inputPln.getLeaves().get(0);
        assertEquals( 2, inputPln.getRoots().size() );
        assertEquals( POProject.class, pAdd.getLhs().getClass() );
        assertEquals( POProject.class, pAdd.getRhs().getClass() );
    }
    
    public void testPlanwithSubtract() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:int);" +
        "b = foreach a generate a-b;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        PhysicalOperator pFE = phyPlan.getSuccessors( phyPlan.getRoots().get(0) ).get(0);
        assertEquals( POForEach.class, pFE.getClass() );
        POForEach pForEach = (POForEach)pFE;
        PhysicalPlan inputPln = pForEach.getInputPlans().get(0);
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
        assertEquals( LOForEach.class, fe.getClass() );
        LOForEach forEach = (LOForEach)fe;
        
        LogicalPlan innerPlan = 
            forEach.getInnerPlan();
        
        assertEquals( 1, innerPlan.getSinks().size() );        
        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        assertEquals( 1, gen.getOutputPlans().size() );
        LogicalExpressionPlan genExp = gen.getOutputPlans().get(0);
        
        assertEquals( 1, genExp.getSources().size() );
        
        // Main Tests start here
        assertEquals( SubtractExpression.class, genExp.getSources().get(0).getClass() );
        SubtractExpression add = (SubtractExpression) genExp.getSources().get(0);
        assertEquals( ls.getField(0).uid, add.getLhs().getFieldSchema().uid );
        assertEquals( ls.getField(1).uid, add.getRhs().getFieldSchema().uid );
        assertTrue( ls.getField(0).uid != add.getFieldSchema().uid );
        assertTrue( ls.getField(1).uid != add.getFieldSchema().uid );
        
        assertEquals( 1, inputPln.getLeaves().size() );
        assertEquals( Subtract.class, inputPln.getLeaves().get(0).getClass() );
        Subtract pSubtract = (Subtract) inputPln.getLeaves().get(0);
        assertEquals( 2, inputPln.getRoots().size() );
        assertEquals( POProject.class, pSubtract.getLhs().getClass() );
        assertEquals( POProject.class, pSubtract.getRhs().getClass() );
    }
    
    public void testPlanwithMultiply() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:int);" +
        "b = foreach a generate a*b;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        PhysicalOperator pFE = phyPlan.getSuccessors( phyPlan.getRoots().get(0) ).get(0);
        assertEquals( POForEach.class, pFE.getClass() );
        POForEach pForEach = (POForEach)pFE;
        PhysicalPlan inputPln = pForEach.getInputPlans().get(0);
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
        assertEquals( LOForEach.class, fe.getClass() );
        LOForEach forEach = (LOForEach)fe;
        
        LogicalPlan innerPlan = 
            forEach.getInnerPlan();
        
        assertEquals( 1, innerPlan.getSinks().size() );        
        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        assertEquals( 1, gen.getOutputPlans().size() );
        LogicalExpressionPlan genExp = gen.getOutputPlans().get(0);
        
        assertEquals( 1, genExp.getSources().size() );
        
        // Main Tests start here
        assertEquals( MultiplyExpression.class, genExp.getSources().get(0).getClass() );
        MultiplyExpression add = (MultiplyExpression) genExp.getSources().get(0);
        assertEquals( ls.getField(0).uid, add.getLhs().getFieldSchema().uid );
        assertEquals( ls.getField(1).uid, add.getRhs().getFieldSchema().uid );
        assertTrue( ls.getField(0).uid != add.getFieldSchema().uid );
        assertTrue( ls.getField(1).uid != add.getFieldSchema().uid );
        
        assertEquals( 1, inputPln.getLeaves().size() );
        assertEquals( Multiply.class, inputPln.getLeaves().get(0).getClass() );
        Multiply pMultiply = (Multiply) inputPln.getLeaves().get(0);
        assertEquals( 2, inputPln.getRoots().size() );
        assertEquals( POProject.class, pMultiply.getLhs().getClass() );
        assertEquals( POProject.class, pMultiply.getRhs().getClass() );
    }
    
    public void testPlanwithDivide() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:int);" +
        "b = foreach a generate a/b;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        PhysicalOperator pFE = phyPlan.getSuccessors( phyPlan.getRoots().get(0) ).get(0);
        assertEquals( POForEach.class, pFE.getClass() );
        POForEach pForEach = (POForEach)pFE;
        PhysicalPlan inputPln = pForEach.getInputPlans().get(0);
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
        assertEquals( LOForEach.class, fe.getClass() );
        LOForEach forEach = (LOForEach)fe;
        
        LogicalPlan innerPlan = 
            forEach.getInnerPlan();
        
        assertEquals( 1, innerPlan.getSinks().size() );        
        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        assertEquals( 1, gen.getOutputPlans().size() );
        LogicalExpressionPlan genExp = gen.getOutputPlans().get(0);
        
        assertEquals( 1, genExp.getSources().size() );
        
        // Main Tests start here
        assertEquals( DivideExpression.class, genExp.getSources().get(0).getClass() );
        DivideExpression add = (DivideExpression) genExp.getSources().get(0);
        assertEquals( ls.getField(0).uid, add.getLhs().getFieldSchema().uid );
        assertEquals( ls.getField(1).uid, add.getRhs().getFieldSchema().uid );
        assertTrue( ls.getField(0).uid != add.getFieldSchema().uid );
        assertTrue( ls.getField(1).uid != add.getFieldSchema().uid );
        
        assertEquals( 1, inputPln.getLeaves().size() );
        assertEquals( Divide.class, inputPln.getLeaves().get(0).getClass() );
        Divide pDivide = (Divide) inputPln.getLeaves().get(0);
        assertEquals( 2, inputPln.getRoots().size() );
        assertEquals( POProject.class, pDivide.getLhs().getClass() );
        assertEquals( POProject.class, pDivide.getRhs().getClass() );
    }
    
    public void testPlanwithMod() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:int);" +
        "b = foreach a generate a%b;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        PhysicalOperator pFE = phyPlan.getSuccessors( phyPlan.getRoots().get(0) ).get(0);
        assertEquals( POForEach.class, pFE.getClass() );
        POForEach pForEach = (POForEach)pFE;
        PhysicalPlan inputPln = pForEach.getInputPlans().get(0);
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
        assertEquals( LOForEach.class, fe.getClass() );
        LOForEach forEach = (LOForEach)fe;
        
        LogicalPlan innerPlan = 
            forEach.getInnerPlan();
        
        assertEquals( 1, innerPlan.getSinks().size() );        
        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        assertEquals( 1, gen.getOutputPlans().size() );
        LogicalExpressionPlan genExp = gen.getOutputPlans().get(0);
        
        assertEquals( 1, genExp.getSources().size() );
        
        // Main Tests start here
        assertEquals( ModExpression.class, genExp.getSources().get(0).getClass() );
        ModExpression add = (ModExpression) genExp.getSources().get(0);
        assertEquals( ls.getField(0).uid, add.getLhs().getFieldSchema().uid );
        assertEquals( ls.getField(1).uid, add.getRhs().getFieldSchema().uid );
        assertTrue( ls.getField(0).uid != add.getFieldSchema().uid );
        assertTrue( ls.getField(1).uid != add.getFieldSchema().uid );
        
        assertEquals( 1, inputPln.getLeaves().size() );
        assertEquals( Mod.class, inputPln.getLeaves().get(0).getClass() );
        Mod pMod = (Mod) inputPln.getLeaves().get(0);
        assertEquals( 2, inputPln.getRoots().size() );
        assertEquals( POProject.class, pMod.getLhs().getClass() );
        assertEquals( POProject.class, pMod.getRhs().getClass() );
    }
    
    public void testPlanwithNegative() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:int);" +
        "b = foreach a generate -a;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        PhysicalOperator pFE = phyPlan.getSuccessors( phyPlan.getRoots().get(0) ).get(0);
        assertEquals( POForEach.class, pFE.getClass() );
        POForEach pForEach = (POForEach)pFE;
        PhysicalPlan inputPln = pForEach.getInputPlans().get(0);
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
        assertEquals( LOForEach.class, fe.getClass() );
        LOForEach forEach = (LOForEach)fe;
        
        LogicalPlan innerPlan = 
            forEach.getInnerPlan();
        
        assertEquals( 1, innerPlan.getSinks().size() );        
        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        assertEquals( 1, gen.getOutputPlans().size() );
        LogicalExpressionPlan genExp = gen.getOutputPlans().get(0);
        
        assertEquals( 1, genExp.getSources().size() );
        
        // Main Tests start here
        assertEquals( NegativeExpression.class, genExp.getSources().get(0).getClass() );
        NegativeExpression add = (NegativeExpression) genExp.getSources().get(0);
        assertEquals( ls.getField(0).uid, add.getExpression().getFieldSchema().uid );
        assertTrue( ls.getField(0).uid != add.getFieldSchema().uid );
        assertTrue( ls.getField(1).uid != add.getFieldSchema().uid );
        
        assertEquals( 1, inputPln.getLeaves().size() );
        assertEquals( PONegative.class, inputPln.getLeaves().get(0).getClass() );
        PONegative pNegative = (PONegative) inputPln.getLeaves().get(0);
        assertEquals( 1, inputPln.getRoots().size() );
        assertEquals( POProject.class, pNegative.getInputs().get(0).getClass() );
    }
    
    public void testPlanwithisNull() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:int);" +
        "b = filter a by a is null;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fil = (LogicalRelationalOperator)
        newLogicalPlan.getSuccessors( newLogicalPlan.getSources().get(0) ).get(0);
        assertEquals( LOFilter.class, 
                fil.getClass() );
        LOFilter filter = (LOFilter)fil;
        
        LogicalExpressionPlan filPlan = filter.getFilterPlan();
        
        assertEquals( 1, filPlan.getSources().size() );
        assertEquals( 2, filPlan.size() );
        assertEquals( 1, filPlan.getSinks().size() );
        assertEquals( IsNullExpression.class, filPlan.getSources().get(0).getClass() );
        IsNullExpression isNull = (IsNullExpression)filPlan.getSources().get(0);
        assertTrue( ls.getField(0).uid != isNull.getFieldSchema().uid );
        assertTrue( ls.getField(1).uid != isNull.getFieldSchema().uid );
        
        assertEquals( ProjectExpression.class, isNull.getExpression().getClass() );
        ProjectExpression prj = (ProjectExpression) isNull.getExpression();
        assertEquals( ls.getField(0).uid, prj.getFieldSchema().uid );
    }
    
    public void testPlanwithisNotNull() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:int);" +
        "b = filter a by a is not null;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fil = (LogicalRelationalOperator)
        newLogicalPlan.getSuccessors( newLogicalPlan.getSources().get(0) ).get(0);
        assertEquals( LOFilter.class, 
                fil.getClass() );
        LOFilter filter = (LOFilter)fil;
        
        LogicalExpressionPlan filPlan = filter.getFilterPlan();
        
        assertEquals( 1, filPlan.getSources().size() );
        assertEquals( 3, filPlan.size() );
        assertEquals( 1, filPlan.getSinks().size() );
        assertEquals( NotExpression.class, filPlan.getSources().get(0).getClass() );
        NotExpression notExp = (NotExpression)filPlan.getSources().get(0);
        assertTrue( ls.getField(0).uid != notExp.getFieldSchema().uid );
        assertTrue( ls.getField(1).uid != notExp.getFieldSchema().uid );
        assertEquals( IsNullExpression.class, notExp.getExpression().getClass() );
        IsNullExpression isNull = (IsNullExpression)notExp.getExpression();
        assertTrue( ls.getField(0).uid != isNull.getFieldSchema().uid );
        assertTrue( ls.getField(1).uid != isNull.getFieldSchema().uid );
        
        assertEquals( ProjectExpression.class, isNull.getExpression().getClass() );
        ProjectExpression prj = (ProjectExpression) isNull.getExpression();
        assertEquals( ls.getField(0).uid, prj.getFieldSchema().uid );
    }
    
    public void testPlanwithBinCond() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:int);" +
        "b = foreach a generate ( a < b ? b : a );" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        PhysicalOperator pFE = phyPlan.getSuccessors( phyPlan.getRoots().get(0) ).get(0);
        assertEquals( POForEach.class, pFE.getClass() );
        POForEach pForEach = (POForEach)pFE;
        PhysicalPlan inputPln = pForEach.getInputPlans().get(0);
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
        assertEquals( LOForEach.class, fe.getClass() );
        LOForEach forEach = (LOForEach)fe;
        
        LogicalPlan innerPlan = 
            forEach.getInnerPlan();
        
        assertEquals( 1, innerPlan.getSinks().size() );        
        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        assertEquals( 1, gen.getOutputPlans().size() );
        LogicalExpressionPlan genExp = gen.getOutputPlans().get(0);
        
        assertEquals( 1, genExp.getSources().size() );
        
        // Main Tests start here
        assertEquals( BinCondExpression.class, genExp.getSources().get(0).getClass() );
        BinCondExpression add = (BinCondExpression) genExp.getSources().get(0);
        assertEquals( LessThanExpression.class, add.getCondition().getClass() );
        LessThanExpression lessThan = (LessThanExpression) add.getCondition();
        assertEquals( ProjectExpression.class, lessThan.getLhs().getClass() );
        ProjectExpression prj1 = ((ProjectExpression)lessThan.getLhs());
        ProjectExpression prj2 = ((ProjectExpression)lessThan.getRhs());
        assertEquals( ls.getField(0).uid, prj1.getFieldSchema().uid );
        assertEquals( ProjectExpression.class, lessThan.getRhs().getClass() );
        assertEquals( ls.getField(1).uid, prj2.getFieldSchema().uid );
        
        assertEquals( ProjectExpression.class, add.getLhs().getClass() );
        ProjectExpression prj3 = ((ProjectExpression)add.getLhs());
        assertEquals( ls.getField(1).uid, prj3.getFieldSchema().uid );
        assertEquals( ProjectExpression.class, add.getRhs().getClass() );
        ProjectExpression prj4 = ((ProjectExpression)add.getRhs());
        assertEquals( ls.getField(0).uid, prj4.getFieldSchema().uid );
        
        
        assertEquals( 4, inputPln.getRoots().size() ); 
        for( PhysicalOperator p : inputPln.getRoots() ) {
            assertEquals( POProject.class, p.getClass() );
        }
        assertEquals( 1, inputPln.getLeaves().size() );
        assertEquals( POBinCond.class, inputPln.getLeaves().get(0).getClass() );
        POBinCond binCond = (POBinCond) inputPln.getLeaves().get(0);
        assertEquals( POProject.class, binCond.getLhs().getClass() );
        POProject prj_1 = (POProject)binCond.getLhs();
        assertEquals( 1, prj_1.getColumn() );
        assertEquals( POProject.class, binCond.getRhs().getClass() );
        POProject prj_2 = (POProject) binCond.getRhs();
        assertEquals( 0, prj_2.getColumn() );
        assertEquals( LessThanExpr.class, binCond.getCond().getClass() );
        LessThanExpr lessThan_p = (LessThanExpr) binCond.getCond();
        
        assertEquals( POProject.class, lessThan_p.getLhs().getClass() );
        POProject prj_3 = (POProject) lessThan_p.getLhs();
        assertEquals( 0, prj_3.getColumn() );
        assertEquals( POProject.class, lessThan_p.getRhs().getClass() );
        POProject prj_4 = (POProject) lessThan_p.getRhs();
        assertEquals( 1, prj_4.getColumn() );
    }
    
    public void testPlanwithUserFunc() throws Exception {
        String query = ("a = load 'd.txt' as (a:int, b:bag{t:tuple(b_a:int,b_b:int)});" +
        "b = foreach a generate a,COUNT(b);" +
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        PhysicalOperator pFE = phyPlan.getSuccessors( phyPlan.getRoots().get(0) ).get(0);
        assertEquals( POForEach.class, pFE.getClass() );
        POForEach pForEach = (POForEach)pFE;
        PhysicalPlan inputPln1 = pForEach.getInputPlans().get(0);
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
        assertEquals( LOForEach.class, fe.getClass() );
        LOForEach forEach = (LOForEach)fe;
        
        LogicalPlan innerPlan = 
            forEach.getInnerPlan();
        
        assertEquals( 1, innerPlan.getSinks().size() );        
        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        assertEquals( 2, gen.getOutputPlans().size() );
        
        LogicalExpressionPlan genExp1 = gen.getOutputPlans().get(0);
        
        assertEquals( 1, genExp1.getSources().size() );
        assertEquals( ProjectExpression.class, genExp1.getSources().get(0).getClass() );
        ProjectExpression prj1  = (ProjectExpression) genExp1.getSources().get(0);
        assertEquals( ls.getField(0).uid, prj1.getFieldSchema().uid );
        
        LogicalExpressionPlan genExp2 = gen.getOutputPlans().get(1);
        assertEquals( UserFuncExpression.class, genExp2.getSources().get(0).getClass() );
        assertEquals( ProjectExpression.class, genExp2.getSinks().get(0).getClass() );
        ProjectExpression prj2 = (ProjectExpression)genExp2.getSinks().get(0);
        assertEquals( ls.getField(1).uid, prj2.getFieldSchema().uid );
        
        assertEquals( 1, inputPln1.getLeaves().size() );
        assertEquals( 1, inputPln1.getRoots().size() );
        assertEquals( POProject.class, inputPln1.getLeaves().get(0).getClass() );
        assertEquals( 0, (( POProject) inputPln1.getLeaves().get(0)).getColumn() );
        PhysicalPlan inputPln2 = pForEach.getInputPlans().get(1);
        assertEquals( POUserFunc.class, inputPln2.getLeaves().get(0).getClass() );
        assertEquals( "org.apache.pig.builtin.COUNT", 
                ((POUserFunc) inputPln2.getLeaves().get(0)).getFuncSpec().getClassName() );
        assertEquals( POProject.class, inputPln2.getRoots().get(0).getClass() );
        assertEquals( 1, ((POProject)inputPln2.getRoots().get(0)).getColumn() );
        
    }
    
    public void testPlanwithUserFunc2() throws Exception {
        // This one uses BagDereferenceExpression
        String query = ("a = load 'd.txt' as (a:int, b:bag{t:tuple(b_a:int,b_b:int)});" +
        "b = foreach a generate a,COUNT(b.b_a);" +
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        LogicalRelationalOperator ld =  (LogicalRelationalOperator)newLogicalPlan.getSources().get(0);
        assertEquals( LOLoad.class, ld.getClass() );
        LOLoad load = (LOLoad)ld;
        LogicalSchema ls = load.getSchema();
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        PhysicalOperator pFE = phyPlan.getSuccessors( phyPlan.getRoots().get(0) ).get(0);
        assertEquals( POForEach.class, pFE.getClass() );
        POForEach pForEach = (POForEach)pFE;
        PhysicalPlan inputPln = pForEach.getInputPlans().get(0);
        
        assertEquals(1, ls.getField(0).uid);
        assertEquals(2, ls.getField(1).uid);
        
        LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
        assertEquals( LOForEach.class, fe.getClass() );
        LOForEach forEach = (LOForEach)fe;
        
        LogicalPlan innerPlan = 
            forEach.getInnerPlan();
        
        assertEquals( 1, innerPlan.getSinks().size() );        
        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
        assertEquals( 2, gen.getOutputPlans().size() );
        
        LogicalExpressionPlan genExp1 = gen.getOutputPlans().get(0);
        
        assertEquals( 1, genExp1.getSources().size() );
        assertEquals( ProjectExpression.class, genExp1.getSources().get(0).getClass() );
        ProjectExpression prj1  = (ProjectExpression) genExp1.getSources().get(0);
        assertEquals( ls.getField(0).uid, prj1.getFieldSchema().uid );
        
        LogicalExpressionPlan genExp2 = gen.getOutputPlans().get(1);
        assertEquals( UserFuncExpression.class, genExp2.getSources().get(0).getClass() );
        assertEquals( ProjectExpression.class, genExp2.getSinks().get(0).getClass() );
        ProjectExpression prj2 = (ProjectExpression)genExp2.getSinks().get(0);
        assertEquals( ls.getField(1).uid, prj2.getFieldSchema().uid );
        assertEquals( DereferenceExpression.class, genExp2.getPredecessors(prj2).get(0).getClass() );
        assertEquals( 0, (int)((DereferenceExpression)genExp2.getPredecessors(prj2).get(0)).getBagColumns().get(0) );
        
        assertEquals( 1, inputPln.getRoots().size() );
        assertEquals( POProject.class, inputPln.getRoots().get(0).getClass() );
        assertEquals( 0, ((POProject)inputPln.getRoots().get(0)).getColumn() );
        
        PhysicalPlan inputPln2 = pForEach.getInputPlans().get(1);
        assertEquals( 1, inputPln2.getRoots().size() );
        assertEquals( POProject.class, inputPln2.getRoots().get(0).getClass() );
        assertEquals(1, ((POProject)inputPln2.getRoots().get(0)).getColumn() );
        assertEquals( POUserFunc.class, inputPln2.getLeaves().get(0).getClass() );
        assertEquals( "org.apache.pig.builtin.COUNT", 
                ((POUserFunc)inputPln2.getLeaves().get(0)).getFuncSpec().getClassName() );
        
        POProject prj3 = (POProject)inputPln2.getRoots().get(0);
        assertEquals( POProject.class, inputPln2.getSuccessors(prj3).get(0).getClass() );
        assertEquals( 0, ((POProject)inputPln2.getSuccessors(prj3).get(0)).getColumn() );
    }    
    
    public void testCogroup() throws Exception {
        String query = ("a = load 'd.txt' as (name:chararray, age:int, gpa:float);" +
        "b = group a by name;" +
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        assertEquals( 1, phyPlan.getRoots().size() );
        assertEquals( POLoad.class, phyPlan.getRoots().get(0).getClass() );
        POLoad load = (POLoad)phyPlan.getRoots().get(0);
        
        assertEquals( POLocalRearrange.class, phyPlan.getSuccessors(load).get(0).getClass() );
        POLocalRearrange localR = (POLocalRearrange)phyPlan.getSuccessors(load).get(0);
        assertEquals( 1, localR.getInputs().size() );
        assertEquals( 1, localR.getPlans().size() );
        PhysicalPlan cogroupPlan = localR.getPlans().get(0);
        assertEquals( 1, cogroupPlan.getLeaves().size() );        
        assertEquals( POProject.class, cogroupPlan.getLeaves().get(0).getClass() );
        POProject prj = (POProject)cogroupPlan.getLeaves().get(0);
        assertEquals( 0, prj.getColumn() );
        assertEquals( DataType.CHARARRAY, prj.getResultType() );
        
        assertEquals( POGlobalRearrange.class, phyPlan.getSuccessors(localR).get(0).getClass() );
        POGlobalRearrange globalR = (POGlobalRearrange)phyPlan.getSuccessors(localR).get(0);
        assertEquals( DataType.TUPLE, globalR.getResultType() );
        
        assertEquals( POPackage.class, phyPlan.getSuccessors(globalR).get(0).getClass() );
        POPackage pack = (POPackage)phyPlan.getSuccessors(globalR).get(0);
        assertEquals( DataType.TUPLE, pack.getResultType() );
    }
    
    public void testCogroup2() throws Exception {
        String query = ("a = load 'd.txt' as (name:chararray, age:int, gpa:float);" +
        "b = group a by ( name, age );" +
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        assertEquals( 1, phyPlan.getRoots().size() );
        assertEquals( POLoad.class, phyPlan.getRoots().get(0).getClass() );
        POLoad load = (POLoad)phyPlan.getRoots().get(0);
        
        assertEquals( POLocalRearrange.class, phyPlan.getSuccessors(load).get(0).getClass() );
        POLocalRearrange localR = (POLocalRearrange)phyPlan.getSuccessors(load).get(0);
        assertEquals( 1, localR.getInputs().size() );
        assertEquals( 2, localR.getPlans().size() );
        PhysicalPlan cogroupPlan = localR.getPlans().get(0);
        assertEquals( 1, cogroupPlan.getLeaves().size() );        
        assertEquals( POProject.class, cogroupPlan.getLeaves().get(0).getClass() );
        POProject prj = (POProject)cogroupPlan.getLeaves().get(0);
        assertEquals( 0, prj.getColumn() );
        assertEquals( DataType.CHARARRAY, prj.getResultType() );
        
        PhysicalPlan cogroupPlan2 = localR.getPlans().get(1);
        POProject prj2 = (POProject)cogroupPlan2.getLeaves().get(0);
        assertEquals( 1, prj2.getColumn() );
        assertEquals( DataType.INTEGER, prj2.getResultType() );
        
        assertEquals( POGlobalRearrange.class, phyPlan.getSuccessors(localR).get(0).getClass() );
        POGlobalRearrange globalR = (POGlobalRearrange)phyPlan.getSuccessors(localR).get(0);
        assertEquals( DataType.TUPLE, globalR.getResultType() );
        
        assertEquals( POPackage.class, phyPlan.getSuccessors(globalR).get(0).getClass() );
        POPackage pack = (POPackage)phyPlan.getSuccessors(globalR).get(0);
        assertEquals( DataType.TUPLE, pack.getResultType() );
    }
    
    public void testCogroup3() throws Exception {
        String query = "a = load 'd.txt' as (name:chararray, age:int, gpa:float);" +
        "b = load 'e.txt' as (name:chararray, age:int, gpa:float);" +
        "c = group a by name, b by name;" +
        "store c into 'empty';";  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        assertEquals( 2, phyPlan.getRoots().size() );
        assertEquals( POLoad.class, phyPlan.getRoots().get(0).getClass() );
        POLoad load = (POLoad)phyPlan.getRoots().get(0);
        
        assertEquals( POLocalRearrange.class, phyPlan.getSuccessors(load).get(0).getClass() );
        POLocalRearrange localR = (POLocalRearrange)phyPlan.getSuccessors(load).get(0);
        assertEquals( 1, localR.getPlans().size() );
        PhysicalPlan cogroupPlan = localR.getPlans().get(0);
        assertEquals( 1, cogroupPlan.getLeaves().size() );        
        assertEquals( POProject.class, cogroupPlan.getLeaves().get(0).getClass() );
        POProject prj = (POProject)cogroupPlan.getLeaves().get(0);
        assertEquals( 0, prj.getColumn() );
        assertEquals( DataType.CHARARRAY, prj.getResultType() );
        
        assertEquals( POGlobalRearrange.class, phyPlan.getSuccessors(localR).get(0).getClass() );
        POGlobalRearrange globalR = (POGlobalRearrange)phyPlan.getSuccessors(localR).get(0);
        assertEquals( DataType.TUPLE, globalR.getResultType() );
        
        assertEquals( POLoad.class, phyPlan.getRoots().get(1).getClass() );
        POLoad load2 = (POLoad)phyPlan.getRoots().get(0);
        
        assertEquals( POLocalRearrange.class, phyPlan.getSuccessors(load2).get(0).getClass() );
        POLocalRearrange localR2 = (POLocalRearrange)phyPlan.getSuccessors(load2).get(0);
        assertEquals( 1, localR2.getPlans().size() );
        PhysicalPlan cogroupPlan2 = localR2.getPlans().get(0);
        POProject prj2 = (POProject)cogroupPlan2.getLeaves().get(0);
        assertEquals( 0, prj2.getColumn() );
        assertEquals( DataType.CHARARRAY, prj2.getResultType() );
        
        assertEquals( POPackage.class, phyPlan.getSuccessors(globalR).get(0).getClass() );
        POPackage pack = (POPackage)phyPlan.getSuccessors(globalR).get(0);
        assertEquals( DataType.TUPLE, pack.getResultType() );
    }
    
    public void testCogroup4() throws Exception {
        String query = "a = load 'd.txt' as (name:chararray, age:int, gpa:float);" +
        "b = load 'e.txt' as (name:chararray, age:int, gpa:float);" +
        "c = group a by ( name, age ), b by ( name, age );" +
        "store c into 'empty';";  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        
        PhysicalPlan phyPlan = translatePlan(newLogicalPlan);
        
        assertEquals( 2, phyPlan.getRoots().size() );
        assertEquals( POLoad.class, phyPlan.getRoots().get(0).getClass() );
        POLoad load = (POLoad)phyPlan.getRoots().get(0);
        
        assertEquals( POLocalRearrange.class, phyPlan.getSuccessors(load).get(0).getClass() );
        POLocalRearrange localR = (POLocalRearrange)phyPlan.getSuccessors(load).get(0);
        assertEquals( 2, localR.getPlans().size() );
        PhysicalPlan cogroupPlan = localR.getPlans().get(0);
        assertEquals( 1, cogroupPlan.getLeaves().size() );        
        assertEquals( POProject.class, cogroupPlan.getLeaves().get(0).getClass() );
        POProject prj = (POProject)cogroupPlan.getLeaves().get(0);
        assertEquals( 0, prj.getColumn() );
        assertEquals( DataType.CHARARRAY, prj.getResultType() );
        
        PhysicalPlan cogroupPlan2 = localR.getPlans().get(1);
        assertEquals( 1, cogroupPlan2.getLeaves().size() );        
        assertEquals( POProject.class, cogroupPlan2.getLeaves().get(0).getClass() );
        POProject prj2 = (POProject)cogroupPlan2.getLeaves().get(0);
        assertEquals( 1, prj2.getColumn() );
        assertEquals( DataType.INTEGER, prj2.getResultType() );
        
        assertEquals( POGlobalRearrange.class, phyPlan.getSuccessors(localR).get(0).getClass() );
        POGlobalRearrange globalR = (POGlobalRearrange)phyPlan.getSuccessors(localR).get(0);
        assertEquals( DataType.TUPLE, globalR.getResultType() );
        
        assertEquals( POLoad.class, phyPlan.getRoots().get(1).getClass() );
        POLoad load2 = (POLoad)phyPlan.getRoots().get(0);
        
        assertEquals( POLocalRearrange.class, phyPlan.getSuccessors(load2).get(0).getClass() );
        
        POLocalRearrange localR3 = (POLocalRearrange)phyPlan.getSuccessors(load2).get(0);
        assertEquals( 2, localR3.getPlans().size() );
        PhysicalPlan cogroupPlan3 = localR3.getPlans().get(0);
        POProject prj3 = (POProject)cogroupPlan3.getLeaves().get(0);
        assertEquals( 0, prj3.getColumn() );
        assertEquals( DataType.CHARARRAY, prj3.getResultType() );
        
        PhysicalPlan cogroupPlan4 = localR3.getPlans().get(1);
        POProject prj4 = (POProject)cogroupPlan4.getLeaves().get(0);
        assertEquals( 1, prj4.getColumn() );
        assertEquals( DataType.INTEGER, prj4.getResultType() );
        
        assertEquals( POPackage.class, phyPlan.getSuccessors(globalR).get(0).getClass() );
        POPackage pack = (POPackage)phyPlan.getSuccessors(globalR).get(0);
        assertEquals( DataType.TUPLE, pack.getResultType() );
    }
    
    public void testUserDefinedForEachSchema1() throws Exception {
        String query = ("a = load 'a.txt';" +
        "b = foreach a generate $0 as a0, $1 as a1;" +        
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        Operator store = newLogicalPlan.getSinks().get(0);
        LOForEach foreach = (LOForEach)newLogicalPlan.getPredecessors(store).get(0);
        foreach.getSchema();
        
        assertTrue(foreach.getSchema().size()==2);
        assertTrue(foreach.getSchema().getField(0).alias.equals("a0"));
        assertTrue(foreach.getSchema().getField(1).alias.equals("a1"));
    }

    public void testUserDefinedForEachSchema2() throws Exception {
        String query = ("a = load 'a.txt' as (b:bag{});" +
        "b = foreach a generate flatten($0) as (a0, a1);" +
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        Operator store = newLogicalPlan.getSinks().get(0);
        LOForEach foreach = (LOForEach)newLogicalPlan.getPredecessors(store).get(0);
        foreach.getSchema();
        
        assertTrue(foreach.getSchema().size()==2);
        assertTrue(foreach.getSchema().getField(0).alias.equals("a0"));
        assertTrue(foreach.getSchema().getField(1).alias.equals("a1"));
    }
    
    // See PIG-767
    public void testCogroupSchema1() throws Exception {
        String query = ("a = load '1.txt' as (a0, a1);" +
        "b = group a by a0;" +
        "store b into 'empty';");  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        Operator store = newLogicalPlan.getSinks().get(0);
        LOCogroup cogroup = (LOCogroup)newLogicalPlan.getPredecessors(store).get(0);
        
        LogicalSchema cogroupSchema = cogroup.getSchema();
        assertEquals(cogroupSchema.getField(1).type, DataType.BAG);
        assertTrue(cogroupSchema.getField(1).alias.equals("a"));
        LogicalSchema bagSchema = cogroupSchema.getField(1).schema;
        assertEquals(bagSchema.getField(0).type, DataType.TUPLE);
        assertEquals(bagSchema.getField(0).alias, null);
        LogicalSchema tupleSchema = bagSchema.getField(0).schema;
        assertEquals(tupleSchema.size(), 2);
    }
    
    // See PIG-767
    public void testCogroupSchema2() throws Exception {
        String query = "a = load '1.txt' as (a0, a1);" +
        "b = group a by a0;" +
        "c = foreach b generate a.a1;" +
        "store c into 'empty';";  
        
        LogicalPlan newLogicalPlan = buildPlan(query);
        Operator store = newLogicalPlan.getSinks().get(0);
        LOForEach foreach = (LOForEach)newLogicalPlan.getPredecessors(store).get(0);
        
        LogicalSchema foreachSchema = foreach.getSchema();
        assertEquals(foreachSchema.getField(0).type, DataType.BAG);
        LogicalSchema bagSchema = foreachSchema.getField(0).schema;
        assertEquals(bagSchema.getField(0).type, DataType.TUPLE);
        assertEquals(bagSchema.getField(0).alias, null);
        LogicalSchema tupleSchema = bagSchema.getField(0).schema;
        assertEquals(tupleSchema.size(), 1);
        assertTrue(tupleSchema.getField(0).alias.equals("a1"));
    }
}
