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

import java.util.Properties;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPlanGeneration extends junit.framework.TestCase {
    
    PigContext pc;
    
    @Override
    @BeforeClass
    public void setUp() throws ExecException {
        pc = new PigContext(ExecType.LOCAL, new Properties());
        pc.connect();
    }
    
    @Test
    public void testGenerateStar() throws Exception  {
        String query = "a = load 'x';" +
            "b = foreach a generate *;" +
            "store b into '111';";
        
        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOForEach loForEach = (LOForEach)lp.getPredecessors(loStore).get(0);
        assert(loForEach.getSchema()==null);
    }
    
    @Test
    public void testEmptyBagDereference() throws Exception  {
        String query = "A = load 'x' as ( u:bag{} );" +
            "B = foreach A generate u.$100;" +
            "store B into '111';";
        
        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOForEach loForEach = (LOForEach)lp.getPredecessors(loStore).get(0);
        LogicalSchema schema = loForEach.getSchema();
        Assert.assertTrue(schema.size()==1);
        LogicalFieldSchema bagFieldSchema = schema.getField(0);
        Assert.assertTrue(bagFieldSchema.type==DataType.BAG);
        LogicalFieldSchema tupleFieldSchema = bagFieldSchema.schema.getField(0); 
        Assert.assertTrue(tupleFieldSchema.schema.size()==1);
        Assert.assertTrue(tupleFieldSchema.schema.getField(0).type==DataType.BYTEARRAY);
    }
    
    @Test
    public void testEmptyTupleDereference() throws Exception  {
        String query = "A = load 'x' as ( u:tuple() );" +
            "B = foreach A generate u.$100;" +
            "store B into '111';";
        
        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOForEach loForEach = (LOForEach)lp.getPredecessors(loStore).get(0);
        LogicalSchema schema = loForEach.getSchema();
        Assert.assertTrue(schema.size()==1);
        Assert.assertTrue(schema.getField(0).type==DataType.BYTEARRAY);
    }

    @Test
    public void testEmptyBagInnerPlan() throws Exception  {
        String query = "A = load 'x' as ( u:bag{} );" +
            "B = foreach A { B1 = filter u by $1==0; generate B1;};" +
            "store B into '111';";
        
        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOForEach loForEach = (LOForEach)lp.getPredecessors(loStore).get(0);
        LogicalSchema schema = loForEach.getSchema();
        Assert.assertTrue(schema.size()==1);
        LogicalFieldSchema bagFieldSchema = schema.getField(0);
        Assert.assertTrue(bagFieldSchema.type==DataType.BAG);
        LogicalFieldSchema tupleFieldSchema = bagFieldSchema.schema.getField(0); 
        Assert.assertTrue(tupleFieldSchema.schema==null);
    }
    
    @Test
    public void testOrderByNullFieldSchema() throws Exception  {
        String query = "A = load 'x';" +
            "B = order A by *;" +
            "store B into '111';";
        
        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOSort loSort = (LOSort)lp.getPredecessors(loStore).get(0);
        Operator sortPlanLeaf = loSort.getSortColPlans().get(0).getSources().get(0);
        LogicalFieldSchema sortPlanFS = ((LogicalExpression)sortPlanLeaf).getFieldSchema();
        Assert.assertTrue(sortPlanFS==null);
        
        PhysicalPlan pp = Util.buildPhysicalPlanFromNewLP(lp, pc);
        POStore poStore = (POStore)pp.getLeaves().get(0);
        POSort poSort = (POSort)pp.getPredecessors(poStore).get(0);
        POProject poProject = (POProject)poSort.getSortPlans().get(0).getLeaves().get(0);
        Assert.assertTrue(poProject.getResultType()==DataType.TUPLE);
    }
    
    @Test
    public void testGroupByNullFieldSchema() throws Exception  {
        String query = "A = load 'x';" +
            "B = group A by *;" +
            "store B into '111';";
        
        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOCogroup loCoGroup = (LOCogroup)lp.getPredecessors(loStore).get(0);
        LogicalFieldSchema groupFieldSchema = loCoGroup.getSchema().getField(0);
        Assert.assertTrue(groupFieldSchema.type==DataType.TUPLE);
        Assert.assertTrue(groupFieldSchema.schema==null);
    }
    
    @Test
    public void testStoreAlias() throws Exception  {
        String query = "A = load 'data' as (a0, a1);" +
            "B = filter A by a0 > 1;" +
            "store B into 'output';";
        
        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        assert(loStore.getAlias().equals("B"));
        
        PhysicalPlan pp = Util.buildPhysicalPlanFromNewLP(lp, pc);
        POStore poStore = (POStore)pp.getLeaves().get(0);
        assert(poStore.getAlias().equals("B"));
        
        MROperPlan mrp = Util.buildMRPlanWithOptimizer(pp, pc);
        MapReduceOper mrOper = mrp.getLeaves().get(0);
        poStore = (POStore)mrOper.mapPlan.getLeaves().get(0);
        assert(poStore.getAlias().equals("B"));
    }
}
