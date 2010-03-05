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

import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.AndExpression;
import org.apache.pig.experimental.logical.expression.ConstantExpression;
import org.apache.pig.experimental.logical.expression.EqualExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.experimental.logical.optimizer.AllSameVisitor;
import org.apache.pig.experimental.logical.optimizer.ProjectionPatcher;
import org.apache.pig.experimental.logical.optimizer.SchemaPatcher;
import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.logical.relational.LOJoin;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LogicalPlan;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.experimental.plan.DepthFirstWalker;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.impl.util.MultiMap;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Tests for PlanTransformListerns
 *
 */
public class TestExperimentalListener extends TestCase {
    
    private LogicalPlan lp;
    private LogicalPlan changedPlan;

    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        
        // Build a plan that looks like it has just been transformed
        // It is roughly the logical plan for
        // A = load 'bla' as (x);
        // B = load 'morebla' as (y);
        // C = join A on x, B on y;
        // D = filter C by y > 0;
        // The plan is built with the filter having been pushed above the join
        // but the listners not yet having been called.
        // A = load
        lp = new LogicalPlan();
        LogicalSchema aschema = new LogicalSchema();
        aschema.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        aschema.getField(0).uid = 1;
        LOLoad A = new LOLoad(null, aschema, lp);
        lp.add(A);
        
        // B = load
        LogicalSchema bschema = new LogicalSchema();
        bschema.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        bschema.getField(0).uid = 2;
        LOLoad B = new LOLoad(null, bschema, lp);
        lp.add(B);
        
        // C = join
        LogicalSchema cschema = new LogicalSchema();
        cschema.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        cschema.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        cschema.getField(0).uid = 1;
        cschema.getField(1).uid = 2;
        LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
        ProjectExpression x = new ProjectExpression(aprojplan, DataType.INTEGER, 0, 0);
        x.neverUseForRealSetUid(1);
        LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
        ProjectExpression y = new ProjectExpression(bprojplan, DataType.INTEGER, 1, 0);
        y.neverUseForRealSetUid(2);
        MultiMap<Integer, LogicalExpressionPlan> mm = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm.put(0, aprojplan);
        mm.put(1, bprojplan);
        LOJoin C = new LOJoin(lp, mm, JOINTYPE.HASH, new boolean[] {true, true});
        C.neverUseForRealSetSchema(cschema);
        // Don't add it to the plan quite yet
        
        // D = filter
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression fy = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 1);
        fy.neverUseForRealSetUid(2);
        ConstantExpression fc = new ConstantExpression(filterPlan, DataType.INTEGER, new Integer(0));
        new EqualExpression(filterPlan, fy, fc);
        LOFilter D = new LOFilter(lp, filterPlan);
        D.neverUseForRealSetSchema(cschema);
        // Connect D to B, since the transform has happened.
        lp.add(D);
        lp.connect(B, D);
        
        // Now add in C, connected to A and D.
        lp.add(C);
        lp.connect(A, C);
        lp.connect(D, C);
        
        changedPlan = new LogicalPlan();
        changedPlan.add(D);
        changedPlan.add(C);
        changedPlan.connect(D, C);
    }
    
    private static class SillySameVisitor extends AllSameVisitor {
        StringBuffer buf = new StringBuffer();

        SillySameVisitor(OperatorPlan plan) {
            super(plan, new DepthFirstWalker(plan));
        }
        
        @Override
        protected void execute(LogicalRelationalOperator op) throws IOException {
            buf.append(op.getName());
            buf.append(" ");
        }
        
        @Override
        public String toString() {
            return buf.toString();
        }
        
    }
    
    // Test that the AllSameVisitor calls execute on every node
    // in the plan.
    @Test
    public void testAllSameVisitor() throws IOException {
        SillySameVisitor v = new SillySameVisitor(lp);
        v.visit();
        assertTrue("LOLoad LOJoin LOLoad LOFilter ".equals(v.toString()) ||
            "LOLoad LOFilter LOJoin LOLoad ".equals(v.toString()));
        
    }
    
    private static class SillyExpressionVisitor extends LogicalExpressionVisitor {
        StringBuffer buf;

        protected SillyExpressionVisitor(OperatorPlan p, StringBuffer b) {
            super(p, new DepthFirstWalker(p));
            buf = b;
        }
        
        @Override
        public void visitAnd(AndExpression andExpr) throws IOException {
            buf.append("and ");
        }
        
        @Override
        public void visitEqual(EqualExpression equal) throws IOException {
            buf.append("equal ");
        }
        
        @Override
        public void visitProject(ProjectExpression p) throws IOException {
            buf.append("proj ");
        }
        
        @Override
        public void visitConstant(ConstantExpression c) throws IOException {
            buf.append("const ");
        }
    }
    
    private static class SillyAllExpressionVisitor extends AllExpressionVisitor {
        StringBuffer buf = new StringBuffer();

        public SillyAllExpressionVisitor(OperatorPlan plan) {
            super(plan, new DepthFirstWalker(plan));
        }
     

        @Override
        protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) {
            return new SillyExpressionVisitor(expr, buf);
        }   
        
        @Override
        public String toString() {
            return buf.toString();
        }
    }
    
    // Test that the AllExpressionVisitor executes on every
    // expression in the plan
    @Test
    public void testAllExpressionVisitor() throws IOException {
        SillyAllExpressionVisitor v = new SillyAllExpressionVisitor(lp);
        v.visit();
        assertTrue("proj proj equal proj const ".equals(v.toString()) ||
            "equal proj const proj proj ".equals(v.toString()));
    }
    
    // Test that schemas are patched up after a transform
    @Test
    public void testSchemaPatcher() throws IOException {
        SchemaPatcher patcher = new SchemaPatcher();
        patcher.transformed(lp, changedPlan);
        
        // Check that the filter now has the proper schema.
        List<Operator> roots = changedPlan.getSources();
        assertEquals(1, roots.size());
        LOFilter D = (LOFilter)roots.get(0);
        assertNotNull(D);
        LogicalSchema dschema = D.getSchema();
        assertEquals(1, dschema.size());
        LogicalSchema.LogicalFieldSchema y = dschema.getField(0);
        assertEquals("y", y.alias);
        assertEquals(2, y.uid);
    }
    
    // Test that projections are patched up after a transform
    @Test
    public void testProjectionPatcher() throws IOException {
        ProjectionPatcher patcher = new ProjectionPatcher();
        patcher.transformed(lp, changedPlan);
        
        // Check that the projections in filter are now set properly
        List<Operator> roots = changedPlan.getSources();
        assertEquals(1, roots.size());
        LOFilter D = (LOFilter)roots.get(0);
        assertNotNull(D);
        LogicalExpressionPlan filterPlan = D.getFilterPlan();
        List<Operator> leaves = filterPlan.getSinks();
        assertEquals(2, leaves.size());
        ProjectExpression proj = null;
        for (Operator leaf : leaves) {
            if (leaf instanceof ProjectExpression) {
                proj = (ProjectExpression)leaf;
                break;
            }
        }
        assertNotNull(proj);
        assertEquals(0, proj.getInputNum());
        assertEquals(0, proj.getColNum());
    }

}

