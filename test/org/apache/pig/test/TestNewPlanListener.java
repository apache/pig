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
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.optimizer.AllSameRalationalNodesVisitor;
import org.apache.pig.newplan.logical.optimizer.ProjectionPatcher;
import org.apache.pig.newplan.logical.optimizer.SchemaPatcher;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for PlanTransformListerns
 *
 */
public class TestNewPlanListener {

    private LogicalPlan lp;
    private LogicalPlan changedPlan;

    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    @Before
    public void setUp() throws Exception {
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
        LOLoad A = new LOLoad(null, lp);
        A.neverUseForRealSetSchema(aschema);
        lp.add(A);

        // B = load
        LogicalSchema bschema = new LogicalSchema();
        bschema.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        bschema.getField(0).uid = 2;
        LOLoad B = new LOLoad(null, lp);
        B.neverUseForRealSetSchema(bschema);
        lp.add(B);

        // C = join
        LogicalSchema cschema = new LogicalSchema();
        cschema.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        cschema.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        cschema.getField(0).uid = 1;
        cschema.getField(1).uid = 2;

        MultiMap<Integer, LogicalExpressionPlan> mm =
            new MultiMap<Integer, LogicalExpressionPlan>();
        LOJoin C = new LOJoin(lp, mm, JOINTYPE.HASH, new boolean[] {true, true});

        LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
        ProjectExpression x = new ProjectExpression(aprojplan, 0, 0, C);
        x.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null,
                DataType.INTEGER, 1));
        LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
        ProjectExpression y = new ProjectExpression(bprojplan, 1, 0, C);
        y.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null,
                DataType.INTEGER, 2));
        mm.put(0, aprojplan);
        mm.put(1, bprojplan);

        C.neverUseForRealSetSchema(cschema);
        // Don't add it to the plan quite yet

        // D = filter
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        LOFilter D = new LOFilter(lp, filterPlan);
        ProjectExpression fy = new ProjectExpression(filterPlan, 0, 1, D);
        fy.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null,
                DataType.INTEGER, 2));
        ConstantExpression fc = new ConstantExpression(filterPlan, new Integer(0));
        new EqualExpression(filterPlan, fy, fc);

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

    private static class SillySameVisitor extends AllSameRalationalNodesVisitor {
        StringBuffer buf = new StringBuffer();

        SillySameVisitor(OperatorPlan plan) throws FrontendException {
            super(plan, new DepthFirstWalker(plan));
        }

        @Override
        protected void execute(LogicalRelationalOperator op) throws FrontendException {
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
    public void testAllSameVisitor() throws FrontendException {
        SillySameVisitor v = new SillySameVisitor(lp);
        v.visit();
        assertTrue("LOLoad LOJoin(HASH) LOLoad LOFilter ".equals(v.toString()) ||
            "LOLoad LOFilter LOJoin(HASH) LOLoad ".equals(v.toString()));

    }

    private static class SillyExpressionVisitor extends LogicalExpressionVisitor {
        StringBuffer buf;

        protected SillyExpressionVisitor(OperatorPlan p, StringBuffer b) throws FrontendException {
            super(p, new DepthFirstWalker(p));
            buf = b;
        }

        @Override
        public void visit(AndExpression andExpr) throws FrontendException {
            buf.append("and ");
        }

        @Override
        public void visit(EqualExpression equal) throws FrontendException {
            buf.append("equal ");
        }

        @Override
        public void visit(ProjectExpression p) throws FrontendException {
            buf.append("proj ");
        }

        @Override
        public void visit(ConstantExpression c) throws FrontendException {
            buf.append("const ");
        }
    }

    private static class SillyAllExpressionVisitor extends AllExpressionVisitor {
        StringBuffer buf = new StringBuffer();

        public SillyAllExpressionVisitor(OperatorPlan plan) throws FrontendException {
            super(plan, new DepthFirstWalker(plan));
        }


        @Override
        protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) throws FrontendException {
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
    public void testAllExpressionVisitor() throws FrontendException {
        SillyAllExpressionVisitor v = new SillyAllExpressionVisitor(lp);
        v.visit();
        assertTrue("proj proj equal proj const ".equals(v.toString()) ||
            "equal proj const proj proj ".equals(v.toString()));
    }

    // Test that schemas are patched up after a transform
    @Test
    public void testSchemaPatcher() throws FrontendException {
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
    public void testProjectionPatcher() throws FrontendException {
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