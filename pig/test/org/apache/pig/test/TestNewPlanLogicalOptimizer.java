/**
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

import static org.apache.pig.newplan.logical.relational.LOTestHelper.newLOLoad;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.junit.Before;
import org.junit.Test;

/**
 * Test end to end logical optimizations.
 */
public class TestNewPlanLogicalOptimizer {

    Configuration conf = null;

    @Before
    public void setUp() throws Exception {
        PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
        pc.connect();
        conf = new Configuration(
                ConfigurationUtil.toConfiguration(pc.getFs().getConfiguration())
                );
    }

    @Test
    public void testFilterPushDown() throws IOException {
        // A logical plan for:
        // A = load 'bla' as (x, y);
        // B = load 'morebla' as (a, b);
        // C = join A on x, B on a;
        // D = filter C by x = a and x = 0 and b = 1 and y = b;
        // store D into 'whatever';

        // A = load
        LogicalPlan lp = new LogicalPlan();
        {
            LogicalSchema aschema = new LogicalSchema();
            aschema.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.BYTEARRAY));
            aschema.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.BYTEARRAY));
            LOLoad A = newLOLoad(new FileSpec("bla", new FuncSpec("PigStorage", "\t")), aschema, lp, conf);
            A.setAlias("A");
            lp.add(A);

            // B = load
            LogicalSchema bschema = new LogicalSchema();
            bschema.addField(new LogicalSchema.LogicalFieldSchema(
                "a", null, DataType.BYTEARRAY));
            bschema.addField(new LogicalSchema.LogicalFieldSchema(
                "b", null, DataType.BYTEARRAY));
            LOLoad B = newLOLoad(new FileSpec("morebla", new FuncSpec("PigStorage", "\t")), bschema, lp, conf);
            B.setAlias("B");
            lp.add(B);

            // C = join
            LogicalSchema cschema = new LogicalSchema();
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.BYTEARRAY));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.BYTEARRAY));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "a", null, DataType.BYTEARRAY));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "b", null, DataType.BYTEARRAY));
            MultiMap<Integer, LogicalExpressionPlan> mm =
                new MultiMap<Integer, LogicalExpressionPlan>();
            LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
            LOJoin C = new LOJoin(lp, mm, JOINTYPE.HASH, new boolean[] {true, true});
            new ProjectExpression(aprojplan, 0, 0, C);
            LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
            new ProjectExpression(bprojplan, 1, 0, C);
            mm.put(0, aprojplan);
            mm.put(1, bprojplan);

            C.setAlias("C");
            lp.add(C);
            lp.connect(A, C);
            lp.connect(B, C);

            // D = filter
            LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
            LOFilter D = new LOFilter(lp, filterPlan);
            ProjectExpression fx = new ProjectExpression(filterPlan, 0, 0, D);
            ConstantExpression fc0 = new ConstantExpression(filterPlan, new Integer(0));
            EqualExpression eq1 = new EqualExpression(filterPlan, fx, fc0);
            ProjectExpression fanotherx = new ProjectExpression(filterPlan, 0, 0, D);
            ProjectExpression fa = new ProjectExpression(filterPlan, 0, 2, D);
            EqualExpression eq2 = new EqualExpression(filterPlan, fanotherx, fa);
            AndExpression and1 = new AndExpression(filterPlan, eq1, eq2);
            ProjectExpression fb = new ProjectExpression(filterPlan, 0, 3, D);
            ConstantExpression fc1 = new ConstantExpression(filterPlan, new Integer(1));
            EqualExpression eq3 = new EqualExpression(filterPlan, fb, fc1);
            AndExpression and2 = new AndExpression(filterPlan, and1, eq3);
            ProjectExpression fanotherb = new ProjectExpression(filterPlan, 0, 3, D);
            ProjectExpression fy = new ProjectExpression(filterPlan, 0, 1, D);
            EqualExpression eq4 = new EqualExpression(filterPlan, fy, fanotherb);
            new AndExpression(filterPlan, and2, eq4);

            D.setAlias("D");
            // Connect D to B, since the transform has happened.
            lp.add(D);
            lp.connect(C, D);
        }

        System.out.println(lp);
        LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer(lp, 500, null);
        optimizer.optimize();

        LogicalPlan expected = new LogicalPlan();
        {
            // A = load
            LogicalSchema aschema = new LogicalSchema();
            aschema.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.BYTEARRAY));
            aschema.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.BYTEARRAY));
            LOLoad A = newLOLoad(new FileSpec("bla", new FuncSpec("PigStorage", "\t")), aschema, expected, conf);
            expected.add(A);

            // DA = filter
            LogicalExpressionPlan DAfilterPlan = new LogicalExpressionPlan();
            LOFilter DA = new LOFilter(expected, DAfilterPlan);
            ProjectExpression fx = new ProjectExpression(DAfilterPlan, 0, 0, DA);
            fx.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
            ConstantExpression fc0 = new ConstantExpression(DAfilterPlan, new Integer(0));
            new EqualExpression(DAfilterPlan, fx, fc0);

            DA.neverUseForRealSetSchema(aschema);
            expected.add(DA);
            expected.connect(A, DA);

            // B = load
            LogicalSchema bschema = new LogicalSchema();
            bschema.addField(new LogicalSchema.LogicalFieldSchema(
                "a", null, DataType.BYTEARRAY));
            bschema.addField(new LogicalSchema.LogicalFieldSchema(
                "b", null, DataType.BYTEARRAY));
            LOLoad B = newLOLoad(new FileSpec("morebla", new FuncSpec("PigStorage", "\t")), bschema, expected, conf);
            expected.add(B);

            // DB = filter
            LogicalExpressionPlan DBfilterPlan = new LogicalExpressionPlan();
            LOFilter DB = new LOFilter(expected, DBfilterPlan);
            ProjectExpression fb = new ProjectExpression(DBfilterPlan, 0, 1, DB);
            fb.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
            ConstantExpression fc1 = new ConstantExpression(DBfilterPlan, new Integer(1));
            new EqualExpression(DBfilterPlan, fb, fc1);

            DB.neverUseForRealSetSchema(bschema);
            expected.add(DB);
            expected.connect(B, DB);

            // C = join
            LogicalSchema cschema = new LogicalSchema();
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "D::x", null, DataType.BYTEARRAY));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "D::y", null, DataType.BYTEARRAY));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "null::a", null, DataType.BYTEARRAY));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "null::b", null, DataType.BYTEARRAY));
            cschema.getField(0).uid = 1;
            cschema.getField(1).uid = 2;
            cschema.getField(2).uid = 3;
            cschema.getField(3).uid = 4;
            LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
            MultiMap<Integer, LogicalExpressionPlan> mm =
                new MultiMap<Integer, LogicalExpressionPlan>();
            LOJoin C = new LOJoin(expected, mm, JOINTYPE.HASH, new boolean[] {true, true});

            ProjectExpression x = new ProjectExpression(aprojplan, 0, 0, C);
            x.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
            LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
            ProjectExpression y = new ProjectExpression(bprojplan, 1, 0, C);
            y.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
            mm.put(0, aprojplan);
            mm.put(1, bprojplan);
            C.neverUseForRealSetSchema(cschema);
            expected.add(C);
            expected.connect(DA, C);
            expected.connect(DB, C);

            // D = filter
            LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
            LOFilter D = new LOFilter(expected, filterPlan);
            ProjectExpression fanotherx = new ProjectExpression(filterPlan, 0, 0, D);
            fanotherx.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
            ProjectExpression fa = new ProjectExpression(filterPlan, 0, 2, D);
            fa.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
            EqualExpression eq2 = new EqualExpression(filterPlan, fanotherx, fa);
            ProjectExpression fanotherb = new ProjectExpression(filterPlan, 0, 3, D);
            fanotherb.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
            ProjectExpression fy = new ProjectExpression(filterPlan, 0, 1, D);
            fy.neverUseForRealSetFieldSchema(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
            EqualExpression eq4 = new EqualExpression(filterPlan, fy, fanotherb);
            new AndExpression(filterPlan, eq2, eq4);

            D.neverUseForRealSetSchema(cschema);
            expected.add(D);
            expected.connect(C, D);
        }

        SchemaResetter schemaResetter = new SchemaResetter(lp);
        schemaResetter.visit();

        schemaResetter = new SchemaResetter(expected);
        schemaResetter.visit();


        assertTrue( lp.isEqual(expected) );
    }

}
