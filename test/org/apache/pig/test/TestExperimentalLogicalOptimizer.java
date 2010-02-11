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

import java.io.IOException;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.AndExpression;
import org.apache.pig.experimental.logical.expression.ConstantExpression;
import org.apache.pig.experimental.logical.expression.EqualExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.logical.relational.LOJoin;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LogicalPlan;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.MultiMap;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Test end to end logical optimizations.
 */
public class TestExperimentalLogicalOptimizer extends TestCase {
    
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
            	"x", null, DataType.INTEGER));
        	aschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"y", null, DataType.INTEGER));
        	aschema.getField(0).uid = 1;
        	aschema.getField(1).uid = 2;
        	LOLoad A = new LOLoad(new FileSpec("bla", new FuncSpec("PigStorage", "\t")), aschema, lp);
        	lp.add(A);
	        
        	// B = load
        	LogicalSchema bschema = new LogicalSchema();
        	bschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"a", null, DataType.INTEGER));
        	bschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"b", null, DataType.INTEGER));
        	bschema.getField(0).uid = 3;
        	bschema.getField(1).uid = 4;
        	LOLoad B = new LOLoad(null, bschema, lp);
        	lp.add(B);
	        
        	// C = join
        	LogicalSchema cschema = new LogicalSchema();
        	cschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"x", null, DataType.INTEGER));
        	cschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"y", null, DataType.INTEGER));
        	cschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"a", null, DataType.INTEGER));
        	cschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"b", null, DataType.INTEGER));
        	cschema.getField(0).uid = 1;
        	cschema.getField(1).uid = 2;
        	cschema.getField(2).uid = 3;
        	cschema.getField(3).uid = 4;
        	LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
        	ProjectExpression x = new ProjectExpression(aprojplan, DataType.INTEGER, 0, 0);
        	x.neverUseForRealSetUid(1);
        	LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
        	ProjectExpression y = new ProjectExpression(bprojplan, DataType.INTEGER, 1, 0);
        	y.neverUseForRealSetUid(3);
        	MultiMap<Integer, LogicalExpressionPlan> mm = 
            	new MultiMap<Integer, LogicalExpressionPlan>();
        	mm.put(0, aprojplan);
        	mm.put(1, bprojplan);
        	LOJoin C = new LOJoin(lp, mm, JOINTYPE.HASH, new boolean[] {true, true});
        	C.neverUseForRealSetSchema(cschema);
        	lp.add(new LogicalRelationalOperator[] {A, B}, C, null);
        
        	// D = filter
        	LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        	ProjectExpression fx = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 0);
        	fx.neverUseForRealSetUid(1);
        	ConstantExpression fc0 = new ConstantExpression(filterPlan, DataType.INTEGER, new Integer(0));
        	EqualExpression eq1 = new EqualExpression(filterPlan, fx, fc0);
        	ProjectExpression fanotherx = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 0);
        	fanotherx.neverUseForRealSetUid(1);
        	ProjectExpression fa = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 2);
        	fa.neverUseForRealSetUid(3);
        	EqualExpression eq2 = new EqualExpression(filterPlan, fanotherx, fa);
        	AndExpression and1 = new AndExpression(filterPlan, eq1, eq2);
        	ProjectExpression fb = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 3);
        	fb.neverUseForRealSetUid(4);
        	ConstantExpression fc1 = new ConstantExpression(filterPlan, DataType.INTEGER, new Integer(1));
        	EqualExpression eq3 = new EqualExpression(filterPlan, fb, fc1);
        	AndExpression and2 = new AndExpression(filterPlan, and1, eq3);
        	ProjectExpression fanotherb = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 3);
        	fanotherb.neverUseForRealSetUid(4);
        	ProjectExpression fy = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 1);
        	fy.neverUseForRealSetUid(2);
        	EqualExpression eq4 = new EqualExpression(filterPlan, fy, fanotherb);
        	new AndExpression(filterPlan, and2, eq4);
        
        	LOFilter D = new LOFilter(lp, filterPlan);
        	D.neverUseForRealSetSchema(cschema);
        	// Connect D to B, since the transform has happened.
            lp.add(C, D, (LogicalRelationalOperator)null);
        }
        
        LogicalPlanOptimizer optimizer = new LogicalPlanOptimizer(lp, 500);
        optimizer.optimize();
        
        LogicalPlan expected = new LogicalPlan();
        {
            // A = load
        	LogicalSchema aschema = new LogicalSchema();
        	aschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"x", null, DataType.INTEGER));
        	aschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"y", null, DataType.INTEGER));
        	aschema.getField(0).uid = 1;
        	aschema.getField(1).uid = 2;
        	LOLoad A = new LOLoad(new FileSpec("bla", new FuncSpec("PigStorage", "\t")), aschema, expected);
        	expected.add(A);
        	
        	// DA = filter
        	LogicalExpressionPlan DAfilterPlan = new LogicalExpressionPlan();
        	ProjectExpression fx = new ProjectExpression(DAfilterPlan, DataType.INTEGER, 0, 0);
        	fx.neverUseForRealSetUid(1);
        	ConstantExpression fc0 = new ConstantExpression(DAfilterPlan, DataType.INTEGER, new Integer(0));
        	new EqualExpression(DAfilterPlan, fx, fc0);
	        
        	LOFilter DA = new LOFilter(expected, DAfilterPlan);
        	DA.neverUseForRealSetSchema(aschema);
        	expected.add(A, DA, (LogicalRelationalOperator)null);
	        
        	// B = load
        	LogicalSchema bschema = new LogicalSchema();
        	bschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"a", null, DataType.INTEGER));
        	bschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"b", null, DataType.INTEGER));
        	bschema.getField(0).uid = 3;
        	bschema.getField(1).uid = 4;
        	LOLoad B = new LOLoad(null, bschema, expected);
        	expected.add(B);
        	
        	// DB = filter
        	LogicalExpressionPlan DBfilterPlan = new LogicalExpressionPlan();
        	ProjectExpression fb = new ProjectExpression(DBfilterPlan, DataType.INTEGER, 0, 1);
        	fb.neverUseForRealSetUid(4);
        	ConstantExpression fc1 = new ConstantExpression(DBfilterPlan, DataType.INTEGER, new Integer(1));
        	new EqualExpression(DBfilterPlan, fb, fc1);
	        
        	LOFilter DB = new LOFilter(expected, DBfilterPlan);
        	DB.neverUseForRealSetSchema(bschema);
        	expected.add(B, DB, (LogicalRelationalOperator)null);
	        
        	// C = join
        	LogicalSchema cschema = new LogicalSchema();
        	cschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"x", null, DataType.INTEGER));
        	cschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"y", null, DataType.INTEGER));
        	cschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"a", null, DataType.INTEGER));
        	cschema.addField(new LogicalSchema.LogicalFieldSchema(
            	"b", null, DataType.INTEGER));
        	cschema.getField(0).uid = 1;
        	cschema.getField(1).uid = 2;
        	cschema.getField(2).uid = 3;
        	cschema.getField(3).uid = 4;
        	LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
        	ProjectExpression x = new ProjectExpression(aprojplan, DataType.INTEGER, 0, 0);
        	x.neverUseForRealSetUid(1);
        	LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
        	ProjectExpression y = new ProjectExpression(bprojplan, DataType.INTEGER, 1, 0);
        	y.neverUseForRealSetUid(3);
        	MultiMap<Integer, LogicalExpressionPlan> mm = 
            	new MultiMap<Integer, LogicalExpressionPlan>();
        	mm.put(0, aprojplan);
        	mm.put(1, bprojplan);
        	LOJoin C = new LOJoin(expected, mm, JOINTYPE.HASH, new boolean[] {true, true});
        	C.neverUseForRealSetSchema(cschema);
        	expected.add(new LogicalRelationalOperator[] {DA, DB}, C, null);
	        
        	// D = filter
        	LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        	ProjectExpression fanotherx = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 0);
        	fanotherx.neverUseForRealSetUid(1);
        	ProjectExpression fa = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 2);
        	fa.neverUseForRealSetUid(3);
        	EqualExpression eq2 = new EqualExpression(filterPlan, fanotherx, fa);
        	ProjectExpression fanotherb = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 3);
        	fanotherb.neverUseForRealSetUid(4);
        	ProjectExpression fy = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 1);
        	fy.neverUseForRealSetUid(2);
        	EqualExpression eq4 = new EqualExpression(filterPlan, fy, fanotherb);
        	new AndExpression(filterPlan, eq2, eq4);
	        
        	LOFilter D = new LOFilter(expected, filterPlan);
        	D.neverUseForRealSetSchema(cschema);
        	expected.add(C, D, (LogicalRelationalOperator)null);
        }
        
        assertTrue( lp.isEqual(expected) );
        // assertEquals(lp, expected);
    }

}
