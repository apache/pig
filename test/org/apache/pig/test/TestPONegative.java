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

import java.util.Iterator;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.plans.ExprPlan;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.POProject;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.unaryExprOps.PONegative;
import org.apache.pig.impl.plan.PlanException;
import org.junit.Before;

import junit.framework.TestCase;

public class TestPONegative extends TestCase {
    
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    Random r = new Random();
    TupleFactory tf = TupleFactory.getInstance();
    final int MAX = 10;
    
    public void testPONegInt () throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextInt());
            bag.add(t);
        }
        
        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.INTEGER);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.INTEGER);
        
        ExprPlan plan = new ExprPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Integer expected = -(Integer)t.get(0);
            int output = (Integer) pn.getNext(expected).result;
            assertEquals(expected.intValue(), output);
        }
        
    }
    
    public void testPONegLong () throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextLong());
            bag.add(t);
        }
        
        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.LONG);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.LONG);
        
        ExprPlan plan = new ExprPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Long expected = -(Long)t.get(0);
            long output = (Long) pn.getNext(expected).result;
            assertEquals(expected.longValue(), output);
        }
        
    }
    
    public void testPONegDouble() throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextDouble());
            bag.add(t);
        }
        
        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.DOUBLE);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.DOUBLE);
        
        ExprPlan plan = new ExprPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Double expected = -(Double)t.get(0);
            double output = (Double) pn.getNext(expected).result;
            assertEquals(expected.doubleValue(), output);
        }
        
    }
    
    public void testPONegFloat() throws PlanException, ExecException {
        for(int i = 0; i < MAX; i++) {
            Tuple t = tf.newTuple();
            t.append(r.nextFloat());
            bag.add(t);
        }
        
        POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj.setResultType(DataType.FLOAT);
        PONegative pn = new PONegative(new OperatorKey("", r.nextLong()), -1, prj);
        pn.setResultType(DataType.FLOAT);
        
        ExprPlan plan = new ExprPlan();
        plan.add(prj); plan.add(pn);
        plan.connect(prj, pn);
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Float expected = -(Float)t.get(0);
            float output = (Float) pn.getNext(expected).result;
            assertEquals(expected.floatValue(), output);
        }
        
    }
}
