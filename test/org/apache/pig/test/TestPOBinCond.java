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
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.impl.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.impl.physicalLayer.expressionOperators.POProject;
import org.apache.pig.impl.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.test.utils.GenPhyOp;
import org.junit.Before;

import junit.framework.TestCase;

public class TestPOBinCond extends TestCase {
    Random r = new Random();
    DataBag bag = BagFactory.getInstance().newDefaultBag();
    final int MAX = 10;
    
    @Before
    @Override
    public void setUp() {
        for(int i = 0; i < 10; i ++) {
            Tuple t = TupleFactory.getInstance().newTuple();
            t.append(r.nextInt(2));
            t.append(0);
            t.append(1);
            bag.add(t);
        }
    }
    
    public void testPOBinCond() throws ExecException, PlanException {
        ConstantExpression rt = (ConstantExpression) GenPhyOp.exprConst();
        rt.setValue(1);
        rt.setResultType(DataType.INTEGER);
        
        POProject prj1 = GenPhyOp.exprProject();
        prj1.setColumn(0);
        prj1.setResultType(DataType.INTEGER);
        
        EqualToExpr equal = (EqualToExpr) GenPhyOp.compEqualToExpr();
        equal.setLhs(prj1);
        equal.setRhs(rt);
        equal.setOperandType(DataType.INTEGER);
        
        POProject prjLhs = GenPhyOp.exprProject();
        prjLhs.setResultType(DataType.INTEGER);
        prjLhs.setColumn(1);
        
        POProject prjRhs = GenPhyOp.exprProject();
        prjRhs.setResultType(DataType.INTEGER);
        prjRhs.setColumn(2);
        
        POBinCond op = new POBinCond(new OperatorKey("", r.nextLong()), -1, equal, prjLhs, prjRhs);
        op.setResultType(DataType.INTEGER);
        
        PhysicalPlan plan = new PhysicalPlan();
        plan.add(op);
        plan.add(prjLhs);
        plan.add(prjRhs);
        plan.add(equal);
        plan.connect(equal, op);
        plan.connect(prjLhs, op);
        plan.connect(prjRhs, op);
        
        plan.add(prj1);
        plan.add(rt);
        plan.connect(prj1, equal);
        plan.connect(rt, equal);
        
        for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            plan.attachInput(t);
            Integer i = (Integer) t.get(0);
            assertEquals(1, i | (Integer)op.getNext(i).result);
//            System.out.println(t + " " + op.getNext(i).result.toString());
        }
        
        
    }
}
