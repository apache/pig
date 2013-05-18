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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class PONegative extends UnaryExpressionOperator {

    private static final long serialVersionUID = 1L;

    public PONegative(OperatorKey k, int rp) {
        super(k, rp);

    }

    public PONegative(OperatorKey k) {
        super(k);

    }

    public PONegative(OperatorKey k, int rp, ExpressionOperator input) {
        super(k, rp);
        this.expr = input;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitNegative(this);
    }

    @Override
    public String name() {
        // TODO Auto-generated method stub
        return "PONegative" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    @Override
    public Result getNextDouble() throws ExecException {
        Result res = expr.getNextDouble();
        if(res.returnStatus == POStatus.STATUS_OK && res.result!=null) {
                 res.result = -1*((Double)res.result);

       }

        return res;
    }

    @Override
    public Result getNextFloat() throws ExecException {
        Result res = expr.getNextFloat();
        if(res.returnStatus == POStatus.STATUS_OK && res.result!=null ) {
            res.result = -1*((Float)res.result);
        }
        return res;
    }

    @Override
    public Result getNextInteger() throws ExecException {
        Result res = expr.getNextInteger();
        if(res.returnStatus == POStatus.STATUS_OK && res.result!=null ) {
            res.result = -1*((Integer)res.result);
        }
        return res;
    }

    @Override
    public Result getNextLong() throws ExecException {
        Result res = expr.getNextLong();
        if(res.returnStatus == POStatus.STATUS_OK && res.result!=null) {
            res.result = -1*((Long)res.result);
        }
        return res;
    }

    @Override
    public Result getNextBigInteger() throws ExecException {
        Result res = expr.getNextBigInteger();
        if(res.returnStatus == POStatus.STATUS_OK && res.result!=null) {
            res.result = ((BigInteger)res.result).negate();
        }
        return res;
    }

    @Override
    public Result getNextBigDecimal() throws ExecException {
        Result res = expr.getNextBigDecimal();
        if(res.returnStatus == POStatus.STATUS_OK && res.result!=null) {
            res.result = ((BigDecimal)res.result).negate();
        }
        return res;
    }

    @Override
    public PONegative clone() throws CloneNotSupportedException {
        PONegative clone = new PONegative(new OperatorKey(mKey.scope,
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
        clone.cloneHelper(this);
        return clone;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return (Tuple) out;
    }
}
