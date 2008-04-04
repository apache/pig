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
package org.apache.pig.impl.physicalLayer.topLevelOperators;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.ExprPlan;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.ComparisonOperator;

/**
 * This is an implementation of the Filter operator. It has an Expression Plan
 * that decides whether the input tuple should be filtered or passed through. To
 * avoid many function calls, the filter operator, stores the Comparison
 * Operator that is the root of the Expression Plan and uses its getNext
 * directly.
 * 
 * Since the filter is supposed to return tuples only, getNext is not supported
 * on any other data type.
 * 
 */
public class POFilter extends PhysicalOperator<PhyPlanVisitor> {

    private Log log = LogFactory.getLog(getClass());

    // The expression plan
    ExprPlan plan;

    // The root comparison operator of the expression plan
    ComparisonOperator comOp;

    // The operand type for the comparison operator needed
    // to call the comparison operators getNext with the
    // appropriate type
    byte compOperandType;

    public POFilter(OperatorKey k) {
        this(k, -1, null);
    }

    public POFilter(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POFilter(OperatorKey k, List<PhysicalOperator> inputs) {
        this(k, -1, inputs);
    }

    public POFilter(OperatorKey k, int rp, List<PhysicalOperator> inputs) {
        super(k, rp, inputs);
    }

    /**
     * Attaches the proccesed input tuple to the expression plan and checks if
     * comparison operator returns a true. If so the tuple is not filtered and
     * let to pass through. Else, further input is processed till a tuple that
     * can be passed through is found or EOP is reached.
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = null;
        Result inp = null;
        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP
                    || inp.returnStatus == POStatus.STATUS_ERR)
                break;
            if (inp.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }

            plan.attachInput((Tuple) inp.result);

            switch (compOperandType) {
            case DataType.BYTEARRAY:
                res = comOp.getNext(dummyDBA);
                if (res.returnStatus != POStatus.STATUS_OK)
                    continue;
                break;
            case DataType.CHARARRAY:
                res = comOp.getNext(dummyString);
                if (res.returnStatus != POStatus.STATUS_OK)
                    continue;
                break;
            case DataType.DOUBLE:
                res = comOp.getNext(dummyDouble);
                if (res.returnStatus != POStatus.STATUS_OK)
                    continue;
                break;
            case DataType.FLOAT:
                res = comOp.getNext(dummyFloat);
                if (res.returnStatus != POStatus.STATUS_OK)
                    continue;
                break;
            case DataType.INTEGER:
                res = comOp.getNext(dummyInt);
                if (res.returnStatus != POStatus.STATUS_OK)
                    continue;
                break;
            case DataType.LONG:
                res = comOp.getNext(dummyLong);
                if (res.returnStatus != POStatus.STATUS_OK)
                    continue;
                break;
            }

            if (res == null) {
                return new Result();
            }
            if ((Boolean) res.result == true) {
                return inp;
            }
        }
        return inp;
    }

    @Override
    public String name() {
        return "Filter - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws ParseException {
        v.visitFilter(this);
    }

    public void setPlan(ExprPlan plan) {
        this.plan = plan;
        comOp = (ComparisonOperator) (plan.getLeaves()).get(0);
        compOperandType = comOp.getOperandType();
    }

    public ExprPlan getPlan() {
        return plan;
    }
}
