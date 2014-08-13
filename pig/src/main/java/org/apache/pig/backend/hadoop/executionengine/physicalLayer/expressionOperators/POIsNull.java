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

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POIsNull extends UnaryComparisonOperator {

    private static final long serialVersionUID = 1L;

    public POIsNull(OperatorKey k, int rp) {
        super(k, rp);

    }

    public POIsNull(OperatorKey k) {
        super(k);

    }

    public POIsNull(OperatorKey k, int rp, ExpressionOperator in) {
        super(k, rp);
        this.expr = in;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitIsNull(this);
    }

    @Override
    public String name() {
        // TODO Auto-generated method stub
        return "POIsNull" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    @Override
    public Result getNextBoolean() throws ExecException {

        Result res = null;
        switch(operandType) {
        case DataType.BYTEARRAY:
        case DataType.DOUBLE:
        case DataType.INTEGER:
        case DataType.BIGINTEGER:
        case DataType.BIGDECIMAL:
        case DataType.CHARARRAY:
        case DataType.BOOLEAN:
        case DataType.LONG:
        case DataType.FLOAT:
        case DataType.DATETIME:
        case DataType.MAP:
        case DataType.TUPLE:
        case DataType.BAG:
            res = expr.getNext(operandType);
            if(res.returnStatus == POStatus.STATUS_OK) {
                if (res.result == null) {
                    res.result = true;
                } else {
                    res.result = false;
                }
                illustratorMarkup(null, res.result, (Boolean) res.result ? 0 : 1);
            }
            return res;
        default: {
            int errCode = 2067;
            String msg = this.getClass().getSimpleName() + " does not know how to " +
            "handle type: " + DataType.findTypeName(operandType);
            throw new ExecException(msg, errCode, PigException.BUG);
        }

        }
    }

    @Override
    public POIsNull clone() throws CloneNotSupportedException {
        POIsNull clone = new POIsNull(new OperatorKey(mKey.scope,
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
        clone.cloneHelper(this);
        return clone;
    }
}
