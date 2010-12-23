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

import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;

public class Divide extends BinaryExpressionOperator {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public Divide(OperatorKey k) {
        super(k);
    }

    public Divide(OperatorKey k, int rp) {
        super(k, rp);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitDivide(this);
    }

    @Override
    public String name() {
        return "Divide" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    /*
     * This method is used to invoke the appropriate method, as Java does not provide generic
     * dispatch for it.
     */
    @SuppressWarnings("unchecked")
    protected <T extends Number> T divide(T a, T b, byte dataType) throws ExecException {
        switch (dataType) {
        case DataType.DOUBLE:
            return (T) Double.valueOf((Double) a / (Double) b);
        case DataType.INTEGER:
            return (T) Integer.valueOf((Integer) a / (Integer) b);
        case DataType.LONG:
            return (T) Long.valueOf((Long) a / (Long) b);
        case DataType.FLOAT:
            return (T) Float.valueOf((Float) a / (Float) b);
        default:
            throw new ExecException("called on unsupported Number class " + DataType.findTypeName(dataType));
        }
    }

    /*
     * This method is used to invoke the appropriate method, as Java does not provide generic
     * dispatch for it.
     */
    protected <T extends Number> boolean equalsZero(T a, byte dataType) throws ExecException {
        switch (dataType) {
        case DataType.DOUBLE:
            return ((Double) a).equals(0.0);
        case DataType.INTEGER:
            return ((Integer) a).equals(0);
        case DataType.LONG:
            return ((Long) a).equals(0L);
        case DataType.FLOAT:
            return ((Float) a).equals(0.0f);
        default:
            throw new ExecException("Called on unsupported Number class " + DataType.findTypeName(dataType));
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends Number> Result genericGetNext(T number, byte dataType) throws ExecException {
        Result r = accumChild(null, number, dataType);
        if (r != null) {
            return r;
        }


        byte status;
        Result res;
        T left = null, right = null;
        res = lhs.getNext(left, dataType);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        left = (T) res.result;

        res = rhs.getNext(right, dataType);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        right = (T) res.result;

        if (equalsZero(right, dataType)) {
            if(pigLogger != null) {
                pigLogger.warn(this, "Divide by zero. Converting it to NULL.", PigWarning.DIVIDE_BY_ZERO);
            }
            res.result = null;
        } else {
            res.result = divide(left, right, dataType);
        }
        return res;
    }

    @Override
    public Result getNext(Double d) throws ExecException {
        return genericGetNext(d, DataType.DOUBLE);
    }

    @Override
    public Result getNext(Float f) throws ExecException {
        return genericGetNext(f, DataType.FLOAT);

    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        return genericGetNext(i, DataType.INTEGER);
    }

    @Override
    public Result getNext(Long l) throws ExecException {
        return genericGetNext(l, DataType.LONG);
    }

    @Override
    public Divide clone() throws CloneNotSupportedException {
        Divide clone = new Divide(new OperatorKey(mKey.scope,
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
        clone.cloneHelper(this);
        return clone;
    }


}
