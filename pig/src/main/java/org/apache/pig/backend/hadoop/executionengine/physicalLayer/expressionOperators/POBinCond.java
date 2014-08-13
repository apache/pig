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

import java.util.ArrayList;
import java.util.List;


import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POBinCond extends ExpressionOperator {

    private static final long serialVersionUID = 1L;
    ExpressionOperator cond;
    ExpressionOperator lhs;
    ExpressionOperator rhs;
    private transient List<ExpressionOperator> child;

    public POBinCond(OperatorKey k) {
        super(k);
    }

    public POBinCond(OperatorKey k, int rp) {
        super(k, rp);
    }

    public POBinCond(OperatorKey k, int rp, ExpressionOperator cond, ExpressionOperator lhs, ExpressionOperator rhs) {
        super(k, rp);
        this.cond = cond;
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public Result genericGetNext(byte dataType) throws ExecException {
        List<ExpressionOperator> list = new ArrayList<ExpressionOperator>();
        list.add(cond);
        Result r = accumChild(list, DataType.BOOLEAN);

        if (r != null) {
            if (r.returnStatus != POStatus.STATUS_BATCH_OK) {
                return r;
            }
            list.clear();
            list.add(lhs);
            list.add(rhs);
            r = accumChild(list, dataType);
            return r;
        }
        Result res = cond.getNextBoolean();
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) {
            return res;
        }
        Result result = ((Boolean)res.result) == true ? lhs.getNext(dataType) : rhs.getNext(dataType);
        illustratorMarkup(null, result.result, ((Boolean)res.result) ? 0 : 1);
        return result;
    }

    @Override
    public Result getNextBoolean() throws ExecException {
        Result r = accumChild(null, DataType.BOOLEAN);
        if (r != null) {
            return r;
        }

        Result res = cond.getNextBoolean();
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) {
            return res;
        }
        return ((Boolean)res.result) == true ? lhs.getNextBoolean() : rhs.getNextBoolean();

    }

    @Override
    public Result getNextDataBag() throws ExecException {
        return genericGetNext(DataType.BAG);
    }

    @Override
    public Result getNextDataByteArray() throws ExecException {
        return genericGetNext(DataType.BYTEARRAY);
    }

    @Override
    public Result getNextDouble() throws ExecException {
        return genericGetNext(DataType.DOUBLE);
    }

    @Override
    public Result getNextFloat() throws ExecException {
        return genericGetNext(DataType.FLOAT);
    }

    @Override
    public Result getNextInteger() throws ExecException {
        return genericGetNext(DataType.INTEGER);
    }

    @Override
    public Result getNextLong() throws ExecException {
        return genericGetNext(DataType.LONG);
    }

    @Override
    public Result getNextDateTime() throws ExecException {
        return genericGetNext(DataType.DATETIME);
    }

    @Override
    public Result getNextMap() throws ExecException {
        return genericGetNext(DataType.MAP);
    }

    @Override
    public Result getNextString() throws ExecException {
        return genericGetNext(DataType.CHARARRAY);
    }

    @Override
    public Result getNextTuple() throws ExecException {
        return genericGetNext(DataType.TUPLE);
    }

    @Override
    public Result getNextBigInteger() throws ExecException {
        return genericGetNext(DataType.BIGINTEGER);
    }

    @Override
    public Result getNextBigDecimal() throws ExecException {
        return genericGetNext(DataType.BIGDECIMAL);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitBinCond(this);
    }

    @Override
    public String name() {
        return "POBinCond" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    @Override
    public void attachInput(Tuple t) {
        cond.attachInput(t);
        lhs.attachInput(t);
        rhs.attachInput(t);
    }

    public void setCond(ExpressionOperator condOp) {
        this.cond = condOp;
    }

    public void setRhs(ExpressionOperator rhs) {
        this.rhs = rhs;
    }

    public void setLhs(ExpressionOperator lhs) {
        this.lhs = lhs;
    }

    /**
     * Get condition
     */
    public ExpressionOperator getCond() {
        return this.cond;
    }

    /**
     * Get right expression
     */
    public ExpressionOperator getRhs() {
        return this.rhs;
    }

    /**
     * Get left expression
     */
    public ExpressionOperator getLhs() {
        return this.lhs;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public POBinCond clone() throws CloneNotSupportedException {
        POBinCond clone = new POBinCond(new OperatorKey(mKey.scope,
                NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
        clone.cloneHelper(this);
        clone.cond = cond.clone();
        clone.lhs = lhs.clone();
        clone.rhs = rhs.clone();
        return clone;
    }

    /**
     * Get child expressions of this expression
     */
    @Override
    public List<ExpressionOperator> getChildExpressions() {
        if (child == null) {
            child = new ArrayList<ExpressionOperator>();
            child.add(cond);
            child.add(lhs);
            child.add(rhs);
        }
        return child;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null) {

        }
        return null;
    }
}
