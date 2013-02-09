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
import java.util.Map;

import org.joda.time.DateTime;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
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

    public Result genericGetNext(Object obj, byte dataType) throws ExecException {
        List<ExpressionOperator> list = new ArrayList<ExpressionOperator>();
        list.add(cond);
        Result r = accumChild(list, dummyBool);

        if (r != null) {
            if (r.returnStatus != POStatus.STATUS_BATCH_OK) {
                return r;
            }
            list.clear();
            list.add(lhs);
            list.add(rhs);
            r = accumChild(list, obj, dataType);
            return r;
        }
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) {
            return res;
        }
        Result result = ((Boolean)res.result) == true ? lhs.getNext(obj, dataType) : rhs.getNext(obj, dataType);
        illustratorMarkup(null, result.result, ((Boolean)res.result) ? 0 : 1);
        return result;
    }

    @Override
    public Result getNext(Boolean b) throws ExecException {
        Result r = accumChild(null, b);
        if (r != null) {
            return r;
        }

        Result res = cond.getNext(b);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) {
            return res;
        }
        return ((Boolean)res.result) == true ? lhs.getNext(b) : rhs.getNext(b);

    }

    @Override
    public Result getNext(DataBag db) throws ExecException {
        return genericGetNext(db, DataType.BAG);
    }

    @Override
    public Result getNext(DataByteArray ba) throws ExecException {
        return genericGetNext(ba, DataType.BYTEARRAY);
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
    public Result getNext(DateTime dt) throws ExecException {
        return genericGetNext(dt, DataType.DATETIME);
    }

    @Override
    public Result getNext(Map m) throws ExecException {
        return genericGetNext(m, DataType.MAP);
    }

    @Override
    public Result getNext(String s) throws ExecException {
        return genericGetNext(s, DataType.CHARARRAY);
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        return genericGetNext(t, DataType.TUPLE);
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
