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

import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;

public class POBinCond extends ExpressionOperator {
    ExpressionOperator cond;
    ExpressionOperator lhs;
    ExpressionOperator rhs;
    
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
    
    @Override
    public Result getNext(Boolean b) throws ExecException {
        Result res = cond.getNext(b);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(b) : rhs.getNext(b);
        
    }

    @Override
    public Result getNext(DataBag db) throws ExecException {
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(db) : rhs.getNext(db);
    }

    @Override
    public Result getNext(DataByteArray ba) throws ExecException {
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(ba) : rhs.getNext(ba);
    }

    @Override
    public Result getNext(Double d) throws ExecException {
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(d) : rhs.getNext(d);
    }

    @Override
    public Result getNext(Float f) throws ExecException {
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(f) : rhs.getNext(f);
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(i) : rhs.getNext(i);
    }

    @Override
    public Result getNext(Long l) throws ExecException {
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(l) : rhs.getNext(l);
    }

    @Override
    public Result getNext(Map m) throws ExecException {
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(m) : rhs.getNext(m);
    }

    @Override
    public Result getNext(String s) throws ExecException {
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(s) : rhs.getNext(s);
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(t) : rhs.getNext(t);
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

}
