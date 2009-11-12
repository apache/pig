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
    
    @Override
    public Result getNext(Boolean b) throws ExecException {
        Result r = accumChild(null, b);
        if (r != null) {
            return r;
        }
        
        Result res = cond.getNext(b);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(b) : rhs.getNext(b);
        
    }

    @Override
    public Result getNext(DataBag db) throws ExecException {
        List<ExpressionOperator> l = new ArrayList<ExpressionOperator>();
        l.add(cond);
        Result r = accumChild(l, dummyBool);
        
        if (r != null) {    	
            if (r.returnStatus != POStatus.STATUS_BATCH_OK) {
                return r;
            }
            l.clear();
            l.add(lhs);
            l.add(rhs);
            r = accumChild(l, db);
            return r;    		
        }
                        
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(db) : rhs.getNext(db);
    }

    @Override
    public Result getNext(DataByteArray ba) throws ExecException {
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
            r = accumChild(list, ba);
            return r;    		
        }
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(ba) : rhs.getNext(ba);
    }

    @Override
    public Result getNext(Double d) throws ExecException {
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
            r = accumChild(list, d);
            return r;    		
        }
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(d) : rhs.getNext(d);
    }

    @Override
    public Result getNext(Float f) throws ExecException {
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
            r = accumChild(list, f);
            return r;    		
        }
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(f) : rhs.getNext(f);
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
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
            r = accumChild(list, i);
            return r;    		
        }
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(i) : rhs.getNext(i);
    }

    @Override
    public Result getNext(Long l) throws ExecException {
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
            r = accumChild(list, l);
            return r;    		
        }
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(l) : rhs.getNext(l);
    }

    @Override
    public Result getNext(Map m) throws ExecException {
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
            r = accumChild(list, m);
            return r;    		
        }
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(m) : rhs.getNext(m);
    }

    @Override
    public Result getNext(String s) throws ExecException {
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
            r = accumChild(list, s);
            return r;    		
        }
        Result res = cond.getNext(dummyBool);
        if (res.result==null || res.returnStatus != POStatus.STATUS_OK) return res;
        return ((Boolean)res.result) == true ? lhs.getNext(s) : rhs.getNext(s);
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
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
            r = accumChild(list, t);
            return r;    		
        }
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

}
