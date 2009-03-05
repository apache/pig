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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;

public class Add extends BinaryExpressionOperator {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public Add(OperatorKey k) {
        super(k);
    }

    public Add(OperatorKey k, int rp) {
        super(k, rp);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitAdd(this);
    }

    @Override
    public String name() {
        return "Add" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    @Override
    public Result getNext(Double d) throws ExecException {
        byte status;
        Result res;
        Double left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        left = (Double) res.result;
        
        res = rhs.getNext(right);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        right = (Double) res.result;
        
        res.result = new Double(left + right);
        return res;
    }
    
    @Override
    public Result getNext(Float f) throws ExecException {
        byte status;
        Result res;
        Float left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        left = (Float) res.result;
        
        res = rhs.getNext(right);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        right = (Float) res.result;
        
        res.result = new Float(left + right);
        return res;
    }
    
    @Override
    public Result getNext(Integer i) throws ExecException {
        byte status;
        Result res;
        Integer left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        left = (Integer) res.result;
        
        res = rhs.getNext(right);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        right = (Integer) res.result;
        
        res.result = new Integer(left + right);
        return res;
    }
    
    @Override
    public Result getNext(Long l) throws ExecException {
        byte status;
        Result res;
        Long left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        left = (Long) res.result;
        
        res = rhs.getNext(right);
        status = res.returnStatus;
        if(status != POStatus.STATUS_OK || res.result == null) {
            return res;
        }
        right = (Long) res.result;
        
        res.result = new Long(left + right);
        return res;
    }

    @Override
    public Add clone() throws CloneNotSupportedException {
        Add clone = new Add(new OperatorKey(mKey.scope, 
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
        clone.cloneHelper(this);
        return clone;
    }
    

}
