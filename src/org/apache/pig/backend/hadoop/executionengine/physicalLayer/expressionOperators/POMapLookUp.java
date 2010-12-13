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

public class POMapLookUp extends ExpressionOperator {
    
    private static final long serialVersionUID = 1L;
    private String key;

    public POMapLookUp(OperatorKey k) {
        super(k);
    }
    
    public POMapLookUp(OperatorKey k, int rp) {
        super(k, rp);
    }
    
    public POMapLookUp(OperatorKey k, int rp, String key) {
        super(k, rp);
        this.key = key;
    }
    
    public void setLookUpKey(String key) {
        this.key = key;
    }
    
    public String getLookUpKey() {
        return key;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitMapLookUp(this);

    }

    @Override
    public String name() {
        return "POMapLookUp" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }
    
    @Override
    public Result processInput() throws ExecException {
        Result res = new Result();
        Map<String, Object> inpValue = null;
        if (input == null && (inputs == null || inputs.size()==0)) {
//            log.warn("No inputs found. Signaling End of Processing.");
            res.returnStatus = POStatus.STATUS_EOP;
            return res;
        }
        if (!isInputAttached())
            return inputs.get(0).getNext(inpValue);
        else {
            res.result = input;
            res.returnStatus = POStatus.STATUS_OK;
            detachInput();
            return res;
        }
    }
    
    @SuppressWarnings("unchecked")
    private Result getNext() throws ExecException {
        Result res = processInput();
        if(res.result != null && res.returnStatus == POStatus.STATUS_OK) {
            res.result = ((Map<String, Object>)res.result).get(key);
        }
        return res;
    }

    @Override
    public Result getNext(Boolean b) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(DataBag db) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(DataByteArray ba) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Double d) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Float f) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Long l) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Map m) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(String s) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        return getNext();
    }

    @Override
    public POMapLookUp clone() throws CloneNotSupportedException {
        POMapLookUp clone = new POMapLookUp(new OperatorKey(mKey.scope, 
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)), -1, key);
        clone.cloneHelper(this);
        return clone;
    }

    @Override
    public List<ExpressionOperator> getChildExpressions() {		
        return null;
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return (Tuple) out;
    }
}
