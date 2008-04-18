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
package org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.ExprPlanVisitor;
import org.apache.pig.backend.executionengine.ExecException;


/**
 * This class implements a Constant of any type.
 * Its value can be set using the setValue method. 
 *
 */
public class ConstantExpression extends ExpressionOperator {
    
    private Log log = LogFactory.getLog(getClass());
    
    //The value that this constant represents
    Object value;
    
    //The result of calling getNext
    Result res = new Result();

    public ConstantExpression(OperatorKey k) {
        this(k,-1);
    }

    public ConstantExpression(OperatorKey k, int rp) {
        super(k, rp);
    }

    @Override
    public String name() {
        return "Constant(" + value.toString() +") - " + mKey.toString();
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
    public void visit(ExprPlanVisitor v) throws VisitorException {
        v.visitConstant(this);
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public Result getNext(DataBag db) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (DataBag)value;
        return res;
    }

    @Override
    public Result getNext(DataByteArray ba) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (DataByteArray)value;
        return res;
    }

    @Override
    public Result getNext(Double d) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (Double)value;
        return res;
    }

    @Override
    public Result getNext(Float f) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (Float)value;
        return res;
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (Integer)value;
        return res;
    }

    @Override
    public Result getNext(Long l) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (Long)value;
        return res;
    }

    @Override
    public Result getNext(String s) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (String)value;
        return res;
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (Tuple)value;
        return res;
    }
    
    
    
    @Override
    public Result getNext(Boolean b) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (Boolean)value;
        return res;
    }

    @Override
    public Result getNext(Map m) throws ExecException {
        
        res.returnStatus = POStatus.STATUS_OK;
        res.result = (Map)value;
        return res;
    }
}
