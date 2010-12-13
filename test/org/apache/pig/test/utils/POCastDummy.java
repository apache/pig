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
package org.apache.pig.test.utils;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This is just a cast that converts DataByteArray into either
 * String or Integer. Just added it for testing the POUnion. 
 * Need the full operator implementation.
 */
public class POCastDummy extends ExpressionOperator {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public POCastDummy(OperatorKey k) {
        super(k);
        // TODO Auto-generated constructor stub
    }

    public POCastDummy(OperatorKey k, int rp) {
        super(k, rp);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        // TODO Auto-generated method stub

    }

    @Override
    public String name() {
        return "Cast - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        Result res = inputs.get(0).getNext(i);

        if(res.returnStatus != POStatus.STATUS_OK){
            return res;
        }
        
        if(res.result instanceof DataByteArray){
            String rslt = ((DataByteArray)res.result).toString();
            res.result = Integer.parseInt(rslt.trim());
            return res;
        }
        return new Result();
    }

    @Override
    public Result getNext(String s) throws ExecException {
        Result res = inputs.get(0).getNext(s);

        if(res.returnStatus != POStatus.STATUS_OK){
            return res;
        }
        
        if(res.result instanceof DataByteArray){
            String rslt = ((DataByteArray)res.result).toString();
            res.result = rslt;
            return res;
        }
        return new Result();
    }

    protected List<ExpressionOperator> getChildExpressions() {		
        return null;
    }    

    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return null;
    }
}
