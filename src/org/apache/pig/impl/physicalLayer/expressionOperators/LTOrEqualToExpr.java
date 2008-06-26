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
package org.apache.pig.impl.physicalLayer.expressionOperators;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.backend.executionengine.ExecException;

public class LTOrEqualToExpr extends BinaryComparisonOperator {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final Log log = LogFactory.getLog(getClass());

    public LTOrEqualToExpr(OperatorKey k) {
        this(k, -1);
    }

    public LTOrEqualToExpr(OperatorKey k, int rp) {
        super(k, rp);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visiLTOrEqual(this);
    }

    @Override
    public String name() {
        return "Less Than or Equal" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }

    @Override
    public Result getNext(DataByteArray ba) throws ExecException {
        byte status;
        Result res;
        DataByteArray left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        left = (DataByteArray) res.result;

        res = rhs.getNext(right);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        right = (DataByteArray) res.result;

        int ret = left.compareTo(right);
        if (ret == -1 || ret == 0) {
            res.result = new Boolean(true);
            //left = right = null;
            return res;
        } else {
            res.result = new Boolean(false);
            //left = right = null;
            return res;
        }
    }

    @Override
    public Result getNext(Double d) throws ExecException {
        byte status;
        Result res;
        Double left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        left = (Double) res.result;

        res = rhs.getNext(right);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        right = (Double) res.result;

        if (left <= right) {
            res.result = new Boolean(true);
            //left = right = null;
            return res;
        } else {
            res.result = new Boolean(false);
            //left = right = null;
            return res;
        }
    }

    @Override
    public Result getNext(Float f) throws ExecException {
        byte status;
        Result res;
        Float left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        left = (Float) res.result;

        res = rhs.getNext(right);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        right = (Float) res.result;

        if (left <= right) {
            res.result = new Boolean(true);
            //left = right = null;
            return res;
        } else {
            res.result = new Boolean(false);
            //left = right = null;
            return res;
        }
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        byte status;
        Result res;
        Integer left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        left = (Integer) res.result;

        res = rhs.getNext(right);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        right = (Integer) res.result;

        if (left <= right) {
            res.result = new Boolean(true);
            //left = right = null;
            return res;
        } else {
            res.result = new Boolean(false);
            //left = right = null;
            return res;
        }
    }

    @Override
    public Result getNext(Long l) throws ExecException {
        byte status;
        Result res;
        Long left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        left = (Long) res.result;

        res = rhs.getNext(right);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        right = (Long) res.result;

        if (left <= right) {
            res.result = new Boolean(true);
            //left = right = null;
            return res;
        } else {
            res.result = new Boolean(false);
            //left = right = null;
            return res;
        }
    }

    @Override
    public Result getNext(String s) throws ExecException {
        byte status;
        Result res;
        String left = null, right = null;
        res = lhs.getNext(left);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        left = (String) res.result;

        res = rhs.getNext(right);
        status = res.returnStatus;
        if (status != POStatus.STATUS_OK) {

            return res;
        }
        right = (String) res.result;

        int ret = left.compareTo(right);
        if (ret <= 0) {
            res.result = new Boolean(true);
            //left = right = null;
            return res;
        } else {
            res.result = new Boolean(false);
            //left = right = null;
            return res;
        }
    }
}
