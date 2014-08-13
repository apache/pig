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
package org.apache.pig.test;

import java.util.Iterator;
import java.util.List;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This operator is used to read tuples from a databag in memory. Used mostly
 * for testing. It'd also be useful for the example generator
 * 
 */
public class PORead extends PhysicalOperator {
    private static final long serialVersionUID = 1L;
    DataBag bag;
    transient Iterator<Tuple> it;

    public PORead(OperatorKey k) {
        super(k);
        // TODO Auto-generated constructor stub
    }

    public PORead(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        // TODO Auto-generated constructor stub
    }

    public PORead(OperatorKey k, int rp) {
        super(k, rp);
        // TODO Auto-generated constructor stub
    }

    public PORead(OperatorKey k, List<PhysicalOperator> inp) {
        super(k, inp);
        // TODO Auto-generated constructor stub
    }

    public PORead(OperatorKey k, DataBag bag) {
        super(k);
        this.bag = bag;
    }

    @Override
    public Result getNextTuple() {
        if (it == null) {
            it = bag.iterator();
        }
        Result res = new Result();
        if (it.hasNext()) {
            res.returnStatus = POStatus.STATUS_OK;
            res.result = it.next();
        } else {
            res.returnStatus = POStatus.STATUS_EOP;
        }
        return res;
    }

    @Override
    public String name() {
        // TODO Auto-generated method stub
        return "PORead - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        // TODO Auto-generated method stub
        throw new VisitorException("Vistor not accepted !");
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return null;
    }
}
