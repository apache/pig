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

package org.apache.pig.impl.physicalLayer.relationalOperators;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This is a blocking operator. All the input is put in the hashset implemented
 * in DistinctDataBag which also provides the other DataBag interfaces.
 * 
 * 
 */
public class PODistinct extends PhysicalOperator {

    private boolean inputsAccumulated = false;
    private DataBag distinctBag = BagFactory.getInstance().newDistinctBag();
    private final Log log = LogFactory.getLog(getClass());
    transient Iterator<Tuple> it;

    public PODistinct(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        // TODO Auto-generated constructor stub
    }

    public PODistinct(OperatorKey k, int rp) {
        super(k, rp);
        // TODO Auto-generated constructor stub
    }

    public PODistinct(OperatorKey k, List<PhysicalOperator> inp) {
        super(k, inp);
        // TODO Auto-generated constructor stub
    }

    public PODistinct(OperatorKey k) {
        super(k);
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean isBlocking() {
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        if (!inputsAccumulated) {
            Result in = processInput();
            while (in.returnStatus != POStatus.STATUS_EOP) {
                if (in.returnStatus == POStatus.STATUS_ERR) {
                    log.error("Error in reading from inputs");
                    continue;
                } else if (in.returnStatus == POStatus.STATUS_NULL) {
                    continue;
                }
                distinctBag.add((Tuple) in.result);
                in = processInput();
            }
            inputsAccumulated = true;
        }
        if (it == null) {
            it = distinctBag.iterator();
        }
        res.result = it.next();
        if (res.result == null)
            res.returnStatus = POStatus.STATUS_EOP;
        else
            res.returnStatus = POStatus.STATUS_OK;
        return res;

    }

    @Override
    public String name() {
        // TODO Auto-generated method stub
        return "PODistinct" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
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
        v.visitDistinct(this);
    }

}
