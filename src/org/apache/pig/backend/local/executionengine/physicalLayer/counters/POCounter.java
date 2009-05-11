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

package org.apache.pig.backend.local.executionengine.physicalLayer.counters;

import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POCounter extends PhysicalOperator {

	static final long serialVersionUID = 1L;

    private long count = 0;
    
    public POCounter(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        // TODO Auto-generated constructor stub
    }

    public POCounter(OperatorKey k, int rp) {
        super(k, rp);
        // TODO Auto-generated constructor stub
    }

    public POCounter(OperatorKey k, List<PhysicalOperator> inp) {
        super(k, inp);
        // TODO Auto-generated constructor stub
    }

    public POCounter(OperatorKey k) {
        super(k);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        // TODO Auto-generated method stub

    }

    
    @Override
    public Result getNext(Tuple t) throws ExecException {
        // TODO Auto-generated method stub
        return getNext(processInput());
    }
    
    private Result getNext(Result res) {
        //System.out.println("Status = " + res.returnStatus);
        if(res.returnStatus == POStatus.STATUS_OK) {
            //System.out.println("Incrementing counter");
            count++;
        }
        return res;
    }

    @Override
    public String name() {
        // TODO Auto-generated method stub
        return "POCounter - " + mKey.toString();
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
    
    public long getCount() {
        return count;
    }

}
