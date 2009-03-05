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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The local rearrange operator is a part of the co-group
 * implementation. It has an embedded physical plan that
 * generates tuples of the form (grpKey,(indxed inp Tuple)).
 *
 */
public class POLocalRearrangeForIllustrate extends POLocalRearrange {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public POLocalRearrangeForIllustrate(OperatorKey k) {
        this(k, -1, null);
    }

    public POLocalRearrangeForIllustrate(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POLocalRearrangeForIllustrate(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POLocalRearrangeForIllustrate(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        index = -1;
        leafOps = new ArrayList<ExpressionOperator>();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitLocalRearrangeForIllustrate(this);
    }

    @Override
    public String name() {
        return "Local Rearrange For Illustrate" + "[" + DataType.findTypeName(resultType) +
            "]" + "{" + DataType.findTypeName(keyType) + "}" + "(" +
            mIsDistinct + ") - " + mKey.toString();
    }

    protected Tuple constructLROutput(List<Result> resLst, Tuple value) throws ExecException{
        //Construct key
        Object key;
        if(resLst.size()>1){
            Tuple t = mTupleFactory.newTuple(resLst.size());
            int i=-1;
            for(Result res : resLst)
                t.set(++i, res.result);
            key = t;
        }
        else{
            key = resLst.get(0).result;
        }
        
        Tuple output = mTupleFactory.newTuple(3);
        if (mIsDistinct) {

            //Put the key and the indexed tuple
            //in a tuple and return
            output.set(0, new Byte((byte)0));
            output.set(1, key);
            output.set(2, mFakeTuple);
            return output;
        } else {
            if(isCross){
                for(int i=0;i<plans.size();i++)
                    value.getAll().remove(0);
            }

            //Put the index, key, and value
            //in a tuple and return
            output.set(0, new Byte(index));
            output.set(1, key);
            output.set(2, value);
            return output;
        }
    }

    /**
     * Make a deep copy of this operator.  
     * @throws CloneNotSupportedException
     */
    @Override
    public POLocalRearrangeForIllustrate clone() throws CloneNotSupportedException {
        List<PhysicalPlan> clonePlans = new
            ArrayList<PhysicalPlan>(plans.size());
        for (PhysicalPlan plan : plans) {
            clonePlans.add(plan.clone());
        }
        POLocalRearrangeForIllustrate clone = new POLocalRearrangeForIllustrate(new OperatorKey(
            mKey.scope, 
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)),
            requestedParallelism);
        try {
            clone.setPlans(clonePlans);
        } catch (PlanException pe) {
            CloneNotSupportedException cnse = new CloneNotSupportedException("Problem with setting plans of " + this.getClass().getSimpleName());
            cnse.initCause(pe);
            throw cnse;
        }
        clone.keyType = keyType;
        clone.index = index;
        // Needs to be called as setDistinct so that the fake index tuple gets
        // created.
        clone.setDistinct(mIsDistinct);
        return clone;
    }

}
