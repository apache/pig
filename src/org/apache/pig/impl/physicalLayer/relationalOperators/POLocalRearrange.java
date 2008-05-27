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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.ExprPlan;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The local rearrange operator is a part of the co-group
 * implementation. It has an embedded physical plan that
 * generates tuples of the form (grpKey,(indxed inp Tuple)).
 *
 */
public class POLocalRearrange extends PhysicalOperator<PhyPlanVisitor> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private Log log = LogFactory.getLog(getClass());

    List<ExprPlan> plans;
    
    List<ExpressionOperator> leafOps;

    // The position of this LR in the package operator
    int index;
    
    byte keyType;

    public POLocalRearrange(OperatorKey k) {
        this(k, -1, null);
    }

    public POLocalRearrange(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POLocalRearrange(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POLocalRearrange(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        index = -1;
        leafOps = new ArrayList<ExpressionOperator>();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitLocalRearrange(this);
    }

    @Override
    public String name() {
        return "Local Rearrange - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
    
    /**
     * Overridden since the attachment of the new input should cause the old
     * processing to end.
     */
    @Override
    public void attachInput(Tuple t) {
        super.attachInput(t);
    }
    
    /**
     * Calls getNext on the generate operator inside the nested
     * physical plan. Converts the generated tuple into the proper
     * format, i.e, (key,indexedTuple(value))
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        
        Result inp = null;
        Result res = null;
        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR)
                break;
            if (inp.returnStatus == POStatus.STATUS_NULL)
                continue;
            
            for (ExprPlan ep : plans) {
                ep.attachInput((Tuple)inp.result);
            }
            List<Result> resLst = new ArrayList<Result>();
            for (ExpressionOperator op : leafOps){
                
                switch(op.getResultType()){
                case DataType.BAG:
                    res = op.getNext(dummyBag);
                    break;
                case DataType.BOOLEAN:
                    res = op.getNext(dummyBool);
                    break;
                case DataType.BYTEARRAY:
                    res = op.getNext(dummyDBA);
                    break;
                case DataType.CHARARRAY:
                    res = op.getNext(dummyString);
                    break;
                case DataType.DOUBLE:
                    res = op.getNext(dummyDouble);
                    break;
                case DataType.FLOAT:
                    res = op.getNext(dummyFloat);
                    break;
                case DataType.INTEGER:
                    res = op.getNext(dummyInt);
                    break;
                case DataType.LONG:
                    res = op.getNext(dummyLong);
                    break;
                case DataType.MAP:
                    res = op.getNext(dummyMap);
                    break;
                case DataType.TUPLE:
                    res = op.getNext(dummyTuple);
                    break;
                }
                if(res.returnStatus!=POStatus.STATUS_OK)
                    return new Result();
                resLst.add(res);
            }
            res.result = constructLROutput(resLst,(Tuple)inp.result);
            return res;
        }
        return inp;
    }
    
    private Tuple constructLROutput(List<Result> resLst, Tuple value) throws ExecException{
        //Construct key
        Object key;
        if(resLst.size()>1){
            Tuple t = TupleFactory.getInstance().newTuple(resLst.size());
            int i=-1;
            for(Result res : resLst)
                t.set(++i, res.result);
            key = t;
        }
        else{
            key = resLst.get(0).result;
        }
        
        //Create the indexed tuple out of the value
        //that is remaining in the input tuple
        IndexedTuple it = new IndexedTuple(value, index);
        
        //Put the key and the indexed tuple
        //in a tuple and return
        Tuple outPut = TupleFactory.getInstance().newTuple(2);
        outPut.set(0,key);
        outPut.set(1,it);
        return outPut;
    }

    public byte getKeyType() {
        return keyType;
    }

    public void setKeyType(byte keyType) {
        this.keyType = keyType;
    }

    public List<ExprPlan> getPlans() {
        return plans;
    }

    public void setPlans(List<ExprPlan> plans) {
        this.plans = plans;
        leafOps.clear();
        for (ExprPlan plan : plans) {
            leafOps.add(plan.getLeaves().get(0));
        }
    }
}
