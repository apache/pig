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
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.SingleTupleBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A specialized local rearrange operator which behaves
 * like the regular local rearrange in the getNext() 
 * as far as getting its input and constructing the 
 * "key" out of the input. It then returns a tuple with
 * two fields - the key in the first position and the
 * "value" inside a bag in the second position. This output
 * format resembles the format out of a Package. This output
 * will feed to a foreach which expects this format.  
 */
public class POPreCombinerLocalRearrange extends PhysicalOperator {

    protected static final long serialVersionUID = 1L;

    protected static TupleFactory mTupleFactory = TupleFactory.getInstance();
    protected static BagFactory mBagFactory = BagFactory.getInstance();

    protected List<PhysicalPlan> plans;
    
    protected List<ExpressionOperator> leafOps;

    protected byte keyType;

    public POPreCombinerLocalRearrange(OperatorKey k) {
        this(k, -1, null);
    }

    public POPreCombinerLocalRearrange(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POPreCombinerLocalRearrange(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POPreCombinerLocalRearrange(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        leafOps = new ArrayList<ExpressionOperator>();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitPreCombinerLocalRearrange(this);
    }

    @Override
    public String name() {
        return "Pre Combiner Local Rearrange" + "[" + DataType.findTypeName(resultType) +
            "]" + "{" + DataType.findTypeName(keyType) + "} - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
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
            
            for (PhysicalPlan ep : plans) {
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
        
        Tuple output = mTupleFactory.newTuple(2);
        output.set(0, key);
        // put the value in a bag so that the initial
        // version of the Algebraics will get a bag as
        // they would expect.
        DataBag bg = new SingleTupleBag(value);
        output.set(1, bg);
        return output;
    }

    public byte getKeyType() {
        return keyType;
    }

    public void setKeyType(byte keyType) {
        this.keyType = keyType;
    }

    public List<PhysicalPlan> getPlans() {
        return plans;
    }

    public void setPlans(List<PhysicalPlan> plans) {
        this.plans = plans;
        leafOps.clear();
        for (PhysicalPlan plan : plans) {
            ExpressionOperator leaf = (ExpressionOperator)plan.getLeaves().get(0); 
            leafOps.add(leaf);
        }            
    }

}
