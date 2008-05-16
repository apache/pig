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
package org.apache.pig.impl.physicalLayer.topLevelOperators;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.ExprPlan;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanVisitor;

public class POGenerate extends PhysicalOperator<PhyPlanVisitor> {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private List<Boolean> isToBeFlattened;
    //private Boolean outputIsDataBag;
    private List<Tuple> outputBuffer = new LinkedList<Tuple>();
    private List<ExprPlan> inputPlans;
    private List<ExpressionOperator> inputs = new LinkedList<ExpressionOperator>();
    
    //its holds the iterators of the databags given by the input expressions which need flattening.
    Iterator<Tuple> [] its = null;
    
    //This holds the outputs given out by the input expressions of any datatype
    Object [] bags = null;
    
    //This is the template whcih contains tuples and is flattened out in CreateTuple() to generate the final output
    Object[] data = null;

    public POGenerate(OperatorKey k) {
        this(k, -1, null, null);
    }
    
    public POGenerate(OperatorKey k, int rp) {
        this(k, rp, null, null);
    }
    
    public POGenerate(OperatorKey k, List<ExprPlan> inp, List<Boolean> isToBeFlattened) {
        this(k, -1, inp, isToBeFlattened);
        
    }
    
    public POGenerate(OperatorKey k, int rp, List<ExprPlan> inp, List<Boolean>  isToBeFlattened) {
        super(k, rp);
        this.isToBeFlattened = isToBeFlattened;
        this.inputPlans = inp;
        getLeaves();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitGenerate(this);
    }

    @Override
    public String name() {
        String fString = getFlatStr();
        return "POGenerate" + "(" + fString + ")" + "  - " + mKey.toString();
    }

    private String getFlatStr() {
        if(isToBeFlattened==null)
            return "";
        StringBuilder sb = new StringBuilder();
        for (Boolean b : isToBeFlattened) {
            sb.append(b);
            sb.append(',');
        }
        if(sb.length()>0){
            sb.deleteCharAt(sb.length()-1);
        }
        return sb.toString();
    }

    /**
     * The idea is to generate permutations of all the bags. Something like trying to generate all possible n digit numbers.
     * We iterate through the units place. Then do a single iteration on the tens place. Then do the iterations on 
     * units place again. This we continue till all the n places have exhausted there digits.
     */    
    @Override
    public Result getNext(Tuple tIn) throws ExecException{
        int noItems = inputs.size();
        Result res = new Result();
        
        //We check if all the databags have exhausted the tuples. If so we enforce the reading of new data by setting data and its to null
        if(its != null) {
            boolean restartIts = true;
            for(int i = 0; i < noItems; ++i) {
                if(its[i] != null && isToBeFlattened.get(i) == true)
                    restartIts &= !its[i].hasNext();
            }
            //this means that all the databags have reached their last elements. so we need to force reading of fresh databags
            if(restartIts) {
                its = null;
                data = null;
            }
        }
        
        if(its == null) {
            //getNext being called for the first time OR starting with a set of new data from inputs 
            its = new Iterator[noItems];
            bags = new Object[noItems];
            
            for(int i = 0; i < noItems; ++i) {
                //Getting the iterators
                //populate the input data
                Result inputData = null;
                Byte resultType = ((PhysicalOperator)inputs.get(i)).resultType;
                switch(resultType) {
                case DataType.BAG : DataBag b = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(b);
                break;
                case DataType.TUPLE : Tuple t = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(t);
                break;
                case DataType.BYTEARRAY : DataByteArray db = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(db);
                break; 
                case DataType.MAP : Map map = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(map);
                break;
                case DataType.BOOLEAN : Boolean bool = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(bool);
                break;
                case DataType.INTEGER : Integer integer = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(integer);
                break;
                case DataType.DOUBLE : Double d = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(d);
                break;
                case DataType.LONG : Long l = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(l);
                break;
                case DataType.FLOAT : Float f = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(f);
                break;
                case DataType.CHARARRAY : String str = null;
                inputData = ((PhysicalOperator)inputs.get(i)).getNext(str);
                break;
                }
                
                if(inputData.returnStatus == POStatus.STATUS_EOP) {
                    //we are done with all the elements. Time to return.
                    its = null;
                    bags = null;
                    return inputData;
                }

                Object input = null;
                
                bags[i] = inputData.result;
                
                if(inputData.result instanceof DataBag && isToBeFlattened.get(i)) 
                    its[i] = ((DataBag)bags[i]).iterator();
                else 
                    its[i] = null;
                                
                
            }
        }
        
        Boolean done = false;
        while(!done) {
            if(data == null) {
                //getNext being called for the first time or starting on new input data
                //we instantiate the template array and start populating it with data
                data = new Object[noItems];
                for(int i = 0; i < noItems; ++i) {
                    if(isToBeFlattened.get(i) && bags[i] instanceof DataBag) {
                        if(its[i].hasNext()) {
                            data[i] = its[i].next();
                        } else {
                            //the input set is null, so we return
                            res.returnStatus = POStatus.STATUS_NULL;
                            return res;
                        }
                    } else {
                        data[i] = bags[i];
                    }
                    
                }
                
                //CreateTuple(data);
                res.result = CreateTuple(data);
                res.returnStatus = POStatus.STATUS_OK;
                return res;
            } else {
                //we try to find the last expression which needs flattening and start iterating over it
                //we also try to update the template array
                for(int index = noItems - 1; index >= 0; --index) {
                    if(its[index] != null && isToBeFlattened.get(index)) {
                        while(its[index].hasNext()) {
                            data[index] =  its[index].next();
                            res.result = CreateTuple(data);
                            res.returnStatus = POStatus.STATUS_OK;
                            return res;
                        }
                    }
                }
                //now since the last expression is exhausted, we goto the next tuples of the next expression which
                //needs flattening and also restart the iterator of the previously exhausted expression
                for(int j = noItems - 1; j >= 0; --j) {
                    if(its[j] != null) {
                        if(its[j].hasNext()) {
                            //we've found the next input which needs flattening and we update the template array.
                            data[j] = its[j].next();
                            res.result = CreateTuple(data);
                            res.returnStatus = POStatus.STATUS_OK;
                            return res;
                        } else {
                            //rewind the iterator for this particular databag
                            if(j == 0) {
                                //this is the first databag and means that we are done with the crossproduct. so we break out
                                done = true;
                                its = null;
                                data = null;
                                break;
                            }
                            its[j] = ((DataBag)bags[j]).iterator();
                            data[j] = its[j].next();
                        }
                    }
                }
            }
        }
        
        return null;
        
    }
    
    /**
     * 
     * @param data array that is the template for the final flattened tuple
     * @return the final flattened tuple
     */
    private Tuple CreateTuple(Object[] data) throws ExecException {
        TupleFactory tf = TupleFactory.getInstance();
        Tuple out = tf.newTuple();
        for(int i = 0; i < data.length; ++i) {
            Object in = data[i];
            
            if(in instanceof Tuple) {
                Tuple t = (Tuple)in;
                for(int j = 0; j < t.size(); ++j) {
                    out.append(t.get(j));
                }
            } else
                out.append(in);
        }
        return out;
    }

    @Override
    public void attachInput(Tuple t) {
        for(ExprPlan p : inputPlans) {
            p.attachInput(t);
        }
    }
    
    private void getLeaves() {
        for(ExprPlan p : inputPlans) {
            inputs.add(p.getLeaves().get(0));
        }
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public List<ExprPlan> getInputPlans() {
        return inputPlans;
    }
    


}
