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

import java.util.Iterator;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Implements the overloaded form of the project operator.
 * Projects the specified column from the input tuple.
 * However, if asked for tuples when the input is a bag,
 * the overloaded form is invoked and the project streams
 * the tuples through instead of the bag.
 */
public class POProject extends ExpressionOperator {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    //The column to project
    int column = 0;
    
    //True if we are in the middle of streaming tuples
    //in a bag
    boolean processingBagOfTuples = false;
    
    //The bag iterator used while straeming tuple
    Iterator<Tuple> bagIterator = null;
    
    //Represents the fact that this instance of POProject
    //is overloaded to stream tuples in the bag rather
    //than passing the entire bag. It is the responsibility
    //of the translator to set this.
    boolean overloaded = false;
    
    boolean star = false;
    
    public POProject(OperatorKey k) {
        this(k,-1,0);
    }

    public POProject(OperatorKey k, int rp) {
        this(k, rp, 0);
    }
    
    public POProject(OperatorKey k, int rp, int col) {
        super(k, rp);
        this.column = col;
    }

    @Override
    public String name() {
        
        return "Project" + "[" + DataType.findTypeName(resultType) + "]" +"(" + ((star) ? "*" : column) + ") - " + mKey.toString();
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
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitProject(this);
    }
    
    
    /**
     * Overridden since the attachment of the new input
     * should cause the old processing to end.
     */
    @Override
    public void attachInput(Tuple t) {
        super.attachInput(t);
        processingBagOfTuples = false;
    }
    
    /**
     * Fetches the input tuple and returns the requested
     * column
     * @return next value.
     * @throws ExecException
     */
    public Result getNext() throws ExecException{
        Result res = processInput();

        if(res.returnStatus != POStatus.STATUS_OK){
            return res;
        }
        res.result = ((Tuple)res.result).get(column);
        return res;
    }

    @Override
    public Result getNext(DataBag db) throws ExecException {
        Result res = processInputBag();
        if(res.returnStatus!=POStatus.STATUS_OK)
            return res;
        DataBag inpBag = (DataBag) res.result;

        if(isInputAttached() || star){
            res.result = inpBag;
            res.returnStatus = POStatus.STATUS_OK;
            detachInput();
            return res;
        }
        DataBag outBag = BagFactory.getInstance().newDefaultBag();
        for (Tuple tuple : inpBag) {
            Tuple tmpTuple = TupleFactory.getInstance().newTuple(tuple.get(column));
            outBag.add(tmpTuple);
        }
        res.result = outBag;
        res.returnStatus = POStatus.STATUS_OK;
        return res;
    }

    @Override
    public Result getNext(DataByteArray ba) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Double d) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Float f) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Long l) throws ExecException {
        return getNext();
    }
    
    

    @Override
    public Result getNext(Boolean b) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(Map m) throws ExecException {
        return getNext();
    }

    @Override
    public Result getNext(String s) throws ExecException {
        return getNext();
    }
    
    /**
     * Asked for Tuples. Check if the input is a bag.
     * If so, stream the tuples in the bag instead of
     * the entire bag.
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = new Result();
        if(!processingBagOfTuples){
            Tuple inpValue = null;
            res = processInput();
            if(res.returnStatus!=POStatus.STATUS_OK)
                return res;
            if(star)
                return res;
            
            inpValue = (Tuple)res.result;
            res.result = null;
            
            Object ret = inpValue.get(column);
            if(overloaded){
                DataBag retBag = (DataBag)ret;
                bagIterator = retBag.iterator();
                if(bagIterator.hasNext()){
                    processingBagOfTuples = true;
                    res.result = bagIterator.next();
                }
            }
            else {
                res.result = (Tuple)ret;
            }
            return res;
        }
        if(bagIterator.hasNext()){
            res.result = bagIterator.next();
            res.returnStatus = POStatus.STATUS_OK;
            return res;
        }
        else{
            //done processing the bag of tuples
            processingBagOfTuples = false;
            return getNext(t);
        }
    }

    public int getColumn() {
        return column;
    }

    public void setColumn(int column) {
        this.column = column;
    }

    public boolean isOverloaded() {
        return overloaded;
    }

    public void setOverloaded(boolean overloaded) {
        this.overloaded = overloaded;
    }

    public boolean isStar() {
        return star;
    }

    public void setStar(boolean star) {
        this.star = star;
    }
    
    private Result processInputBag() throws ExecException {
        
        Result res = new Result();
        if (input==null && (inputs == null || inputs.size()==0)) {
//            log.warn("No inputs found. Signaling End of Processing.");
            res.returnStatus = POStatus.STATUS_EOP;
            return res;
        }
        
        //Should be removed once the model is clear
        if(reporter!=null) reporter.progress();
        
        if(!isInputAttached())
            return inputs.get(0).getNext(dummyBag);
        else{
            res.result = (DataBag)input.get(column);
            res.returnStatus = POStatus.STATUS_OK;
            return res;
        }
    }

}
