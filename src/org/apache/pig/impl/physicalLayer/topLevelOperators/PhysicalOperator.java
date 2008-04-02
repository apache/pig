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

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * 
 * This is the base class for all operators. This supports a 
 * generic way of processing inputs which can be overridden by 
 * operators extending this class. The input model assumes that
 * it can either be taken from an operator or can be attached 
 * directly to this operator. Also it is assumed that inputs to an
 * operator are always in the form of a tuple.
 * 
 * For this pipeline rework, we assume a pull based model, i.e, 
 * the root operator is going to call getNext with the appropriate
 * type which initiates a cascade of getNext calls that unroll to
 * create input for the root operator to work on. 
 * 
 * Any operator that extends the PhysicalOperator, supports a getNext
 * with all the different types of parameter types. The concrete implementation
 * should use the result type of its input operator to decide the type of getNext's
 * parameter. This is done to avoid switch/case based on the type
 * as much as possible. The default is assumed to return an erroneus Result
 * corresponding to an unsupported operation on that type. So the operators need
 * to implement only those types that are supported.
 *
 * @param <V>
 */
public abstract class PhysicalOperator<V extends PhyPlanVisitor> extends Operator<V> {
    
    private Log log = LogFactory.getLog(getClass());
    
    static final long serialVersionUID = 1L;
    
    //The degree of parallelism requested
    protected int requestedParallelism;
    
    //The inputs that this operator will read data from
    protected List<PhysicalOperator<V>> inputs;
    
    //The outputs that this operator will write data to
    //Will be used to create Targeted tuples
    protected List<PhysicalOperator<V>> outputs;
    
    //The data type for the results of this operator
    protected byte resultType = DataType.TUPLE;
    
    //Specifies if the input has been directly attached
    protected boolean inputAttached = false;
    
    //If inputAttached is true, input is set to the input tuple
    protected Tuple input = null;
    
    //The result of performing the operation along with the output
    protected Result res = null;

    public PhysicalOperator(OperatorKey k) {
        this(k,-1,null);
    }
    
    public PhysicalOperator(OperatorKey k, int rp){
        this(k,rp,null);
    }
    
    public PhysicalOperator(OperatorKey k, List<PhysicalOperator<V>> inp){
        this(k,-1,inp);
    }
    
    public PhysicalOperator(OperatorKey k, int rp, List<PhysicalOperator<V>> inp){
        super(k);
        requestedParallelism = rp;
        inputs = inp;
        res = new Result();
    }

    public int getRequestedParallelism() {
        return requestedParallelism;
    }

    public void setRequestedParallelism(int requestedParallelism) {
        this.requestedParallelism = requestedParallelism;
    }
    
    public byte getResultType() {
        return resultType;
    }

    public void setResultType(byte resultType) {
        this.resultType = resultType;
    }
    
    public List<PhysicalOperator<V>> getInputs() {
        return inputs;
    }
    
    public void setInputs(List<PhysicalOperator<V>> inputs) {
        this.inputs = inputs;
    }
    
    public boolean isInputAttached() {
        return inputAttached;
    }

    public void setInputAttached(boolean inputAttached) {
        this.inputAttached = inputAttached;
    }
    
    /**
     * Shorts the input path of this operator by providing
     * the input tuple directly
     * @param t - The tuple that should be used as input
     */
    public void attachInput(Tuple t){
        input = t;
        this.inputAttached = true;
    }
    
    /**
     * Detaches any tuples that are attached
     *
     */
    public void detachInput(){
        input = null;
        this.inputAttached = false;
    }
    
    /**
     * A blocking operator should override this to return true.
     * Blocking operators are those that need the full bag before
     * operate on the tuples inside the bag. Example is the Global Rearrange.
     * Non-blocking or pipeline operators are those that work on
     * a tuple by tuple basis.
     * @return true if blocking and false otherwise
     */
    public boolean isBlocking(){
        return false;
    }
    
    /**
     * A generic method for parsing input that either returns 
     * the attached input if it exists or fetches it from its
     * predecessor. If special processing is required, this 
     * method should be overridden.
     * @return The Result object that results from processing the
     *             input
     * @throws ExecException
     */
    public Result processInput() throws ExecException{
        Result res = new Result();
        Tuple inpValue = null;
        if(input==null && inputs==null) {
            log.warn("No inputs found. Signaling End of Processing.");
            res.returnStatus = POStatus.STATUS_EOP;
            return res;
        }
        if(!isInputAttached())
            return inputs.get(0).getNext(inpValue);
        else{
            res.result = input;
            res.returnStatus = POStatus.STATUS_OK;
            detachInput();
            return res;
        }
    }
    
    public abstract void visit(V v) throws ParseException ;

    public Result getNext(Integer i) throws ExecException {
        return res;
    }
    
    
    public Result getNext(Long l) throws ExecException {
        return res;
    }

    
    public Result getNext(Double d) throws ExecException {
        return res;
    }

    
    public Result getNext(Float f) throws ExecException {
        return res;
    }

    
    public Result getNext(String s) throws ExecException {
        return res;
    }

    
    public Result getNext(DataByteArray ba) throws ExecException {
        return res;
    }
    
    public Result getNext(Map m) throws ExecException{
        return res;
    }
    
    public Result getNext(Boolean b) throws ExecException{
        return res;
    }

    
    public Result getNext(Tuple t) throws ExecException {
        return res;
    }

    
    public Result getNext(DataBag db) throws ExecException {
        return res;
    }

}
