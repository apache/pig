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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DistinctDataBag;
import org.apache.pig.data.InternalDistinctBag;
import org.apache.pig.data.InternalSortedBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;
import org.apache.pig.impl.util.IdentityHashSet;

/**
 * Find the distinct set of tuples in a bag.
 * This is a blocking operator. All the input is put in the hashset implemented
 * in DistinctDataBag which also provides the other DataBag interfaces.
 * 
 * 
 */
public class PODistinct extends PhysicalOperator implements Cloneable {
    private static final Log log = LogFactory.getLog(PODistinct.class);
    private static final long serialVersionUID = 1L;
    private boolean inputsAccumulated = false;
    private DataBag distinctBag = null;
    transient Iterator<Tuple> it;

    public PODistinct(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    public PODistinct(OperatorKey k, int rp) {
        super(k, rp);
    }

    public PODistinct(OperatorKey k, List<PhysicalOperator> inp) {
        super(k, inp);
    }

    public PODistinct(OperatorKey k) {
        super(k);
    }

    @Override
    public boolean isBlocking() {
        return true;
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
         if (!inputsAccumulated) {
            Result in = processInput();    
            
            // by default, we create InternalSortedBag, unless user configures
			// explicitly to use old bag
           	String bagType = null;
            if (PigMapReduce.sJobConfInternal.get() != null) {
       			bagType = PigMapReduce.sJobConfInternal.get().get("pig.cachedbag.distinct.type");       			
       	    }            
            if (bagType != null && bagType.equalsIgnoreCase("default")) {        	    	
            	distinctBag = BagFactory.getInstance().newDistinctBag();    			
       	    } else {
       	    	distinctBag = new InternalDistinctBag(3);
    	    }
            
            while (in.returnStatus != POStatus.STATUS_EOP) {
                if (in.returnStatus == POStatus.STATUS_ERR) {
                    log.error("Error in reading from inputs");
                    return in;
                    //continue;
                } else if (in.returnStatus == POStatus.STATUS_NULL) {
                    // Ignore the null, read the next tuple.  It's not clear
                    // to me that we should ever get this, or if we should, 
                    // how it differs from EOP.  But ignoring it here seems
                    // to work.
                    in = processInput();
                    continue;
                }
                distinctBag.add((Tuple) in.result);
                illustratorMarkup(in.result, in.result, 0);
                in = processInput();
            }
            inputsAccumulated = true;
        }
        if (it == null) {
            it = distinctBag.iterator();
        }
        res.result = it.next();
        if (res.result == null){
            res.returnStatus = POStatus.STATUS_EOP;
            reset();
        } else {
            res.returnStatus = POStatus.STATUS_OK;
        }
        return res;
    }

    @Override
    public String name() {
        return getAliasString() + "PODistinct" + "["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
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
    public void reset() {
        inputsAccumulated = false;
        distinctBag = null;
        it = null;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitDistinct(this);
    }
    /* (non-Javadoc)
     * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator#clone()
     */
    @Override
    public PODistinct clone() throws CloneNotSupportedException {
        // TODO Auto-generated method stub
        return new PODistinct(new OperatorKey(this.mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(this.mKey.scope)), this.requestedParallelism, this.inputs);
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null) {
            illustrator.getEquivalenceClasses().get(eqClassIndex).add((Tuple) out);
            illustrator.addData((Tuple) out);
        }
        return null;
    }
}
