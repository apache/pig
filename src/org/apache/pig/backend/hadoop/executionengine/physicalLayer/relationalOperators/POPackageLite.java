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

/**
 * 
 */
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.impl.util.Pair;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.ReadOnceBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

/**
 * This package operator is a specialization
 * of POPackage operator used for the specific
 * case of the order by query. See JIRA 802 
 * for more details. 
 */
public class POPackageLite extends POPackage {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public POPackageLite(OperatorKey k) {
        super(k, -1, null);
    }

    public POPackageLite(OperatorKey k, int rp) {
        super(k, rp, null);
    }

    public POPackageLite(OperatorKey k, List<PhysicalOperator> inp) {
        super(k, -1, inp);
    }

    public POPackageLite(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    /* (non-Javadoc)
     * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage#setNumInps(int)
     */
	@Override
    public void setNumInps(int numInps) {
		if(numInps != 1)
		{
			throw new RuntimeException("POPackageLite can only take 1 input");
		}
        this.numInputs = numInps;
    }
	
    public boolean[] getInner() {
        throw new RuntimeException("POPackageLite does not support getInner operation");
    }

    public void setInner(boolean[] inner) {
        throw new RuntimeException("POPackageLite does not support setInner operation");
    }
    
    /**
     * Make a deep copy of this operator.  
     * @throws CloneNotSupportedException
     */
    @Override
    public POPackageLite clone() throws CloneNotSupportedException {
        POPackageLite clone = (POPackageLite)super.clone();
        clone.inner = null;
        clone.keyInfo = new HashMap<Integer, Pair<Boolean,Map<Integer,Integer>>>();
        for (Entry<Integer, Pair<Boolean, Map<Integer,Integer>>> entry: keyInfo.entrySet()) {
            clone.keyInfo.put(entry.getKey(), entry.getValue());
        }
        return clone;
    }
    
    /**
     * @return the distinct
     */
    @Override
    public boolean isDistinct() {
        throw new RuntimeException("POPackageLite does not support isDistinct operation");
    }

    /**
     * @param distinct the distinct to set
     */
    @Override
    public void setDistinct(boolean distinct) {
        throw new RuntimeException("POPackageLite does not support setDistinct operation");
    }

    /**
     * @return the isKeyTuple
     */
    public boolean getKeyTuple() {
        return isKeyTuple;
    }

    /**
     * @return the keyAsTuple
     */
    public Tuple getKeyAsTuple() {
        return keyAsTuple;
    }

    /**
     * @return the tupIter
     */
    public Iterator<NullableTuple> getTupIter() {
        return tupIter;
    }

    /**
     * @return the key
     */
    public Object getKey() {
        return key;
    }

    /**
     * Similar to POPackage.getNext except that
     * only one input is expected with index 0 
     * and ReadOnceBag is used instead of 
     * DefaultDataBag.
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Tuple res;
        //Create numInputs bags
        ReadOnceBag db = null;
        db = new ReadOnceBag(this, tupIter, key);
        if(reporter!=null) reporter.progress();
        
        //Construct the output tuple by appending
        //the key and all the above constructed bags
        //and return it.
        res = mTupleFactory.newTuple(numInputs+1);
        res.set(0,key);
        res.set(1,db);
        detachInput();
        Result r = new Result();
        r.returnStatus = POStatus.STATUS_OK;
        r.result = illustratorMarkup(null, res, 0);
        return r;
    }
    
    /**
     * Makes use of the superclass method, but this requires 
     * an additional parameter key passed by ReadOnceBag.
     * key of this instance will be set to null in detachInput 
     * call, but an instance of ReadOnceBag may have the original 
     * key that it uses. Therefore this extra argument is taken
     * to temporarily set it before the call to the superclass method 
     * and then restore it.  
     */
    public Tuple getValueTuple(NullableTuple ntup, int index, Object key) throws ExecException {
        Object origKey = this.key;
        this.key = key;
        Tuple retTuple = super.getValueTuple(ntup, index);
        this.key = origKey;
        return retTuple;
    }

    @Override
    public String name() {
        return getAliasString() + "PackageLite" + "["
                + DataType.findTypeName(resultType) + "]" + "{"
                + DataType.findTypeName(keyType) + "}" + " - "
                + mKey.toString();
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null) {
            ExampleTuple tOut = new ExampleTuple((Tuple) out);
            LineageTracer lineageTracer = illustrator.getLineage();
            lineageTracer.insert(tOut);
            if (illustrator.getEquivalenceClasses() == null) {
                LinkedList<IdentityHashSet<Tuple>> equivalenceClasses = new LinkedList<IdentityHashSet<Tuple>>();
                for (int i = 0; i < numInputs; ++i) {
                    IdentityHashSet<Tuple> equivalenceClass = new IdentityHashSet<Tuple>();
                    equivalenceClasses.add(equivalenceClass);
                }
                illustrator.setEquivalenceClasses(equivalenceClasses, this);
            }
            illustrator.getEquivalenceClasses().get(eqClassIndex).add(tOut);
            tOut.synthetic = false;  // not expect this to be really used
            illustrator.addData((Tuple) tOut);
            return tOut;
        } else
            return (Tuple) out;
    }
}

