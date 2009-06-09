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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The package operator that packages the globally rearranged tuples 
 * into output format as required by multi-query de-multiplexer.
 * <p>
 * This operator is used when merging multiple Map-Reduce splittees
 * into a Map-only splitter during multi-query optimization. 
 * The package operators of the reduce plans of the splittees form an
 * indexed package list inside this operator. When this operator 
 * receives an input, it extracts the index from the key and calls the 
 * corresponding package to get the output data.
 * <p>
 * Due to the recursive nature of multi-query optimization, this operator
 * may be contained in another multi-query packager.
 * <p>
 * The successor of this operator must be a PODemux operator which 
 * knows how to consume the output of this operator.
 */
public class POMultiQueryPackage extends POPackage {
    
    private static final long serialVersionUID = 1L;
    
    private static int idxPart = 0x7F;

    private final Log log = LogFactory.getLog(getClass());
    
    private List<POPackage> packages = new ArrayList<POPackage>();

    private PigNullableWritable myKey;
    
    private int baseIndex = 0;      

    /**
     * Constructs an operator with the specified key.
     * 
     * @param k the operator key
     */
    public POMultiQueryPackage(OperatorKey k) {
        this(k, -1, null);
    }

    /**
     * Constructs an operator with the specified key
     * and degree of parallelism.
     *  
     * @param k the operator key
     * @param rp the degree of parallelism requested
     */
    public POMultiQueryPackage(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    /**
     * Constructs an operator with the specified key and inputs.
     *  
     * @param k the operator key
     * @param inp the inputs that this operator will read data from
     */
    public POMultiQueryPackage(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    /**
     * Constructs an operator with the specified key,
     * degree of parallelism and inputs.
     * 
     * @param k the operator key
     * @param rp the degree of parallelism requested 
     * @param inp the inputs that this operator will read data from
     */
    public POMultiQueryPackage(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    @Override
    public String name() {
        return "MultiQuery Package[" + baseIndex +"] - " +  getOperatorKey().toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitMultiQueryPackage(this);
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }
    
    @Override
    public void attachInput(PigNullableWritable k, Iterator<NullableTuple> inp) {
        tupIter = inp;
        myKey = k;
    }

    @Override
    public void detachInput() {
        tupIter = null;
        myKey = null;
    }

    /**
     * Appends the specified package object to the end of 
     * the package list.
     * 
     * @param pack package to be appended to the list
     */
    public void addPackage(POPackage pack) {
        packages.add(pack);        
    }

    /**
     * Returns the list of packages.
     *  
     * @return the list of the packages
     */
    public List<POPackage> getPackages() {
        return packages;
    }

    /**
     * Constructs the output tuple from the inputs.
     * <p> 
     * The output is consumed by for the demultiplexer operator 
     * (PODemux) in the format (key, {bag of tuples}) where key 
     * is an indexed WritableComparable, not the wrapped value as a pig type.
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        
        int index = myKey.getIndex();
        index &= idxPart;
        index -= baseIndex;
        
        if (index >= packages.size() || index < 0) {
            int errCode = 2140;
            String msg = "Invalid package index " + index 
                + " should be in the range between 0 and " + packages.size();
            throw new ExecException(msg, errCode, PigException.BUG);
        }
               
        POPackage pack = packages.get(index);

        pack.attachInput(myKey, tupIter);

        Result res = pack.getNext(t);
        
        Tuple tuple = (Tuple)res.result;

        // the key present in the first field
        // of the tuple above is the real key without
        // index information - this is because the
        // package above, extracts the real key out of
        // the PigNullableWritable key - we are going to
        // give this result tuple to a PODemux operator
        // which needs a PigNullableWritable key so
        // it can figure out the index - we already have
        // the PigNullableWritable key cachec in "myKey"
        // let's send this in the result tuple
        tuple.set(0, myKey);

        return res;
    }

    /**
     * Sets the base index of this operator
     * 
     * @param baseIndex the base index of this operator
     */
    public void setBaseIndex(int baseIndex) {
        this.baseIndex = baseIndex;
    }

    /**
     * Returns the base index of this operator
     * 
     * @return the base index of this operator
     */
    public int getBaseIndex() {
        return baseIndex;
    }      
}
