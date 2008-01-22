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
package org.apache.pig.impl.physicalLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.log4j.Logger;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.StarSpec;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.mapreduceExec.MapReduceLauncher;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.PigLogger;

public class POMapreduce extends PhysicalOperator {
	private static final long serialVersionUID = 1L;
	
    public ArrayList<EvalSpec> toMap             = new ArrayList<EvalSpec>();
    //public ArrayList<EvalSpec>     toCombine         = null;
    public EvalSpec     toCombine         = null;
    public EvalSpec    toReduce          = null;
    public ArrayList<EvalSpec>  groupFuncs           = null;
    public SplitSpec        toSplit           = null;
    public ArrayList<FileSpec>           inputFileSpecs         = new ArrayList<FileSpec>();
    public FileSpec         outputFileSpec        = null;
    public Class           partitionFunction = null;
    public Class<WritableComparator> userComparator = null;
    public String quantilesFile = null;
    public PigContext pigContext;
    
    public int                     mapParallelism       = -1;     // -1 means let hadoop decide
    public int                     reduceParallelism    = -1;

    
    static MapReduceLauncher mapReduceLauncher = new MapReduceLauncher();

    
    public boolean doesGrouping() {
        return !(groupFuncs == null);
    }
    
    public boolean doesSplitting() {
    	return toSplit !=null;
    }
    
    public boolean doesSorting() {
    	return quantilesFile!=null;
    }

    public int numInputFiles() {
        if (inputFileSpecs == null)
            return 0;
        else
            return inputFileSpecs.size();
    }

    public POMapreduce(PigContext pigContext, int mapParallelism, int reduceParallelism) {
        this(pigContext);
        this.mapParallelism = mapParallelism;
        this.reduceParallelism = reduceParallelism;
    }

    public POMapreduce(PigContext pigContext, PhysicalOperator[] inputsIn) {
    	this.pigContext = pigContext;
        inputs = inputsIn;
    }

    public POMapreduce(PigContext pigContext, PhysicalOperator inputIn) {
    	this.pigContext = pigContext;
        inputs = new PhysicalOperator[1];
        inputs[0] = inputIn;
    }

    public POMapreduce(PigContext pigContext) {
    	this.pigContext = pigContext;
        inputs = new PhysicalOperator[0];
    }

    public void addInputOperator(PhysicalOperator newInput) {
        PhysicalOperator[] oldInputs = inputs;
        inputs = new PhysicalOperator[oldInputs.length + 1];
        for (int i = 0; i < oldInputs.length; i++)
            inputs[i] = oldInputs[i];
        inputs[inputs.length - 1] = newInput;
    }

    public void addInputOperators(PhysicalOperator[] newInputs) {
        for (int i = 0; i < newInputs.length; i++)
            addInputOperator(newInputs[i]);
    }
    
    public void addInputFile(FileSpec fileSpec){
    	addInputFile(fileSpec, new StarSpec());
    }
    
    public void addInputFile(FileSpec fileSpec, EvalSpec evalSpec){
    	inputFileSpecs.add(fileSpec);
    	toMap.add(evalSpec);
    }
    
    
    @Override
	public boolean open(boolean continueFromLast) throws IOException {
        // first, call open() on all inputs
        if (inputs != null) {
            for (int i = 0; i < inputs.length; i++) {
                if (!inputs[i].open(continueFromLast))
                    return false;
            }
        }

        // then, have hadoop run this MapReduce job:
        if (pigContext.debug) print();
        boolean success = mapReduceLauncher.launchPig(this);
        if (!success) {
            // XXX: If we throw an exception on failure, why do we return a boolean ?!? - ben
            throw new IOException("Job failed");
        }
        return success;
    }

    @Override
	public Tuple getNext() throws IOException {
        // drain all inputs
        for (int i = 0; i < inputs.length; i++) {
            while (inputs[i].getNext() != null)
                ;
        }

        // indicate that we are done
        return null;
    }
    
    public int numMRJobs() {
        int numInputJobs = 0;
        if (inputs != null) {
            for (PhysicalOperator i : inputs) {
                numInputJobs += ((POMapreduce) i).numMRJobs();
            }
        }
        return 1 + numInputJobs;
    }

    void print() {
        Logger log = PigLogger.getLogger();
        log.debug("Input: " + inputFileSpecs);
        log.debug("Map: " + toMap);
        log.debug("Group: " + groupFuncs);
        log.debug("Combine: " + toCombine);
        log.debug("Reduce: " + toReduce);
        log.debug("Output: " + outputFileSpec);
        log.debug("Split: " + toSplit);
        log.debug("Map parallelism: " + mapParallelism);
        log.debug("Reduce parallelism: " + reduceParallelism);
    }
    
    public POMapreduce copy(){
    	try{
    		POMapreduce copy = ((POMapreduce)ObjectSerializer.deserialize(ObjectSerializer.serialize(this)));
    		copy.pigContext = pigContext;
    		copy.inputs = inputs;
    		return copy;
    	}catch(IOException e){
    		throw new RuntimeException(e);
    	}
    }
    
    public EvalSpec getEvalSpec(int i){
    	return toMap.get(i);
    }
    
    public FileSpec getFileSpec(int i){
    	return inputFileSpecs.get(i);
    }
    
    public void addMapSpec(int i, EvalSpec spec){
    	if (toMap.get(i) == null)
    		toMap.set(i, spec);
    	else
    		toMap.set(i, toMap.get(i).addSpec(spec));
    }
    
    public void addReduceSpec(EvalSpec spec){
    	if (toReduce == null)
    		toReduce = spec;
    	else
    		toReduce = toReduce.addSpec(spec);
    }

    public void visit(POVisitor v) {
        v.visitMapreduce(this);
    }
    
}


