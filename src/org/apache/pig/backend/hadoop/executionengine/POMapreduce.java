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
package org.apache.pig.backend.hadoop.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.mapreduceExec.MapReduceLauncher;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.StarSpec;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POVisitor;
import org.apache.pig.impl.util.ObjectSerializer;

public class POMapreduce extends PhysicalOperator {
    private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());
    
    public ArrayList<EvalSpec> toMap             = new ArrayList<EvalSpec>();
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
    public Properties properties = new Properties();
    
    public OperatorKey sourceLogicalKey;
    
    public int                     mapParallelism       = -1;     // -1 means let hadoop decide
    public int                     reduceParallelism    = -1;

    /**
     * A list of configs to be merged, not overwritten ...
     */
    private static String[] PIG_CONFIGS_TO_MERGE = 
        { 
            "pig.streaming.cache.files",
            "pig.streaming.ship.files",
        };
    
    static MapReduceLauncher mapReduceLauncher = new MapReduceLauncher();

    public String getScope() {
        return scope;
    }
    
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

    public POMapreduce(String scope, 
                       long id, 
                       Map<OperatorKey, ExecPhysicalOperator> opTable,
                       OperatorKey sourceLogicalKey,
                       PigContext pigContext, 
                       int mapParallelism, 
                       int reduceParallelism) {
        this(scope, id, opTable, sourceLogicalKey, pigContext);
        this.mapParallelism = mapParallelism;
        this.reduceParallelism = reduceParallelism;
    }

    public POMapreduce(String scope, 
                       long id, 
                       Map<OperatorKey, ExecPhysicalOperator> opTable,
                       OperatorKey sourceLogicalKey,
                       PigContext pigContext, 
                       OperatorKey[] inputsIn) {
        super(scope, id, opTable, LogicalOperator.FIXED);
        this.sourceLogicalKey = sourceLogicalKey;
        this.pigContext = pigContext;
        inputs = inputsIn;
    }

    public POMapreduce(String scope, 
                       long id,
                       Map<OperatorKey, ExecPhysicalOperator> opTable,
                       OperatorKey sourceLogicalKey,
                       PigContext pigContext, 
                       OperatorKey inputIn) {
        super(scope, id, opTable, LogicalOperator.FIXED);
        this.sourceLogicalKey = sourceLogicalKey;
        this.pigContext = pigContext;
        inputs = new OperatorKey[1];
        inputs[0] = inputIn;
    }

    public POMapreduce(String scope, 
                       long id, 
                       Map<OperatorKey, ExecPhysicalOperator> opTable,
                       OperatorKey sourceLogicalKey,
                       PigContext pigContext) {
        super(scope, id, opTable, LogicalOperator.FIXED);
        this.sourceLogicalKey = sourceLogicalKey;
        this.pigContext = pigContext;
        inputs = new OperatorKey[0];
    }

    public void addInputOperator(OperatorKey newInput) {
        OperatorKey[] oldInputs = inputs;
        inputs = new OperatorKey[oldInputs.length + 1];
        for (int i = 0; i < oldInputs.length; i++)
            inputs[i] = oldInputs[i];
        inputs[inputs.length - 1] = newInput;
    }
    
    public void addInputOperators(OperatorKey[] newInputs) {
        for (int i = 0; i < newInputs.length; i++) {
            addInputOperator(newInputs[i]);
        }
    }
    
    public void addInputFile(FileSpec fileSpec){
        addInputFile(fileSpec, new StarSpec());
    }
    
    public void addInputFile(FileSpec fileSpec, EvalSpec evalSpec){
        inputFileSpecs.add(fileSpec);
        toMap.add(evalSpec);
        mergeProperties(evalSpec.getProperties());
    }
    
    
    @Override
    public boolean open() throws IOException {
        // first, call open() on all inputs
        if (inputs != null) {
            for (int i = 0; i < inputs.length; i++) {
                if (!((PhysicalOperator)opTable.get(inputs[i])).open())
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
            while (((PhysicalOperator)opTable.get(inputs[i])).getNext() != null)
                ;
        }

        // indicate that we are done
        return null;
    }
    
    public int numMRJobs() {
        int numInputJobs = 0;
        if (inputs != null) {
            for (OperatorKey i : inputs) {
                numInputJobs += ((POMapreduce) opTable.get(i)).numMRJobs();
            }
        }
        return 1 + numInputJobs;
    }

    void print() {
        log.info("----- MapReduce Job -----");
        log.info("Input: " + inputFileSpecs);
        log.info("Map: " + toMap);
        log.info("Group: " + groupFuncs);
        log.info("Combine: " + toCombine);
        log.info("Reduce: " + toReduce);
        log.info("Output: " + outputFileSpec);
        log.info("Split: " + toSplit);
        log.info("Map parallelism: " + mapParallelism);
        log.info("Reduce parallelism: " + reduceParallelism);
    }
    
    public POMapreduce copy(long id){
        try{
            Map<OperatorKey, ExecPhysicalOperator> srcOpTable = this.opTable;
            this.opTable = null;
            
            POMapreduce copy = ((POMapreduce)ObjectSerializer.deserialize(ObjectSerializer.serialize(this)));
            
            copy.pigContext = pigContext;
            copy.inputs = inputs;
            copy.opTable = srcOpTable;
            copy.id = id;
            copy.properties = properties;
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
        
        mergeProperties(spec.getProperties());
    }
    
    public void addReduceSpec(EvalSpec spec){
        if (toReduce == null)
            toReduce = spec;
        else
            toReduce = toReduce.addSpec(spec);
        
        mergeProperties(spec.getProperties());
    }
    
    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }
    
    public String getProperty(String key) {
        return properties.getProperty(key);
    }
    
    public void visit(POVisitor v) {
        v.visitMapreduce(this);
    }
    
    // TODO: Ugly hack! Need a better way to manage multiple properties 
    // Presumably it should be a part of Hadoop Configuration.
    private void mergeProperties(Properties other) {
        Properties mergedProperties = new Properties();
        
        for (String key : PIG_CONFIGS_TO_MERGE) {
            String value = properties.getProperty(key);
            String otherValue = other.getProperty(key);
            
            if (value != null && otherValue != null) {
                mergedProperties.setProperty(key, value + ", " + otherValue);
            }
        }
        
        // Copy the other one
        properties.putAll(other);
        
        // Now, overwrite with the merged one
        properties.putAll(mergedProperties);
    }
}


