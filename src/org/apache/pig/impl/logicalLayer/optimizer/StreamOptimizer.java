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

package org.apache.pig.impl.logicalLayer.optimizer;

import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.ReversibleLoadStoreFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.builtin.BinaryStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.Handle;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

/**
 * A visitor to optimize in the following scenario with
 * streaming:
 * Optimize when LOAD precedes STREAM and the loader class
 * is the same as the serializer for the STREAM.
 * Similarly optimize when STREAM is followed by store and the
 * deserializer class is same as the Storage class.
 * Specifically in both these cases the optimization is to
 * replace the loader/serializer with BinaryStorage which 
 * just moves bytes around and likewise replace the storer/deserializer
 * with BinaryStorage
 *
 */
public class StreamOptimizer extends LogicalTransformer {
    
    private boolean mOptimizeLoad = false;
    private boolean mOptimizeStore = false;

    public StreamOptimizer(LogicalPlan plan, String operatorClassName) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
    }

    @Override
    public boolean check(List<LogicalOperator> nodes) throws OptimizerException {
        mOptimizeLoad = false;
        mOptimizeStore = false;
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }

        LogicalOperator lo = nodes.get(0);
        if (lo == null || !(lo instanceof LOStream)) {
            int errCode = 2005;
            String msg = "Expected " + LOStream.class.getSimpleName() + ", got " + lo.getClass().getSimpleName();
            throw new OptimizerException(msg, errCode, PigException.BUG);            
        }
        LOStream stream = (LOStream)lo;
        
        // check if either the predecessor of stream is load with
        // the same loader function as the serializer of stream
        // OR if the successor of stream is store with the same
        // storage function as the deserializer of stream.
        checkLoadOptimizable(stream);
        checkStoreOptimizable(stream);
        return mOptimizeLoad || mOptimizeStore;
    }
    
    private void checkLoadOptimizable(LOStream stream) {
        List<LogicalOperator> predecessors = mPlan.getPredecessors(stream);
        if((predecessors == null) || (predecessors.size() <= 0)) {
            return;
        }
        LogicalOperator predecessor = predecessors.get(0);
        if(predecessor instanceof LOLoad) {
            LOLoad load = (LOLoad)predecessor;
            if(!load.isSplittable()) {
                // Try and optimize if the load and stream input specs match
                // and input files are to be processed as-is
                StreamingCommand command = stream.getStreamingCommand();
                HandleSpec streamInputSpec = command.getInputSpec(); 
                FileSpec loadFileSpec = load.getInputFile();
                // Instantiate both LoadFunc objects to compare them for 
                // equality
                StoreFunc streamStorer = 
                    (StoreFunc)PigContext.instantiateFuncFromSpec(
                            streamInputSpec.getSpec());
                LoadFunc inputLoader = (LoadFunc)PigContext.instantiateFuncFromSpec(
                                             loadFileSpec.getFuncSpec());
                LogFactory.getLog(this.getClass()).info("streamStorer:" + streamStorer + "," +
                        "inputLoader:" + inputLoader);
                // Check if the streaming command's inputSpec also implements 
                // LoadFunc and if it does, are they of the same _reversible_ 
                // type?
                boolean sameType = false;
                try {
                    // Check if the streamStorer is _reversible_ as 
                    // the inputLoader ...
                    if (streamStorer instanceof LoadFunc) {
                        // Cast to check if they are of the same type...
                        streamStorer.getClass().cast(inputLoader);
                        LogFactory.getLog(this.getClass()).info("streamStorer:" + streamStorer + "," +
                                "inputLoader:" + inputLoader);
                        // Now check if they both are reversible...
                        if (streamStorer instanceof ReversibleLoadStoreFunc &&
                            inputLoader instanceof ReversibleLoadStoreFunc) {
                            sameType = true;
                        }
                    }
                } catch (ClassCastException cce) {
                    sameType = false;
                }
                // Check if both LoadFunc objects belong to the same type and
                // are equivalent
                if (sameType && streamStorer.equals(inputLoader)) {
                    // Since they both are the same, we can flip them 
                    // for BinaryStorage
                    mOptimizeLoad = true;                    
                }
            }
        }   
    }
    
    private void checkStoreOptimizable(LOStream stream) {
        List<LogicalOperator> successors = mPlan.getSuccessors(stream);
        if(successors == null)
            return;
        LogicalOperator successor = successors.get(0);
        if(successor instanceof LOStore) {
            LOStore store = (LOStore)successor;
            // Try and optimize if the store and stream output specs match
            // and input files are to be processed as-is
            StreamingCommand command = stream.getStreamingCommand();
            HandleSpec streamOutputSpec = command.getOutputSpec(); 
            
            FileSpec storeFileSpec = store.getOutputFile();
            
            // Instantiate both to compare them for equality
            LoadFunc streamLoader = 
                (LoadFunc)PigContext.instantiateFuncFromSpec(
                        streamOutputSpec.getSpec());
            
            StoreFunc outputStorer = (StoreFunc)PigContext.instantiateFuncFromSpec(
                                         storeFileSpec.getFuncSpec());
            
            // Check if the streaming command's outputSpec also implements 
            // StoreFunc and if it does, are they of the same _reversible_ 
            // type?
            boolean sameType = false;
            try {
                // Check if the streamLoader is _reversible_ as 
                // the inputLoader ...
                if (streamLoader instanceof StoreFunc) {
                    // Cast to check if they are of the same type...
                    streamLoader.getClass().cast(outputStorer);
                    
                    // Now check if they both are reversible...
                    if (streamLoader instanceof ReversibleLoadStoreFunc &&
                        outputStorer instanceof ReversibleLoadStoreFunc) {
                        sameType = true;
                    }
                }
            } catch (ClassCastException cce) {
                sameType = false;
            }
            // Check if both LoadFunc objects belong to the same type and
            // are equivalent
            if (sameType && streamLoader.equals(outputStorer)) {
                // Since they both are the same, we can flip them 
                // for BinaryStorage
                mOptimizeStore = true;                    
            }
        }
    }
    
    @Override
    public void transform(List<LogicalOperator> nodes) throws OptimizerException {
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        
        try {
            LogicalOperator lo = nodes.get(0);
            if (lo == null || !(lo instanceof LOStream)) {
                throw new RuntimeException("Expected stream, got " +
                    lo.getClass().getName());
            }
            LOStream stream = (LOStream)lo;
            if(mOptimizeLoad) {
                // we have verified in check() that the predecessor was load
                LOLoad load = (LOLoad)mPlan.getPredecessors(lo).get(0);
                FileSpec loadFileSpec = load.getInputFile();
                load.setInputFile(new FileSpec(loadFileSpec.getFileName(), new FuncSpec(BinaryStorage.class.getName())));
                stream.setOptimizedSpec(Handle.INPUT, BinaryStorage.class.getName());
            }
            if(mOptimizeStore) {
                // we have verified in check() that the predecessor was load
                LOStore store = (LOStore)mPlan.getSuccessors(lo).get(0);
                FileSpec storeFileSpec = store.getOutputFile();
                store.setOutputFile(new FileSpec(storeFileSpec.getFileName(), new FuncSpec(BinaryStorage.class.getName())));
                stream.setOptimizedSpec(Handle.OUTPUT, BinaryStorage.class.getName());
            }
        } catch (Exception e) {
            int errCode = 2014;
            String msg = "Unable to optimize load-stream-store optimization"; 
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }
}

 
