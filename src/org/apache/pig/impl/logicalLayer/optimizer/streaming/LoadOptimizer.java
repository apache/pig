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
package org.apache.pig.impl.logicalLayer.optimizer.streaming;

import org.apache.pig.LoadFunc;
import org.apache.pig.ReversibleLoadStoreFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.builtin.BinaryStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.StreamSpec;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOEval;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.optimizer.Optimizer;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.Handle;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

/**
 * {@link LoadOptimizer} tries to optimize away the deserialization done by Pig 
 * for the simple case of a LOAD followed by a STREAM operator; both with the 
 * equivalent {@link LoadFunc} specifications.
 * 
 * In such cases it is safe to replace the equivalent <code>LoadFunc</code>
 * specifications with a {@link BinaryStorage} which doesn't interpret the
 * input bytes at all.
 */
public class LoadOptimizer extends Optimizer {
    boolean optimize = false;
    boolean parentLoad = false;
    LOLoad load = null;
    
    public void visitCogroup(LOCogroup g) {
        super.visitCogroup(g);
        parentLoad = false;
    }

    public void visitEval(LOEval e) {
        super.visitEval(e);

        if (parentLoad) {
            EvalSpec spec = e.getSpec();
            if (spec instanceof StreamSpec && !load.isSplittable()) {
                // Try and optimize if the load and stream input specs match
                // and input files are to be processed as-is
                StreamSpec streamSpec = (StreamSpec)spec;
                StreamingCommand command = streamSpec.getCommand();
                HandleSpec streamInputSpec = command.getInputSpec(); 
                
                FileSpec loadFileSpec = load.getInputFileSpec();
                
                // Instantiate both to compare them for equality
                StoreFunc streamStorer = 
                    (StoreFunc)PigContext.instantiateFuncFromSpec(
                            streamInputSpec.getSpec());
                
                LoadFunc inputLoader = (LoadFunc)PigContext.instantiateFuncFromSpec(
                                                loadFileSpec.getFuncSpec());

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
                    load.setInputFileSpec(new FileSpec(loadFileSpec.getFileName(), BinaryStorage.class.getName()));
                    streamSpec.setOptimizedSpec(Handle.INPUT, 
                                                   BinaryStorage.class.getName());
                    optimize = true;
                }
            }
        } else {
            if (e.getSpec() instanceof StreamSpec) {
                StreamSpec streamSpec = (StreamSpec)e.getSpec();
                streamSpec.revertOptimizedCommand(Handle.INPUT);
            }
        }
        
        parentLoad = false;
    }

    public void visitLoad(LOLoad load) {
        super.visitLoad(load);
        parentLoad = true;
        this.load = load;
    }

    public void visitSort(LOSort s) {
        super.visitSort(s);
        parentLoad = false;
    }

    public void visitSplit(LOSplit s) {
        super.visitSplit(s);
        parentLoad = false;
    }

    public void visitSplitOutput(LOSplitOutput s) {
        super.visitSplitOutput(s);
        parentLoad = false;
    }

    public void visitStore(LOStore s) {
        super.visitStore(s);
        parentLoad = false;
    }

    public void visitUnion(LOUnion u) {
        super.visitUnion(u);
        parentLoad = false;
    }

    public boolean optimize(LogicalPlan root) {
        LogicalOperator r = root.getOpTable().get(root.getRoot());
        r.visit(this);
        return optimize;
    }
}
