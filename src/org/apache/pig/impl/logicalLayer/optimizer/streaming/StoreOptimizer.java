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
 * {@link StoreOptimizer} tries to optimize away the deserialization done by Pig 
 * for the simple case of a STREAM followed by a STORE operator; both with
 * equivalent {@link StoreFunc} specifications.
 * 
 * In such cases it is safe to replace the <code>StoreFunc</code>
 * specifications with a {@link BinaryStorage} which doesn't interpret the
 * output bytes at all.
 */
public class StoreOptimizer extends Optimizer {
    boolean optimize = false;
    boolean parentEval = false;
    LOEval eval = null;
    
    public void visitCogroup(LOCogroup g) {
        super.visitCogroup(g);
        parentEval = false;
    }

    public void visitEval(LOEval e) {
        super.visitEval(e);
        eval = e;
        parentEval = true;

        if (e.getSpec() instanceof StreamSpec) {
            StreamSpec streamSpec = (StreamSpec)e.getSpec();
            streamSpec.revertOptimizedCommand(Handle.OUTPUT);
        }
    }

    public void visitLoad(LOLoad load) {
        super.visitLoad(load);
        parentEval = false;
    }

    public void visitSort(LOSort s) {
        super.visitSort(s);
        parentEval = false;
    }

    public void visitSplit(LOSplit s) {
        super.visitSplit(s);
        parentEval = false;
    }

    public void visitSplitOutput(LOSplitOutput s) {
        super.visitSplitOutput(s);
        parentEval = false;
    }

    public void visitStore(LOStore s) {
        super.visitStore(s);
        
        if (parentEval) {
            EvalSpec spec = eval.getSpec();
            if (spec instanceof StreamSpec) {
                // Try and optimize if the store and stream output specs match
                StreamSpec streamSpec = (StreamSpec)spec;
                StreamingCommand command = streamSpec.getCommand();
                HandleSpec streamOutputSpec = command.getOutputSpec(); 
                
                FileSpec storeFileSpec = s.getOutputFileSpec();
                
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
                    // the outputStorer ...
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
                    s.setOutputFileSpec(new FileSpec(storeFileSpec.getFileName(), BinaryStorage.class.getName()));
                    streamSpec.setOptimizedSpec(Handle.OUTPUT, 
                                                   BinaryStorage.class.getName());
                    optimize = true;
                }
            }
        }

        parentEval = false;
    }

    public void visitUnion(LOUnion u) {
        super.visitUnion(u);
        parentEval = false;
    }

    public boolean optimize(LogicalPlan root) {
        LogicalOperator r = root.getOpTable().get(root.getRoot());
        r.visit(this);
        return optimize;
    }
}
