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
import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;

import org.apache.pig.ReversibleLoadStoreFunc;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.apache.pig.impl.builtin.RandomSampleLoader;
import org.apache.pig.impl.eval.BinCondSpec;
import org.apache.pig.impl.eval.ConstSpec;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.FilterSpec;
import org.apache.pig.impl.eval.FuncEvalSpec;
import org.apache.pig.impl.eval.GenerateSpec;
import org.apache.pig.impl.eval.ProjectSpec;
import org.apache.pig.impl.eval.CompositeEvalSpec;
import org.apache.pig.impl.eval.SortDistinctSpec;
import org.apache.pig.impl.eval.StarSpec;
import org.apache.pig.impl.eval.EvalSpecVisitor;
import org.apache.pig.impl.io.FileLocalizer;
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
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.mapreduceExec.SortPartitioner;

// compiler for mapreduce physical plans
public class MapreducePlanCompiler {

    protected PigContext pigContext;
    protected NodeIdGenerator nodeIdGenerator;

    protected MapreducePlanCompiler(PigContext pigContext) {
        this.pigContext = pigContext;
        this.nodeIdGenerator = NodeIdGenerator.getGenerator();
    }

    private String getTempFile(PigContext pigcontext) throws IOException {
        return FileLocalizer.getTemporaryPath(null,
                                              pigContext).toString();
    }
    
    public OperatorKey compile(OperatorKey logicalKey, 
                               Map<OperatorKey, LogicalOperator> logicalOpTable, 
                               HExecutionEngine execEngine) throws IOException {
        
        // check to see if we have materialized results for the logical tree to
        // compile, if so, re-use them...
        //
        Map<OperatorKey, MapRedResult> materializedResults = execEngine.getMaterializedResults();
        
        MapRedResult materializedResult = materializedResults.get(logicalKey);
        
        if (materializedResult != null) {
            if (PigContext.instantiateFuncFromSpec(materializedResult.outFileSpec.getFuncSpec()) 
                    instanceof ReversibleLoadStoreFunc) {
                POMapreduce pom = new POMapreduce(logicalKey.getScope(),
                    nodeIdGenerator.getNextNodeId(logicalKey.getScope()),
                    execEngine.getPhysicalOpTable(), logicalKey,
                    pigContext);

            	pom.addInputFile(materializedResult.outFileSpec);
            	pom.mapParallelism = Math.max(pom.mapParallelism, materializedResult.parallelismRequest);

            	return pom.getOperatorKey();            
			}
        }
        
        // first, compile inputs into MapReduce operators
        OperatorKey[] compiledInputs = new OperatorKey[logicalOpTable.get(logicalKey).getInputs().size()];
        
        for (int i = 0; i < logicalOpTable.get(logicalKey).getInputs().size(); i++)
            compiledInputs[i] = compile(logicalOpTable.get(logicalKey).getInputs().get(i),
                                        logicalOpTable,
                                        execEngine);
        
        // then, compile this operator; if possible, merge with previous MapReduce
        // operator rather than introducing a new one
        
        LogicalOperator lo = logicalOpTable.get(logicalKey);
        
        if (lo instanceof LOEval) {
            POMapreduce pom = ((POMapreduce)execEngine.getPhysicalOpTable().get(compiledInputs[0]))
                                .copy(nodeIdGenerator.getNextNodeId(logicalKey.getScope())); // make a copy of the previous
            
            // need to do this because the copy is implemented via serialize/deserialize and the ctr is not
            // invoked for pom.
            execEngine.getPhysicalOpTable().put(pom.getOperatorKey(), pom);
            
            // MapReduce operator (NOTE: it's important that we make a copy rather than using it
            // directly; this matters in the case of a "split")

            pushInto((LOEval) lo, pom); // add the computation specified by "lo" to this mapreduce
            // operator
            
            return pom.getOperatorKey();
        } 
        else if (lo instanceof LOCogroup) {
            POMapreduce pom = new POMapreduce(logicalKey.getScope(),
                                              nodeIdGenerator.getNextNodeId(logicalKey.getScope()),
                                              execEngine.getPhysicalOpTable(),
                                              logicalKey,
                                              pigContext, 
                                              -1, 
                                              logicalOpTable.get(logicalKey).getRequestedParallelism());

            pom.groupFuncs = (((LOCogroup) lo).getSpecs());
            
            connectInputs(compiledInputs, pom, execEngine.getPhysicalOpTable());

            return pom.getOperatorKey();
        }  
        else if (lo instanceof LOSplitOutput){
            POMapreduce child = (POMapreduce)execEngine.getPhysicalOpTable().get(compiledInputs[0]);
            String fileName = child.toSplit.tempFiles.get(((LOSplitOutput)lo).getReadFrom());
            POMapreduce pom = new POMapreduce(lo.getScope(),
                                              nodeIdGenerator.getNextNodeId(logicalKey.getScope()),
                                              execEngine.getPhysicalOpTable(),
                                              logicalKey,
                                              pigContext,
                                              child.getOperatorKey());
            FileSpec inputFileSpec = new FileSpec(fileName, BinStorage.class.getName());
            pom.addInputFile(inputFileSpec);
            return pom.getOperatorKey();
        }
        else if (lo instanceof LOSplit) {
            //Make copy of previous operator
            POMapreduce pom = ((POMapreduce)execEngine.getPhysicalOpTable().get(compiledInputs[0])).
                                copy(nodeIdGenerator.getNextNodeId(logicalKey.getScope()));
            
            // need to do this because the copy is implemented via serialize/deserialize and the ctr is not
            // invoked for pom.
            execEngine.getPhysicalOpTable().put(pom.getOperatorKey(), pom);
            
            pom.toSplit = new SplitSpec((LOSplit)lo, pigContext);
            
            //Technically, we don't need the output to be set, in the split case 
            //because nothing will go to the output. But other code assumes its non
            //null, so we set it to a temp file.
            FileSpec fileSpec = new FileSpec(getTempFile(pigContext), BinStorage.class.getName());
            pom.outputFileSpec = fileSpec;
            return pom.getOperatorKey();
        }
        else if (lo instanceof LOLoad) {
            POMapreduce pom = new POMapreduce(lo.getScope(),
                                              nodeIdGenerator.getNextNodeId(logicalKey.getScope()),
                                              execEngine.getPhysicalOpTable(),
                                              logicalKey,
                                              pigContext,
                                              compiledInputs);
            pom.addInputFile(((LOLoad) lo).getInputFileSpec());
            pom.mapParallelism = Math.max(pom.mapParallelism, lo.getRequestedParallelism());
            pom.setProperty("pig.input.splittable", 
                            Boolean.toString(((LOLoad)lo).isSplittable()));
            return pom.getOperatorKey();
        } 
        else if (lo instanceof LOStore) {
            LOStore los = (LOStore) lo;
            ((POMapreduce)execEngine.getPhysicalOpTable().get(compiledInputs[0])).outputFileSpec = los.getOutputFileSpec();
            
            ((POMapreduce)execEngine.getPhysicalOpTable().get(compiledInputs[0])).sourceLogicalKey = 
                los.getInputs().get(0);
            
            return compiledInputs[0];
        } 
        else if (lo instanceof LOUnion) {
            POMapreduce pom = new POMapreduce(lo.getScope(), 
                                              nodeIdGenerator.getNextNodeId(logicalKey.getScope()),
                                              execEngine.getPhysicalOpTable(),
                                              logicalKey,
                                              pigContext, 
                                              -1, 
                                              lo.getRequestedParallelism());
            connectInputs(compiledInputs, pom, execEngine.getPhysicalOpTable());
            return pom.getOperatorKey();
        } 
        else if (lo instanceof LOSort) {
            LOSort loSort = (LOSort) lo;
            //must break up into 2 map reduce jobs, one for gathering quantiles, another for sorting
            POMapreduce quantileJob = getQuantileJob(lo.getScope(), 
                                                     nodeIdGenerator.getNextNodeId(logicalKey.getScope()),
                                                     execEngine.getPhysicalOpTable(),
                                                     logicalKey,
                                                     (POMapreduce) (execEngine.getPhysicalOpTable().get(compiledInputs[0])), 
                                                     loSort);
            
            return getSortJob(lo.getScope(), 
                              nodeIdGenerator.getNextNodeId(logicalKey.getScope()),
                              execEngine.getPhysicalOpTable(),
                              logicalKey,
                              quantileJob, 
                              loSort).getOperatorKey();
        }
        throw new IOException("Unknown logical operator.");
    }
    
    // added for UNION:
    // returns true iff okay to merge this operator with a subsequent binary op (e.g., co-group or union).
    // this is the case iff (1) this operator doesn't do grouping (which requires its own reduce phase), and (2) this operator isn't itself a binary op
    private boolean okayToMergeWithBinaryOp(POMapreduce mro) {
        return (!mro.doesGrouping() && (mro.numInputFiles() == 1));
    }
    
    private void connectInputs(OperatorKey[] compiledInputs, 
                               POMapreduce pom,
                               Map<OperatorKey, ExecPhysicalOperator> physicalOpTable) throws IOException {
        // connect inputs (by merging operators, if possible; 
        // else connect via temp files)
        for (int i = 0; i < compiledInputs.length; i++) {
            POMapreduce input = 
                (POMapreduce)physicalOpTable.get(compiledInputs[i]);
            
            if (okayToMergeWithBinaryOp(input)) {
                // can merge input i with this operator
                pom.addInputFile(input.getFileSpec(0), input.getEvalSpec(0));
                pom.addInputOperators(input.inputs);
            } else {
                // chain together via a temp file
                String tempFile = getTempFile(pigContext);
                FileSpec fileSpec = new FileSpec(tempFile, 
                                                 BinStorage.class.getName());
                input.outputFileSpec = fileSpec;
                pom.addInputFile(fileSpec);
                pom.addInputOperator(compiledInputs[i]);
            }
            
            // propagate input properties
            pom.properties.putAll(input.properties);
        }
    }

    // push the function evaluated by "lo" into the map-reduce operator "mro"
    private void pushInto(LOEval lo, POMapreduce mro) throws IOException {

        if (!mro.doesGrouping()) { // push into "map" phase
           
            // changed for UNION:
            for (int index = 0; index < mro.toMap.size(); index++) {
                mro.addMapSpec(index, lo.getSpec());
            }
            //int index = mro.toMap.list.size() - 1;
            //mro.toMap.list.get(index).add(lo.spec);
            
            mro.mapParallelism = Math.max(mro.mapParallelism, lo.getRequestedParallelism());

        } else { // push into "reduce" phase
            EvalSpec spec = lo.getSpec();

            if (mro.toReduce == null && shouldCombine(spec, mro)) {
                // Push this spec into the combiner.  But we also need to
                // create a new spec with a changed expected projection to
                // push into the reducer.

                if (mro.toCombine != null) {
                    throw new AssertionError("Combiner already set.");
                }
                // mro.toCombine = spec;

                // Now, we need to adjust the expected projection for the
                // eval spec(s) we just pushed.  Also, this will change the
                // function to be the final instead of general instance.
                EvalSpec newSpec = spec.copy(pigContext);
                newSpec.visit(new ReduceAdjuster(pigContext));
                mro.addReduceSpec(newSpec);

                // Adjust the function name for the combine spec, to set it
                // to the initial function instead of the general
                // instance.  Make a copy of the eval spec rather than
                // adjusting the existing one, to prevent breaking the 
                // logical plan in case another physical plan is generated
                // from it later.
                EvalSpec combineSpec = spec.copy(pigContext);
                combineSpec.visit(new CombineAdjuster());
                mro.toCombine = combineSpec;

            } else {
                mro.addReduceSpec(lo.getSpec()); // otherwise, don't use combiner
            }
            
            mro.reduceParallelism = Math.max(mro.reduceParallelism, lo.getRequestedParallelism());

        }
    }
    
    
    private POMapreduce getQuantileJob(String scope,
                                       long id,
                                       Map<OperatorKey, ExecPhysicalOperator> physicalOpTable,
                                       OperatorKey logicalKey,
                                       POMapreduce input, 
                                       LOSort loSort) throws IOException{
        //first the quantile job
        POMapreduce quantileJob = new POMapreduce(scope, 
                                                  id,
                                                  physicalOpTable,
                                                  logicalKey,
                                                  pigContext,
                                                  input.getOperatorKey());
        //first materialize the output of the previous stage
        String fileName = getTempFile(pigContext);
        input.outputFileSpec = new FileSpec(fileName,BinStorage.class.getName());
        
        //Load the output using a random sample load function
        FileSpec inputFileSpec = new FileSpec(fileName, RandomSampleLoader.class.getName());
        quantileJob.addInputFile(inputFileSpec);
        
        quantileJob.addMapSpec(0, loSort.getSortSpec());
        
        //Constructing the query structures by hand, quite ugly.
        
        //group all
        ArrayList<EvalSpec> groupFuncs = new ArrayList<EvalSpec>();
    
        groupFuncs.add(new GenerateSpec(new ConstSpec("all")).getGroupBySpec());
    
        quantileJob.groupFuncs = groupFuncs;
        
        //find the quantiles in the reduce step
        ArrayList<EvalSpec> argsList = new ArrayList<EvalSpec>();
        argsList.add(new ConstSpec(Math.max(loSort.getRequestedParallelism()-1,1)));
        
        //sort the first column of the cogroup output and feed it to the quantiles function
        EvalSpec sortedSampleSpec = new ProjectSpec(1);
        EvalSpec starSpec = new StarSpec();
        starSpec.setComparatorName(loSort.getSortSpec().getComparatorName());
        sortedSampleSpec = sortedSampleSpec.addSpec(new SortDistinctSpec(false, starSpec));
        argsList.add(sortedSampleSpec);
        
        EvalSpec args = new GenerateSpec(argsList);

        EvalSpec reduceSpec = new FuncEvalSpec(pigContext, FindQuantiles.class.getName(), args);
        reduceSpec.setFlatten(true);
        quantileJob.addReduceSpec(new GenerateSpec(reduceSpec));
        
        //a temporary file to hold the quantile data
        String quantileFile = getTempFile(pigContext);
        quantileJob.outputFileSpec = new FileSpec(quantileFile, BinStorage.class.getName());
        
        return quantileJob;
    }
    
    public POMapreduce getSortJob(String scope, 
                                  long id,
                                  Map<OperatorKey, ExecPhysicalOperator> physicalOpTable,
                                  OperatorKey logicalKey,
                                  POMapreduce quantileJob, 
                                  LOSort loSort) throws IOException{
        POMapreduce sortJob = new POMapreduce(scope,
                                              id,
                                              physicalOpTable,
                                              logicalKey,
                                              pigContext, 
                                              quantileJob.getOperatorKey());
        
        sortJob.quantilesFile = quantileJob.outputFileSpec.getFileName();
        
        //same input as the quantile job, but the full BinStorage load function
        sortJob.addInputFile(new FileSpec(quantileJob.getFileSpec(0).getFileName(), BinStorage.class.getName()));
        
        ArrayList<EvalSpec> groupFuncs = new ArrayList<EvalSpec>();
                
        groupFuncs.add(new GenerateSpec(loSort.getSortSpec()).getGroupBySpec());
        
        sortJob.groupFuncs = groupFuncs;
        sortJob.partitionFunction = SortPartitioner.class;
        
        ProjectSpec ps = new ProjectSpec(1);
        ps.setFlatten(true);
        sortJob.addReduceSpec(new GenerateSpec(ps));
    
        sortJob.reduceParallelism = loSort.getRequestedParallelism();
        
        String comparatorFuncName = loSort.getSortSpec().getComparatorName();
        if (comparatorFuncName != null) {
            sortJob.userComparator =
                PigContext.resolveClassName(
                    comparatorFuncName);
        }

        return sortJob;
    }

    private boolean shouldCombine(EvalSpec spec, POMapreduce mro) {
        // Determine whether this something we can combine or not.
        // First, it must be a generate spec.
        if (!(spec instanceof GenerateSpec)) {
            return false;
        }

        // Can only combine if there is a single file being grouped,
        // cogroups can't use the combiner at this point.
        if (mro.groupFuncs.size() > 1) {
            return false;
        }

        GenerateSpec gen = (GenerateSpec)spec;

        // Second, the first immediate child of the generate spec must be
        // a project with a value of 0.
        Iterator<EvalSpec> i = gen.getSpecs().iterator();
        if (!i.hasNext()) return false;
        EvalSpec s = i.next();
        if (!(s instanceof ProjectSpec)) {
            return false;
        } else {
            ProjectSpec p = (ProjectSpec)s;
            if (p.numCols() > 1) return false;
            else if (p.getCol() != 0) return false;
        }

        // Third, all subsequent immediate children of the generate spec
        // must be func eval specs
        while (i.hasNext()) {
            s = i.next();
            if (!(s instanceof FuncEvalSpec)) return false;
        }

        // Third, walk the entire tree of the generate spec and see if we
        // can combine it.
        CombineDeterminer cd = new CombineDeterminer();
        gen.visit(cd);
        return cd.useCombiner();
    }

    private class ReduceAdjuster extends EvalSpecVisitor {
        private int position = 0;
        FunctionInstantiator instantiator = null;

        public ReduceAdjuster(FunctionInstantiator fi) {
            instantiator = fi;
        }

        public void visitGenerate(GenerateSpec g) {
            Iterator<EvalSpec> i = g.getSpecs().iterator();
            for (position = 0; i.hasNext(); position++) {
                i.next().visit(this);
            }
        }
        
        public void visitFuncEval(FuncEvalSpec fe) {
            // Need to replace our arg spec with a project of our position.
            // DON'T visit our args, they're exactly what we're trying to
            // lop off.
            // The first ProjectSpec in the Composite is because the tuples
            // will come out of the combiner in the form (groupkey,
            // {(x, y, z)}).  The second ProjectSpec contains the offset of
            // the projection element we're interested in.
            CompositeEvalSpec cs = new CompositeEvalSpec(new ProjectSpec(1));
            cs.addSpec(new ProjectSpec(position));
            fe.setArgs(new GenerateSpec(cs));


            // Reset the function to call the final instance of itself
            // instead of the general instance.  Have to instantiate the
            // function itself first so we can find out if it's algebraic
            // or not.
            try {
                fe.instantiateFunc(instantiator);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            fe.resetFuncToFinal();
        }
    }

    private class CombineAdjuster extends EvalSpecVisitor {
        private int position = 0;

        //We don't want to be performing any flattening in the combiner since the column numbers in
        //the reduce spec assume that there is no combiner. If the combiner performs flattening, the column
        //numbers get messed up. For now, since combiner works only with generate group, func1(), func2(),...,
        //it suffices to write visitors for those eval spec types.

        public void visitFuncEval(FuncEvalSpec fe) {
            // Reset the function to call the initial instance of itself
            // instead of the general instance.
            fe.resetFuncToInitial();
            fe.setFlatten(false);
        }

        @Override
        public void visitProject(ProjectSpec p) {
            p.setFlatten(false);
        }

    }

    private class CombineDeterminer extends EvalSpecVisitor {
        private class FuncCombinable extends EvalSpecVisitor {
            public boolean combinable = true;

            @Override
            public void visitBinCond(BinCondSpec bc) {
                combinable = false;
            }
            
            @Override
            public void visitFilter(FilterSpec bc) {
                combinable = false;
            }

            @Override
            public void visitFuncEval(FuncEvalSpec bc) {
                combinable = false;
            }

            @Override
            public void visitSortDistinct(SortDistinctSpec bc) {
                combinable = false;
            }
        };

        private int shouldCombine = 0;

        public boolean useCombiner() {
            return shouldCombine > 0;
        }

        @Override
        public void visitBinCond(BinCondSpec bc) {
            // TODO Could be true if both are true.  But the logic in
            // CombineAdjuster and ReduceAdjuster don't know how to handle
            // binconds, so just do false for now.
            shouldCombine = -1;
        }

        @Override
        public void visitCompositeEval(CompositeEvalSpec ce) {
            // If we've already determined we're not combinable, stop.
            if (shouldCombine < 0) return;

            for (EvalSpec spec: ce.getSpecs()) {
                spec.visit(this);
            }
        }

        // ConstSpec is a NOP, as it neither will benefit from nor
        // prevents combinability.
        
        @Override
        public void visitFilter(FilterSpec f) {
            shouldCombine = -1;
        }

        @Override
        public void visitFuncEval(FuncEvalSpec fe) {
            // Check the functions arguments, to make sure they are
            // combinable.
            FuncCombinable fc = new FuncCombinable();
            fe.getArgs().visit(fc);
            if (!fc.combinable) {
                shouldCombine = -1;
                return;
            }
            
            if (fe.combinable()) shouldCombine = 1;
            else shouldCombine = -1;
        }

        @Override
        public void visitGenerate(GenerateSpec g) {
            // If we've already determined we're not combinable, stop.
            if (shouldCombine < 0) return;

            for (EvalSpec spec: g.getSpecs()) {
                spec.visit(this);
            }
        }

        // MapLookupSpec is a NOP, as it neither will benefit from nor
        // prevents combinability.
        
        // ProjectSpec is a NOP, as it neither will benefit from nor
        // prevents combinability.
        
        @Override
        public void visitSortDistinct(SortDistinctSpec sd) {
            shouldCombine = -1;
        }

        // StarSpec is a NOP, as it neither will benefit from nor
        // prevents combinability.
    }

}
