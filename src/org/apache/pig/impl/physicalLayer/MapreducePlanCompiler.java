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
import java.util.Map;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparator;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.Tuple;
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
import org.apache.pig.impl.eval.MapLookupSpec;
import org.apache.pig.impl.eval.SortDistinctSpec;
import org.apache.pig.impl.eval.StarSpec;
import org.apache.pig.impl.eval.EvalSpecVisitor;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOEval;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LORead;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.mapreduceExec.SortPartitioner;


// compiler for mapreduce physical plans
public class MapreducePlanCompiler extends PlanCompiler {

    protected MapreducePlanCompiler(PigContext pigContext) {
		super(pigContext);
	}

	@Override
	public PhysicalOperator compile(LogicalOperator lo, Map queryResults) throws IOException {
        // first, compile inputs into MapReduce operators
        POMapreduce[] compiledInputs = new POMapreduce[lo.getInputs().size()];
        for (int i = 0; i < lo.getInputs().size(); i++)
            compiledInputs[i] = (POMapreduce) compile(lo.getInputs().get(i), queryResults);

        // then, compile this operator; if possible, merge with previous MapReduce
        // operator rather than introducing a new one
        if (lo instanceof LOEval) {
            POMapreduce pom = compiledInputs[0].copy(); // make a copy of the previous
            // MapReduce operator (NOTE: it's important that we make a copy rather than using it
            // directly; this matters in the case of a "split")
            pushInto((LOEval) lo, pom); // add the computation specified by "lo" to this mapreduce
            // operator
            return pom;
        } else if (lo instanceof LOCogroup) {
            POMapreduce pom = new POMapreduce(pigContext, -1, lo.getRequestedParallelism());

            pom.groupFuncs = (((LOCogroup) lo).getSpecs());
            
            connectInputs(compiledInputs, pom);

            return pom;
        }  else if (lo instanceof LOSplit){
        	//Make copy of previous operator
        	POMapreduce pom = compiledInputs[0].copy();
        	pom.toSplit = new SplitSpec((LOSplit)lo, pigContext);
        	
        	//Technically, we don't need the output to be set, in the split case 
        	//because nothing will go to the output. But other code assumes its non
        	//null, so we set it to a temp file.
            FileSpec fileSpec = new FileSpec(getTempFile(pigContext), BinStorage.class.getName());
            pom.outputFileSpec = fileSpec;
        	return pom;
        }else if (lo instanceof LOLoad) {
            POMapreduce pom = new POMapreduce(pigContext, compiledInputs);
            LOLoad loLoad = (LOLoad) lo;
            String filename = FileLocalizer.fullPath(loLoad.getInputFileSpec().getFileName(), pigContext);
            FileSpec fileSpec = new FileSpec(filename, loLoad.getInputFileSpec().getFuncSpec());
            pom.addInputFile(fileSpec);
            pom.mapParallelism = Math.max(pom.mapParallelism, lo.getRequestedParallelism());
            return pom;
        } else if (lo instanceof LORead) {
            LORead loRead = (LORead)lo;
        	IntermedResult readFrom = loRead.getReadFrom();

            if (!loRead.readsFromSplit()){
	            if (readFrom.executed()) {
	                // result should already exist as a temp file
	                POMapreduce pom = new POMapreduce(pigContext);
	                if (readFrom.outputFileSpec == null) readFrom.toDFSFile(new FileSpec(PlanCompiler.getTempFile(pigContext), BinStorage.class.getName()), pigContext);
	                FileSpec inputFileSpec = readFrom.outputFileSpec;
	                pom.addInputFile(inputFileSpec);
	                return pom;
	            } else {
	                // compile other plan (idempotent)
	                readFrom.compile(queryResults);
	
	                // return root of other plan
	                return (POMapreduce) readFrom.pp.root;
	            }
            }else{
            	if (readFrom.executed()){
            		POMapreduce pom = new POMapreduce(pigContext);
            		POMapreduce child = (POMapreduce) readFrom.pp.root;
            		String fileName = child.toSplit.tempFiles.get(loRead.splitOutputToRead);
            		FileSpec inputFileSpec = new FileSpec(fileName, BinStorage.class.getName());
            		pom.addInputFile(inputFileSpec);
            		return pom;
            	}else{
            		readFrom.compile(queryResults);
            		POMapreduce child = (POMapreduce) readFrom.pp.root;
            		POMapreduce pom = new POMapreduce(pigContext,child);
            		String fileName = child.toSplit.tempFiles.get(loRead.splitOutputToRead);
            		FileSpec inputFileSpec = new FileSpec(fileName, BinStorage.class.getName());
            		pom.addInputFile(inputFileSpec);
            		return pom;
            	}
            }

        } else if (lo instanceof LOStore) {
            LOStore los = (LOStore) lo;
            compiledInputs[0].outputFileSpec = los.getOutputFileSpec();
            return compiledInputs[0];
        } else if (lo instanceof LOUnion) {
            POMapreduce pom = new POMapreduce(pigContext, -1, lo.getRequestedParallelism());
            connectInputs(compiledInputs, pom);
            return pom;
        } else if (lo instanceof LOSort) {
        	LOSort loSort = (LOSort) lo;
        	//must break up into 2 map reduce jobs, one for gathering quantiles, another for sorting
        	POMapreduce quantileJob = getQuantileJob(compiledInputs[0], loSort);
        	return getSortJob(quantileJob, loSort);
        	
        	
        }
            throw new IOException("Unknown logical operator.");
    }
    
    // added for UNION:
    // returns true iff okay to merge this operator with a subsequent binary op (e.g., co-group or union).
    // this is the case iff (1) this operator doesn't do grouping (which requires its own reduce phase), and (2) this operator isn't itself a binary op
    private boolean okayToMergeWithBinaryOp(POMapreduce mro) {
        return (!mro.doesGrouping() && (mro.numInputFiles() == 1));
    }
    
    private void connectInputs(POMapreduce[] compiledInputs, POMapreduce pom) throws IOException {
        // connect inputs (by merging operators, if possible; else connect via temp files)
        for (int i = 0; i < compiledInputs.length; i++) {
            if (okayToMergeWithBinaryOp(compiledInputs[i])) {
                // can merge input i with this operator
            	pom.addInputFile(compiledInputs[i].getFileSpec(0), compiledInputs[i].getEvalSpec(0));
                pom.addInputOperators(compiledInputs[i].inputs);
            } else {
                // chain together via a temp file
                String tempFile = getTempFile(pigContext);
                FileSpec fileSpec = new FileSpec( tempFile, BinStorage.class.getName());
                compiledInputs[i].outputFileSpec = fileSpec;
                pom.addInputFile(fileSpec);
                pom.addInputOperator(compiledInputs[i]);
            }
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

            if (mro.toReduce == null && shouldCombine(spec)) {
                // Push this spec into the combiner.  But we also need to
                // create a new spec with a changed expected projection to
                // push into the reducer.

                if (mro.toCombine != null) {
                    throw new AssertionError("Combiner already set.");
                }
                mro.toCombine = spec;

                // Now, we need to adjust the expected projection for the
                // eval spec(s) we just pushed.  Also, this will change the
                // function to be the final instead of general instance.
                EvalSpec newSpec = spec.copy(pigContext);
                newSpec.visit(new ReduceAdjuster(pigContext));
                mro.addReduceSpec(newSpec);

                // Adjust the function name for the combine spec, to set it
                // to the initial function instead of the general
                // instance.  This has to be done after the copy is made
                // for the combiner.
                spec.visit(new CombineAdjuster());

            } else {
                mro.addReduceSpec(lo.getSpec()); // otherwise, don't use combiner
            }
            
            mro.reduceParallelism = Math.max(mro.reduceParallelism, lo.getRequestedParallelism());

        }
    }
    
    
    private POMapreduce getQuantileJob(POMapreduce input, LOSort loSort) throws IOException{
    	//first the quantile job
    	POMapreduce quantileJob = new POMapreduce(pigContext,input);
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
    	sortedSampleSpec = sortedSampleSpec.addSpec(new SortDistinctSpec(false, new StarSpec()));
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
    
    public POMapreduce getSortJob(POMapreduce quantileJob, LOSort loSort) throws IOException{
    	POMapreduce sortJob = new POMapreduce(pigContext, quantileJob);
    	
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
    		try {
    			sortJob.userComparator = 
    				(Class<WritableComparator>)Class.forName(comparatorFuncName);
    		} catch (ClassNotFoundException e) {
    			throw new RuntimeException("Unable to find user comparator " + comparatorFuncName, e);
    		}
    	}

    	return sortJob;
    }

    private boolean shouldCombine(EvalSpec spec) {
        // Determine whether this something we can combine or not.
        // First, it must be a generate spec.
        if (!(spec instanceof GenerateSpec)) {
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

        public void visitFuncEval(FuncEvalSpec fe) {
            // Reset the function to call the initial instance of itself
            // instead of the general instance.
            fe.resetFuncToInitial();
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
