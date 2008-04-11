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
package org.apache.pig.pen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.ExampleTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.ConstSpec;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.FilterSpec;
import org.apache.pig.impl.eval.GenerateSpec;
import org.apache.pig.impl.eval.ProjectSpec;
import org.apache.pig.impl.eval.StarSpec;
import org.apache.pig.impl.eval.cond.AndCond;
import org.apache.pig.impl.eval.cond.CompCond;
import org.apache.pig.impl.eval.cond.Cond;
import org.apache.pig.impl.eval.cond.FalseCond;
import org.apache.pig.impl.eval.cond.FuncCond;
import org.apache.pig.impl.eval.cond.NotCond;
import org.apache.pig.impl.eval.cond.OrCond;
import org.apache.pig.impl.eval.cond.RegexpCond;
import org.apache.pig.impl.eval.cond.TrueCond;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOEval;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;
import org.apache.pig.impl.util.IdentityHashSet;

public class AugmentData {
    static Map<LOLoad, DataBag> AugmentBaseData(LogicalOperator op, Map<LOLoad, DataBag> baseData, IdentityHashSet<Tuple> syntheticTuples, Map<LogicalOperator, DataBag> derivedData, PigContext pigContext) throws IOException {
        Map<LOLoad, DataBag> newBaseData = new HashMap<LOLoad, DataBag>();

        //AugmentBaseData(op, baseData, newBaseData, derivedData, new DataBag(), pigContext);
        AugmentBaseData(op, baseData, newBaseData, derivedData, BagFactory.getInstance().newDefaultBag(), pigContext);

        // merge base data and mark synthetic tuples
        for (LOLoad load : baseData.keySet()) {
            DataBag oldData = baseData.get(load);
            DataBag newData = newBaseData.get(load);
            if (newData == null) {
                //newData = new DataBag();
            	newData = BagFactory.getInstance().newDefaultBag();
                newBaseData.put(load, newData);
            }
            
            int newDataBoundary = newData.cardinality();
            
            for(Iterator<Tuple> it = newData.iterator(); it.hasNext();) {
            	((ExampleTuple)it.next()).makeSynthetic();
            }
            // append oldData to newData
            newData.addAll(oldData);
                
            // mark synthetic tuples (this has to be done after, because once you open an iterator you are not allowed to add new tuples)
            int i = 0;
            for (Iterator<Tuple> it = newData.iterator(); it.hasNext() && i < newDataBoundary; ) {
                syntheticTuples.add(it.next());
                i++;
            }
        }

        return newBaseData;
    }        
    
    // contract: each operator (1) tries to generate supplemental data that illustrates its semantics, and
    //                         (2) ensures that its output is non-empty
    // (if the operator is guaranteed to produce an output tuple if it is given at least one input tuple, 
    //  it doesn't have to do anything special for #2 because the input operator is guaranteed to produce at least one output)
    static void AugmentBaseData(LogicalOperator op, Map<LOLoad, DataBag> baseData, Map<LOLoad, DataBag> newBaseData, Map<LogicalOperator, DataBag> derivedData, DataBag outputConstraints, PigContext pigContext) throws IOException {
        if (op instanceof LOLoad) AugmentBaseData((LOLoad) op, baseData, newBaseData, derivedData, outputConstraints, pigContext);
        //else if (op instanceof LORead) AugmentBaseData((LORead) op, baseData, newBaseData, derivedData, outputConstraints, pigContext);
        else if (op instanceof LOStore) AugmentBaseData((LOStore) op, baseData, newBaseData, derivedData, outputConstraints, pigContext);
        else if (op instanceof LOCogroup) AugmentBaseData((LOCogroup) op, baseData, newBaseData, derivedData, outputConstraints, pigContext);
        else if (op instanceof LOEval) AugmentBaseData((LOEval) op, baseData, newBaseData, derivedData, outputConstraints, pigContext);
        else if (op instanceof LOSort) AugmentBaseData((LOSort) op, baseData, newBaseData, derivedData, outputConstraints, pigContext);
        else if (op instanceof LOSplit) AugmentBaseData((LOSplit) op, baseData, newBaseData, derivedData, outputConstraints, pigContext);
        else if (op instanceof LOUnion) AugmentBaseData((LOUnion) op, baseData, newBaseData, derivedData, outputConstraints, pigContext);
        else throw new RuntimeException("Unrecognized logical operator.");
    }

    static void AugmentBaseData(LOLoad op, Map<LOLoad, DataBag> baseData, Map<LOLoad, DataBag> newBaseData, Map<LogicalOperator, DataBag> derivedData, DataBag outputConstraints, PigContext pigContext) throws IOException {
        
        DataBag inputData = baseData.get(op);
        //DataBag newInputData = new DataBag();
        /*DataBag newInputData = BagFactory.getInstance().newDefaultBag();
        newBaseData.put(op, newInputData);*/
        DataBag newInputData = newBaseData.get(op);
        if(newInputData == null) {
        	newInputData = BagFactory.getInstance().newDefaultBag();
        	newBaseData.put(op, newInputData);
        }
        TupleSchema schema = op.outputSchema();
        
        // first of all, we are required to guarantee that there is at least one output tuple
        if (outputConstraints.cardinality() == 0 && inputData.cardinality() == 0) {
            outputConstraints.add(new Tuple(schema.numFields()));   // add an output constraint for one tuple
        }
        
        // create example tuple to steal values from when we encounter "don't care" fields (i.e. null fields)
        Tuple exampleTuple;
        if (inputData.cardinality() > 0) {
            // select first tuple from input data
            exampleTuple = inputData.iterator().next();
        } else {
            // input data is empty, so make up a tuple
            Tuple exampleT = new Tuple(schema.numFields());
            exampleTuple = new ExampleTuple();
            exampleTuple.copyFrom(exampleT);
            for (int i = 0; i < exampleTuple.arity(); i++) exampleTuple.setField(i, "0");
        }
        
        // run through output constraints; for each one synthesize a tuple and add it to the base data
        // (while synthesizing individual fields, try to match fields that exist in the real data)
        for (Iterator<Tuple> it = outputConstraints.iterator(); it.hasNext(); ) {
            Tuple outputConstraint = it.next();
            
            // sanity check:
            if (outputConstraint.arity() != schema.numFields()) throw new RuntimeException("Internal error: incorrect number of fields in constraint tuple.");
            
            Tuple inputT = new Tuple(outputConstraint.arity());
            ExampleTuple inputTuple = new ExampleTuple();
            inputTuple.copyFrom(inputT);
            for (int i = 0; i < inputTuple.arity(); i++) {
                Datum d = outputConstraint.getField(i);
                if (d == null) d = exampleTuple.getField(i);
                inputTuple.setField(i, d);
            }
            if(inputTuple.equals(exampleTuple)) {
            	//System.out.println("Real tuple being copied!!");
            	
            } else {
            	inputTuple.makeSynthetic();
            }
            newInputData.add(inputTuple);
        }
    }
    
    /*static void AugmentBaseData(LORead op, Map<LOLoad, DataBag> baseData, Map<LOLoad, DataBag> newBaseData, Map<LogicalOperator, DataBag> derivedData, DataBag outputConstraints, PigContext pigContext) throws IOException {
        LogicalOperator subPlan = ((LORead) op).getReadFrom().lp.getRoot();
        AugmentBaseData(subPlan, baseData, newBaseData, derivedData, outputConstraints, pigContext);
    }*/
    
    static void AugmentBaseData(LOStore op, Map<LOLoad, DataBag> baseData, Map<LOLoad, DataBag> newBaseData, Map<LogicalOperator, DataBag> derivedData, DataBag outputConstraints, PigContext pigContext) throws IOException {
        //LogicalOperator subPlan = op.getInputs().get(0);
    	LogicalOperator subPlan = op.getOpTable().get(op.getInputs().get(0));
        AugmentBaseData(subPlan, baseData, newBaseData, derivedData, outputConstraints, pigContext);
    }
    
    static void AugmentBaseData(LOCogroup op, Map<LOLoad, DataBag> baseData, Map<LOLoad, DataBag> newBaseData, Map<LogicalOperator, DataBag> derivedData, DataBag outputConstraints, PigContext pigContext) throws IOException {
        // ensure that every group is "big enough" to illustrate grouping/cogrouping semantics
        
        //List<LogicalOperator> inputs = op.getInputs();
    	List<OperatorKey> inputs = op.getInputs();
        int numInputs = inputs.size();
        
        int minGroupSize = (numInputs > 1)? 1 : 2;

        List<DataBag> inputConstraints = new ArrayList<DataBag>();
        for (int i = 0; i < numInputs; i++) inputConstraints.add(BagFactory.getInstance().newDefaultBag());

        // figure out some basic parameters of the grouping expression
        boolean ableToHandle = true;
        //List<ProjectSpec> groupSpecs = new ArrayList<ProjectSpec>(numInputs);
        List<List<Integer>> groupSpecs = new ArrayList<List<Integer>>(numInputs);
        int numGroupCols = -1;
        for (int i = 0; i < numInputs; i++) {
        	
        	/*
        	ableToHandle = false;
        	break;
        	
        	GroupFunc func = op.getSpecs().get(i).groupFunc;
            if (!func.getClass().getName().equals(GFNoop.class.getName())) {
                ableToHandle = false;
                break;
            }
            */

//            EvalSpec args = op.getSpecs().get(i).getArgs();
        	
        	/*EvalSpec args = ((GenerateSpec)(op.getSpecs().get(i))).getSpecs().get(0);
            if (!(args instanceof ProjectSpec)) {
                ableToHandle = false;
                break;
            }
            
            int numGroupColsThisInput = ((ProjectSpec) args).numCols();
            if (numGroupCols == -1) {
                numGroupCols = numGroupColsThisInput;
            } else {
                if (numGroupColsThisInput != numGroupCols) {
                    ableToHandle = false;
                    break;
                }
            }

            groupSpecs.add((ProjectSpec) args);*/
        	List<Integer> cols = new ArrayList<Integer>();
        	for(Iterator<EvalSpec> it = ((GenerateSpec)(op.getSpecs().get(i))).getSpecs().iterator(); it.hasNext();) {
        		EvalSpec arg = it.next();
        		if(arg instanceof ProjectSpec) {
        			int numGroupColsThisInput = ((ProjectSpec) arg).numCols();
                    if (numGroupCols == -1) {
                        numGroupCols = numGroupColsThisInput;
                    } else {
                        if (numGroupColsThisInput != numGroupCols) {
                            ableToHandle = false;
                            break;
                        }
                    }
//                    List<Integer> cols = ((ProjectSpec)arg).getCols();
//                    groupSpecs.add(cols);
//                    cols = ((ProjectSpec)arg).getCols();
                    cols.addAll(((ProjectSpec)arg).getCols());
//        			groupSpecs.get(i).addAll(((ProjectSpec)arg).getCols());
        		} else if(arg instanceof StarSpec) {
        			continue;
        		} else {
        			ableToHandle = false;
        			break;
        		}
        	}
        	if(ableToHandle) {
        		groupSpecs.add(cols);
        		
        	} else {
        		break;
        	}
        	//if(!ableToHandle) break;
        	            
        }

        if (ableToHandle) {
            // first, go through output constraints and create new groups
            for (Iterator<Tuple> it = outputConstraints.iterator(); it.hasNext(); ) {
                Tuple outputConstraint = it.next();
                Datum groupLabel = outputConstraint.getField(0);
                
                for (int input = 0; input < numInputs; input++) {
                    //int numInputFields = inputs.get(input).outputSchema().numFields();
                	int numInputFields = op.getOpTable().get(inputs.get(input)).outputSchema().numFields();
//                    List<Integer> groupCols = groupSpecs.get(input).getCols();
                    List<Integer> groupCols = groupSpecs.get(input);
                    
                    for (int i = 0; i < minGroupSize; i++) {
                        Tuple inputConstraint = GenerateGroupByInput(groupLabel, groupCols, numInputFields);
                        if (inputConstraint != null) inputConstraints.get(input).add(inputConstraint);
                    }
                }
            }
        
            // then, go through all organic data groups and add input constraints to make each group big enough
            DataBag outputData = derivedData.get(op);
            
            for (Iterator<Tuple> it = outputData.iterator(); it.hasNext(); ) {
            	Tuple groupTup = it.next();
            	Datum groupLabel = groupTup.getField(0);

            	for (int input = 0; input < numInputs; input++) {
            		int numInputFields = op.getOpTable().get(inputs.get(input)).outputSchema().numFields();
//          		List<Integer> groupCols = groupSpecs.get(input).getCols();
            		List<Integer> groupCols = groupSpecs.get(input);                    

            		int numTupsToAdd = minGroupSize - groupTup.getBagField(input+1).cardinality();
            		for (int i = 0; i < numTupsToAdd; i++) {
            			Tuple inputConstraint = GenerateGroupByInput(groupLabel, groupCols, numInputFields);
            			if (inputConstraint != null) inputConstraints.get(input).add(inputConstraint);
            		}
            	}
            }
            
        }
        
        // recurse
        for (int i = 0; i < numInputs; i++) {
            AugmentBaseData(op.getOpTable().get(inputs.get(i)), baseData, newBaseData, derivedData, inputConstraints.get(i), pigContext);
        }
    }
    
    static void AugmentBaseData(LOEval op, Map<LOLoad, DataBag> baseData, Map<LOLoad, DataBag> newBaseData, Map<LogicalOperator, DataBag> derivedData, DataBag outputConstraints, PigContext pigContext) throws IOException {
        LogicalOperator inputOp = op.getOpTable().get(op.getInputs().get(0));
        TupleSchema inputSchema = inputOp.outputSchema();
        DataBag inputConstraints = BagFactory.getInstance().newDefaultBag();
        DataBag outputData = derivedData.get(op);
        DataBag inputData = derivedData.get(inputOp);

        EvalSpec spec = op.getSpec();
        if (spec instanceof ProjectSpec) {
            ProjectSpec pSpec = (ProjectSpec) spec;
            List<Integer> cols = pSpec.getCols();

            if (outputConstraints.cardinality() > 0) {   // there's one or more output constraints; propagate them backwards through the projection
                for (Iterator<Tuple> it = outputConstraints.iterator(); it.hasNext(); ) {
                    Tuple outputConstraint = it.next();
                    Tuple inputConst = BackPropConstraint(outputConstraint, cols, inputSchema);
                    ExampleTuple inputConstraint = new ExampleTuple();
                    inputConstraint.copyFrom(inputConst);
                    if (inputConstraint != null) inputConstraints.add(inputConstraint);
                }
            }
            
            // note: if there are no output constraints, we don't have to do anything because the input operator
            // will ensure that we get at least one input tuple, which in turns ensures that we output at least one tuple
            
        } else if (spec instanceof FilterSpec) {
            FilterSpec fSpec = (FilterSpec) spec;
            
            Cond filterCond = fSpec.cond;
            
            // if necessary, insert one or more positive examples (i.e. tuples that pass the filter)
            if (outputConstraints.cardinality() > 0) {     // there's one or more output constraints; generate corresponding input constraints
                for (Iterator<Tuple> it = outputConstraints.iterator(); it.hasNext(); ) {
                    Tuple outputConstraint = it.next();
                    Tuple inputConst = GenerateMatchingTuple(outputConstraint, filterCond);
                    ExampleTuple inputConstraint = new ExampleTuple();
                    inputConstraint.copyFrom(inputConst);
                    if (inputConstraint != null) inputConstraints.add(inputConstraint);
                }
            } else if (outputData.cardinality() == 0) {    // no output constraints, but output is empty; generate one input that will pass the filter
                Tuple inputConst = GenerateMatchingTuple(inputSchema, filterCond);
                ExampleTuple inputConstraint = new ExampleTuple();
                inputConstraint.copyFrom(inputConst);
                if (inputConstraint != null) inputConstraints.add(inputConstraint);
            }
            
            // if necessary, insert a negative example (i.e. a tuple that does not pass the filter)
            if (outputData.cardinality() == inputData.cardinality()) {     // all tuples pass the filter; generate one input that will not pass the filter
                ExampleTuple inputConstraint = new ExampleTuple(); 
                Tuple inputConst = GenerateMatchingTuple(inputSchema, new NotCond(filterCond));
                //inputConstraint.copyFrom(inputConst);
                if (inputConst != null) {
                	inputConstraint.copyFrom(inputConst);
                	inputConstraints.add(inputConstraint);
                }
            }

        } else {
            // d'oh! we got a complicated EvalSpec; since it's unlikely that we can invert it, 
            // just give up and don't add any input constraints
        }
        

        // recurse
        AugmentBaseData(inputOp, baseData, newBaseData, derivedData, inputConstraints, pigContext);
    }
    
    static void AugmentBaseData(LOSort op, Map<LOLoad, DataBag> baseData, Map<LOLoad, DataBag> newBaseData, Map<LogicalOperator, DataBag> derivedData, DataBag outputConstraints, PigContext pigContext) throws IOException {
        throw new UnsupportedOperationException("ExGen does not currently support Sort/Distinct.");
        // TODO
    }
 
    static void AugmentBaseData(LOSplit op, Map<LOLoad, DataBag> baseData, Map<LOLoad, DataBag> newBaseData, Map<LogicalOperator, DataBag> derivedData, DataBag outputConstraints, PigContext pigContext) throws IOException {
        throw new UnsupportedOperationException("ExGen does not currently support Split.");
        // TODO
    }
    
    static void AugmentBaseData(LOUnion op, Map<LOLoad, DataBag> baseData, Map<LOLoad, DataBag> newBaseData, Map<LogicalOperator, DataBag> derivedData, DataBag outputConstraints, PigContext pigContext) throws IOException {
        // round-robin constraint tuples among inputs, to make it likely that we end up with a good final solution

        //List<LogicalOperator> inputs = op.getInputs();
    	List<OperatorKey> inputs = op.getInputs();
        int numInputs = inputs.size();

        List<DataBag> inputConstraints = new ArrayList<DataBag>();
        for (int i = 0; i < numInputs; i++) inputConstraints.add(BagFactory.getInstance().newDefaultBag());
        
        int currentInput = 0;
        for (Iterator<Tuple> it = outputConstraints.iterator(); it.hasNext(); ) {
            Tuple outputConst = it.next();
            ExampleTuple outputConstraint = new ExampleTuple();
            outputConstraint.copyFrom(outputConst);
            inputConstraints.get(currentInput).add(outputConstraint);
            currentInput = (currentInput + 1) % numInputs;
        }
        
        // note: if there are no output constraints, don't have to do anything because inputs will ensure that
        // we get at least one tuple, so we're guaranteed to produce at least one tuple
        
        // recurse
        for (int i = 0; i < numInputs; i++) {
            AugmentBaseData(op.getOpTable().get(inputs.get(i)), baseData, newBaseData, derivedData, inputConstraints.get(i), pigContext);
        }
    }
    
    
    ////////////////////////////
    // HELPER METHODS:
    
    static Tuple GenerateGroupByInput(Datum groupLabel, List<Integer> groupCols, int numInputFields) throws IOException {
        Tuple inputConst = new Tuple(numInputFields);
    	Tuple inputConstraint = new ExampleTuple();
    	inputConstraint.copyFrom(inputConst);
        if (groupLabel != null) {
            if (groupCols.size() == 1) {   // group by one column, so group label is a data atom
                inputConstraint.setField(groupCols.get(0), groupLabel);
            } else {                       // group by multiple columns, so group label is a tuple
                if (!(groupLabel instanceof Tuple)) throw new RuntimeException("Unexpected group label type.");
                Tuple groupLabelTuple = (Tuple) groupLabel;
                        
                for (int outCol = 0; outCol < groupCols.size(); outCol++) {
                    int inCol = groupCols.get(outCol);
                    Datum outVal = groupLabelTuple.getField(outCol);
                    inputConstraint.setField(inCol, outVal);
                }
            }
        }
        return inputConstraint;
    }

    static Tuple BackPropConstraint(Tuple outputConstraint, List<Integer> cols, TupleSchema inputSchema) throws IOException {
        Tuple inputConst = new Tuple(inputSchema.numFields());
        Tuple inputConstraint = new ExampleTuple();
        inputConstraint.copyFrom(inputConst);

        for (int outCol = 0; outCol < outputConstraint.arity(); outCol++) {
            int inCol = cols.get(outCol);
            Datum outVal = outputConstraint.getField(outCol);
            Datum inVal = inputConstraint.getField(inCol);
            
            if (inVal == null) {
                inputConstraint.setField(inCol, outVal);
            } else {
                if (outVal != null) {
                    // unable to back-propagate, due to conflicting column constraints, so give up
                    return null;
                }
            }
        }
        
        return inputConstraint;
    }
    
    // generate a constraint tuple that conforms to the schema and passes the predicate
    // (or null if unable to find such a tuple)
    static Tuple GenerateMatchingTuple(TupleSchema schema, Cond predicate) throws IOException {
        return GenerateMatchingTuple(new Tuple(schema.numFields()), predicate);
    }

    // generate a constraint tuple that conforms to the constraint and passes the predicate 
    // (or null if unable to find such a tuple)
    //
    // for now, constraint tuples are tuples whose fields are a blend of actual data values and nulls, 
    // where a null stands for "don't care"
    //
    // in the future, may want to replace "don't care" with a more rich constraint language; this would
    // help, e.g. in the case of two filters in a row (you want the downstream filter to tell the upstream filter
    // what predicate it wants satisfied in a given field)
    //
    static Tuple GenerateMatchingTuple(Tuple constraint, Cond predicate) throws IOException {
        // first make a copy of the constraint tuple, since we will be modifying it
        Tuple t = new Tuple(constraint.arity());
        for (int i = 0; i < t.arity(); i++) t.setField(i, constraint.getField(i));
        
        // run best-effort attempt at filling in tuple's "don't care" values (i.e. nulls) in order to satisfy predicate
        GenerateMatchingTupleHelper(t, predicate, false);
        
        // test whether the outcome does indeed satisfy the predicate
        boolean tupleWorks = false;
        try {
            tupleWorks = predicate.eval(t);
        } catch (Exception e) {  // since our tuple may contain nulls, the predicate evaluation might encounter one and barf
            tupleWorks = false;
        }
        if (tupleWorks) return t;
        else return null;     // our attempt at finding a tuple that satisfies the predicate failed
    }
    
    static void GenerateMatchingTupleHelper(Tuple t, Cond pred, boolean invert) throws IOException {
        if (pred instanceof AndCond) GenerateMatchingTupleHelper(t, (AndCond) pred, invert);
        else if (pred instanceof CompCond) GenerateMatchingTupleHelper(t, (CompCond) pred, invert);
        else if (pred instanceof FalseCond) GenerateMatchingTupleHelper(t, (FalseCond) pred, invert);
        else if (pred instanceof FuncCond) GenerateMatchingTupleHelper(t, (FuncCond) pred, invert);
        else if (pred instanceof NotCond) GenerateMatchingTupleHelper(t, (NotCond) pred, invert);
        else if (pred instanceof OrCond) GenerateMatchingTupleHelper(t, (OrCond) pred, invert);
        else if (pred instanceof RegexpCond) GenerateMatchingTupleHelper(t, (RegexpCond) pred, invert);
        else if (pred instanceof TrueCond) GenerateMatchingTupleHelper(t, (TrueCond) pred, invert);
        else throw new RuntimeException("Unrecognized Cond type.");
    }

    static void GenerateMatchingTupleHelper(Tuple t, NotCond pred, boolean invert) throws IOException {
        GenerateMatchingTupleHelper(t, pred.cond, !invert);
    }
    
    static void GenerateMatchingTupleHelper(Tuple t, AndCond pred, boolean invert) throws IOException {
        for (Cond c : pred.cList) {
            GenerateMatchingTupleHelper(t, c, invert);
        }
    }
    
    static void GenerateMatchingTupleHelper(Tuple t, OrCond pred, boolean invert) throws IOException {
        for (Cond c : pred.cList) {
            GenerateMatchingTupleHelper(t, c, invert);
        }
    }
    
    static void GenerateMatchingTupleHelper(Tuple t, CompCond pred, boolean invert) throws IOException {
        
        // decide if the eval functions on the two sides of the predicate are simple enough for us to deal with
        boolean leftIsConst = false, rightIsConst = false;
        Datum leftConst = null, rightConst = null;
        int leftCol = -1, rightCol = -1;
        if (pred.left instanceof ConstSpec) {
            leftIsConst = true;
            leftConst = ((ConstSpec) pred.left).atom;
        } else {
            if (!(pred.left instanceof ProjectSpec && ((ProjectSpec) pred.left).numCols() == 1)) return;  // too hard, give up
            leftCol = ((ProjectSpec) pred.left).getCol(0);
            Datum d = t.getField(leftCol);
            if (d != null) {
                leftIsConst = true;
                leftConst = d;
            }
        }
        if (pred.right instanceof ConstSpec) {
            rightIsConst = true;
            rightConst = ((ConstSpec) pred.right).atom;
        } else {
            if (!(pred.right instanceof ProjectSpec && ((ProjectSpec) pred.right).numCols() == 1)) return;  // too hard, give up
            rightCol = ((ProjectSpec) pred.right).getCol(0);
            Datum d = t.getField(rightCol);
            if (d != null) {
                rightIsConst = true;
                rightConst = d;
            }
        }
        if (leftIsConst && rightIsConst) return;  // if both sides are constants, nothing we can do to affect the outcome
        
        // figure out which operator we're dealing with (and invert if requested)
        String op = (invert)? InvertCompOp(pred.op) : pred.op;
        
        // simplify possibilities for ops
        boolean isStringOp = false;
        if (op.equals("eq")) {
            isStringOp = true;
            op = "==";
        } else if (op.equals("neq")) {
            isStringOp = true;
            op = "!=";
        } else if (op.equals("lt") | op.equals("lte")) {
            isStringOp = true;
            op = "<";
        } else if (op.equals("gt") | op.equals("gte")) {
            isStringOp = true;
            op = ">";
        }
        if (op.equals("<=")) op = "<";
        if (op.equals(">=")) op = ">";
        
        // finally, ready to convert some nulls to constants, to try to get the predicate to be true
        if (op.equals("==")) {
            if (leftIsConst) {
                t.setField(rightCol, leftConst);
            } else if (rightIsConst) {
                t.setField(leftCol, rightConst);
            } else {
                t.setField(leftCol, "0");
                t.setField(rightCol, "0");
            }
        } else if (op.equals("!=")) {
            if (leftIsConst) {
                t.setField(rightCol, GetUnequalValue(leftConst, isStringOp));
            } else if (rightIsConst) {
                t.setField(leftCol, GetUnequalValue(rightConst, isStringOp));
            } else {
                t.setField(leftCol, "0");
                t.setField(rightCol, "1");
            }
        } else if (op.equals("<")) {
            if (leftIsConst) {
                t.setField(rightCol, GetLargerValue(leftConst, isStringOp));
            } else if (rightIsConst) {
                t.setField(leftCol, GetSmallerValue(rightConst, isStringOp));
            } else {
                t.setField(leftCol, "0");
                t.setField(rightCol, "1");
            }
        } else if (op.equals(">")) {
            if (leftIsConst) {
                t.setField(rightCol, GetSmallerValue(leftConst, isStringOp));
            } else if (rightIsConst) {
                t.setField(leftCol, GetLargerValue(rightConst, isStringOp));
            } else {
                t.setField(leftCol, "1");
                t.setField(rightCol, "0");
            }
        }
        
    }
    
    static void GenerateMatchingTupleHelper(Tuple t, TrueCond pred, boolean invert) throws IOException {
        // do nothing here (either we're stuck or we're not - no way to influence chance of success)
    }
    
    static void GenerateMatchingTupleHelper(Tuple t, FalseCond pred, boolean invert) throws IOException {
        // do nothing here (either we're stuck or we're not - no way to influence chance of success)
    }
    
    static void GenerateMatchingTupleHelper(Tuple t, FuncCond pred, boolean invert) throws IOException {
        // do nothing here (don't know how to invert functions)
    }
    
    static void GenerateMatchingTupleHelper(Tuple t, RegexpCond pred, boolean invert) throws IOException {
        throw new UnsupportedOperationException("ExGen does not yet support regular expressions.");
        // TODO: in principle, shouldn't be that difficult to generate a string that conforms to a given regexp
        //       (or one that does *not* confirm, if invert=true)
    }
    
    static String InvertCompOp(String op) {
        char op1 = op.charAt(0);
        char op2 = op.length() >= 2 ? op.charAt(1) : '0';
        char op3 = op.length() == 3 ? op.charAt(2) : '0';
        
        switch (op1) {
            // numeric ops first
        case '=':
            if (op2 == '=') {
                return "!=";
            } else {
                throw new RuntimeException("Internal error: Invalid filter operator: " + op);
            }
        case '<':
            if (op2 == '=') {
                return ">=";
            } else {
                return ">";
            }
        case '>':
            if (op2 == '=') {
                return "<=";
            } else {
                return "<";
            }
        case '!':
            if (op2 == '=') {
                return "==";
            } else {
                throw new RuntimeException("Internal error: Invalid filter operator: " + op);
            }
            // now string ops
        case 'e':
            if (op2 == 'q') {
                return "neq";
            } else {
                throw new RuntimeException("Internal error: Invalid filter operator: " + op);
            }
        case 'l':
            if (op2 == 't' && op3 == 'e') {
                return "gte";
            } else {
                return "gt";
            }
        case 'g':
            if (op2 == 't' && op3 == 'e') {
                return "lte";
            } else {
                return "lt";
            }
        case 'n':
            if (op2 == 'e' && op3 == 'q') {
                return "eq";
            } else {
                throw new RuntimeException("Internal error: Invalid filter operator: " + op);
            }
        default:
            throw new RuntimeException("Internal error: Invalid filter operator: " + op);
        }
    }
    
    static Datum GetUnequalValue(Datum v, boolean stringOp) {
        if (!(v instanceof DataAtom)) return null;
        
        DataAtom zero = new DataAtom("0");
        if (v.equals(zero)) return new DataAtom("1");
        else return zero;
    }
    
    static Datum GetSmallerValue(Datum v, boolean stringOp) {
        if (!(v instanceof DataAtom)) return null;

        if (stringOp) {
            String val = ((DataAtom) v).strval();
            if (val.length() > 0) return new DataAtom(val.substring(0, val.length()-1));
            else return null;   // don't know how to make a string smaller than the empty string
        } else {
            double val = ((DataAtom) v).numval();
            return new DataAtom(val-1);
        }
    }
    
    static Datum GetLargerValue(Datum v, boolean stringOp) {
        if (!(v instanceof DataAtom)) return null;

        if (stringOp) {
            String val = ((DataAtom) v).strval();
            return new DataAtom(val + "0");
        } else {
            double val = ((DataAtom) v).numval();
            return new DataAtom(val+1);
        }
    }

}
