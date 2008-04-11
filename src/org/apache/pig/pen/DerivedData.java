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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.local.executionengine.PORead;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.FilterSpec;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOEval;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.impl.util.LineageTracer;

public class DerivedData {
	
	static Map<LogicalOperator, DataBag> CreateDerivedData(LogicalOperator op, Map<LOLoad, DataBag> baseData, Map<OperatorKey, OperatorKey> logicalToPhysicalKeys, Map<OperatorKey, ExecPhysicalOperator> physicalOpTable) throws IOException {
        return CreateDerivedData(op, baseData, null, null, null, logicalToPhysicalKeys, physicalOpTable);
    }

    static Map<LogicalOperator, DataBag> CreateDerivedData(LogicalOperator op, Map<LOLoad, DataBag> baseData, LineageTracer lineageTracer, Collection<IdentityHashSet<Tuple>> equivalenceClasses, Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses, Map<OperatorKey, OperatorKey> logicalToPhysicalKeys, Map<OperatorKey, ExecPhysicalOperator> physicalOpTable) throws IOException {
        Map<LogicalOperator, DataBag> derivedData = new HashMap<LogicalOperator, DataBag>();
        CreateDerivedData(op, baseData, derivedData, lineageTracer, equivalenceClasses, OperatorToEqClasses, logicalToPhysicalKeys, physicalOpTable);

        return derivedData;
    }
    
            
    static void CreateDerivedData(LogicalOperator op, Map<LOLoad, DataBag> baseData, Map<LogicalOperator, DataBag> derivedData, LineageTracer lineageTracer, Collection<IdentityHashSet<Tuple>> equivalenceClasses, Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses, Map<OperatorKey, OperatorKey> logicalToPhysicalKeys, Map<OperatorKey, ExecPhysicalOperator> physicalOpTable) throws IOException {
        
        // first, recurse on upstream operators (if any)
        for(OperatorKey input : op.getInputs()) {
    		CreateDerivedData(op.getOpTable().get(input), baseData, derivedData, lineageTracer, equivalenceClasses, OperatorToEqClasses, logicalToPhysicalKeys, physicalOpTable);
    	}

        if (op instanceof LOLoad) {
            DataBag output = baseData.get(op);
            derivedData.put(op, output);
                
            // insert base data into lineage tracer
            if (lineageTracer != null) {
            	
                for (Iterator<Tuple> it = output.iterator(); it.hasNext(); ) {
                    lineageTracer.insert(it.next());
                }
            }
            
            if (equivalenceClasses != null) {
            	Collection<IdentityHashSet<Tuple>> eqClasses = GetEquivalenceClasses(op, derivedData);
            	equivalenceClasses.addAll(eqClasses);

            	OperatorToEqClasses.put(op, eqClasses);
            }
        } else if (op instanceof LOStore) {
            derivedData.put(op, derivedData.get(op.getInputs().get(0)));
        } else {
            derivedData.put(op, EvaluateOperator(op, lineageTracer, derivedData, logicalToPhysicalKeys, physicalOpTable));
            if (equivalenceClasses != null) {
            	Collection<IdentityHashSet<Tuple>> eqClasses = GetEquivalenceClasses(op, derivedData);
            	//equivalenceClasses.addAll(GetEquivalenceClasses(op, derivedData));
            	equivalenceClasses.addAll(eqClasses);

            	OperatorToEqClasses.put(op, eqClasses);
            }
        }
     
    }
    
    static DataBag EvaluateOperator(LogicalOperator logOp, LineageTracer lineageTracer, Map<LogicalOperator, DataBag> derivedData, Map<OperatorKey, OperatorKey> logicalToPhysicalKeys, Map<OperatorKey, ExecPhysicalOperator> physicalOpTable) throws IOException {
    	//We have the compiled physical plan. We just replace the inputs to the physical operator corresponding to the supplied logical
    	//operator by POReads reading from databags from the derivedData
    	PhysicalOperator pOp = (PhysicalOperator)physicalOpTable.get(logicalToPhysicalKeys.get(logOp.getOperatorKey()));
    	if(lineageTracer != null) {
    		pOp.setLineageTracer(lineageTracer);
    	}
    	OperatorKey[] origInputs = new OperatorKey[pOp.inputs.length];
    	for(int i = 0; i < origInputs.length; ++i) {
    		origInputs[i] = pOp.inputs[i];
    	}
    	
    	for(int i = 0; i < pOp.inputs.length; ++i) {
    		PORead read = new PORead(logOp.getScope(), 
					NodeIdGenerator.getGenerator().getNextNodeId(logOp.getScope()), 
					physicalOpTable, 
					LogicalOperator.FIXED, 
					derivedData.get(logOp.getOpTable().get(logOp.getInputs().get(i))));
    		
    		pOp.inputs[i] = read.getOperatorKey();
    	}
    	
    	//get the output data
    	DataBag outputData = BagFactory.getInstance().newDefaultBag();
        pOp.open();
        Tuple t = pOp.getNext();
        while(t != null) {
        	outputData.add(t);
        	t = pOp.getNext();
        }
        
        pOp.close();
        
        //restore the physical operator to its previous form
        pOp.setLineageTracer(null);
        for(int i = 0; i < origInputs.length; ++i) {
        	physicalOpTable.remove(pOp.inputs[i]);
        	pOp.inputs[i] = origInputs[i];
        }
        
        return outputData;
    	
    }
    
    public static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LogicalOperator op, Map<LogicalOperator, DataBag> derivedData) throws IOException {
        if (op instanceof LOCogroup) return GetEquivalenceClasses((LOCogroup) op, derivedData);
        else if (op instanceof LOEval) return GetEquivalenceClasses((LOEval) op, derivedData);
        else if (op instanceof LOSort) return GetEquivalenceClasses((LOSort) op, derivedData);
        else if (op instanceof LOSplit) return GetEquivalenceClasses((LOSplit) op, derivedData);
        else if (op instanceof LOUnion) return GetEquivalenceClasses((LOUnion) op, derivedData);
        else if (op instanceof LOLoad) return GetEquivalenceClasses((LOLoad) op, derivedData);
        else throw new RuntimeException("Unrecognized logical operator.");
    }
    
    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOLoad op, Map<LogicalOperator, DataBag> derivedData) throws IOException {
    	//Since its a load, all the tuples belong to a single equivalence class
    	Collection<IdentityHashSet<Tuple>> equivClasses = new LinkedList<IdentityHashSet<Tuple>>();
    	IdentityHashSet<Tuple> input = new IdentityHashSet<Tuple> ();
    	
    	equivClasses.add(input);
    	
    	DataBag output = derivedData.get(op);
    	
    	for(Iterator<Tuple> it = output.iterator(); it.hasNext();) {
    		Tuple t = it.next();
    		
    		input.add(t);
    	}
    	
    	return equivClasses;
    }
    
    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOCogroup op, Map<LogicalOperator, DataBag> derivedData) throws IOException {
        
        // make a single equivalence class, comprised of the set of group tuples that properly illustrate grouping (i.e. ones with adequate cardinality of the inner bags)
        
        Collection<IdentityHashSet<Tuple>> equivClasses = new LinkedList<IdentityHashSet<Tuple>>();
        IdentityHashSet<Tuple> acceptableGroups = new IdentityHashSet<Tuple>();
        equivClasses.add(acceptableGroups);

        DataBag output = derivedData.get(op);
        for (Iterator<Tuple> it = output.iterator(); it.hasNext(); ) {
            Tuple group = it.next();
            
            boolean isAcceptable;
            
            if (group.arity() == 2) {     // for single-relation group-by, need a group with at least 2 tuples
                isAcceptable = (group.getBagField(1).cardinality() >= 2);
            } else {                      // for co-group, need a group with at least one tuple from each input relation
                isAcceptable = true;
                for (int field = 1; field < group.arity(); field++) {
                    DataBag bag = group.getBagField(field);
                    if (bag.cardinality() == 0) {
                        isAcceptable = false;
                        break;
                    }
                }
            }
            
            if (isAcceptable) acceptableGroups.add(group);
        }
        
        return equivClasses;
    }
    
    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOEval op, Map<LogicalOperator, DataBag> derivedData) throws IOException {
        
        Collection<IdentityHashSet<Tuple>> equivClasses = new LinkedList<IdentityHashSet<Tuple>>();
        
        EvalSpec spec = op.getSpec();
        
        if (spec instanceof FilterSpec) {    // it's a FILTER
            FilterSpec fSpec = (FilterSpec) spec;

            // make two equivalence classes: one for tuples that pass the filter, and one for tuples that fail the filter
            IdentityHashSet<Tuple> pass = new IdentityHashSet<Tuple>();
            IdentityHashSet<Tuple> fail = new IdentityHashSet<Tuple>();
            equivClasses.add(pass);
            equivClasses.add(fail);
        
            //DataBag input = derivedData.get(op.getInputs().get(0));
            DataBag input = derivedData.get(op.getOpTable().get(op.getInputs().get(0)));
            for (Iterator<Tuple> it = input.iterator(); it.hasNext(); ) {
                Tuple t = it.next();
                if (fSpec.cond.eval(t)) pass.add(t);
                else fail.add(t);
            }
            
        } else {    // it's a FOREACH
            
            // make a single equivalence class containing all output tuples (presumably, any of them makes an equally good demonstration)
            IdentityHashSet<Tuple> equivClass = new IdentityHashSet<Tuple>();
            equivClasses.add(equivClass);
        
            DataBag output = derivedData.get(op);
            for (Iterator<Tuple> it = output.iterator(); it.hasNext(); ) {
                equivClass.add(it.next());
            }
            
        }
        
        return equivClasses;
    }
    
    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOSort op, Map<LogicalOperator, DataBag> derivedData) throws IOException {
        return new LinkedList<IdentityHashSet<Tuple>>();      // SortDistinct doesn't add any new equivalence classes
    }

    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOSplit op, Map<LogicalOperator, DataBag> derivedData) throws IOException {
        throw new RuntimeException("LOSplit not supported yet in example generator.");
    }

    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOUnion op, Map<LogicalOperator, DataBag> derivedData) throws IOException {
    
        // make one equivalence class per input relation
        
        Collection<IdentityHashSet<Tuple>> equivClasses = new LinkedList<IdentityHashSet<Tuple>>();
        
        for (OperatorKey opKey : op.getInputs()) {
        	LogicalOperator input = op.getOpTable().get(opKey);
            IdentityHashSet<Tuple> equivClass = new IdentityHashSet<Tuple>();
            
            DataBag inputTable = derivedData.get(input);
            for (Iterator<Tuple> it = inputTable.iterator(); it.hasNext(); ) {
                equivClass.add(it.next());
            }
            
            equivClasses.add(equivClass);
        }
        
        return equivClasses;
    }


}
