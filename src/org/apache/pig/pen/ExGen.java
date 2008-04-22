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

import org.apache.pig.PigServer.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.executionengine.ExecPhysicalPlan;
import org.apache.pig.backend.local.executionengine.LocalExecutionEngine;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.ExampleTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.impl.util.LineageTracer;

public class ExGen {

	final static int SAMPLE_SIZE = 10000;    // size of sample used in initial downstream pass
    
    static Map<LOLoad, DataBag> GlobalBaseData = new HashMap<LOLoad, DataBag>();
    
    public static Map<LogicalOperator, DataBag> GenerateExamples(LogicalPlan plan, PigContext hadoopPigContext) throws IOException {
        
    	long time = System.currentTimeMillis();
    	String Result;
    	PigContext pigContext = new PigContext(ExecType.LOCAL, hadoopPigContext.getProperties());
    	
    	//compile the logical plan to get the physical plan once and for all
    	ExecPhysicalPlan PhyPlan = null;
    	try {
    		pigContext.connect();
			PhyPlan = pigContext.getExecutionEngine().compile(plan, null);
		} catch (ExecException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Map<OperatorKey, OperatorKey> logicalToPhysicalKeys = ((LocalExecutionEngine)pigContext.getExecutionEngine()).getLogicalToPhysicalMap();
		Map<OperatorKey, ExecPhysicalOperator> physicalOpTable = PhyPlan.getOpTable();
    	    	
        // Acquire initial base data by sampling from input relations (this is idempotent)
    	FetchBaseData.ReadBaseData(plan.getRootOperator(), GlobalBaseData, SAMPLE_SIZE, hadoopPigContext);
    	    	
        /////// PASS 1: push data sample through query plan
        
        // Push base data through query plan
        Map<LogicalOperator, DataBag> derivedData;
        Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses = new HashMap<LogicalOperator, Collection<IdentityHashSet<Tuple>>>();
        Collection<IdentityHashSet<Tuple>> equivalenceClasses = new LinkedList<IdentityHashSet<Tuple>>();
        derivedData = DerivedData.CreateDerivedData(plan.getRootOperator(), GlobalBaseData, logicalToPhysicalKeys, physicalOpTable);
//        derivedData = DerivedData.CreateDerivedData(plan.getRootOperator(), GlobalBaseData, logicalToPhysicalKeys, physicalOpTable);
        
        /////// PASS 2: augment data sample to reduce sparsity
        
        // Use constraint back-prop to synthesize additional base data, in order to reduce sparsity
        // (and keep track of which tuples are synthetic)
        IdentityHashSet<Tuple> syntheticTuples = new IdentityHashSet<Tuple>();  
        Map<LOLoad, DataBag> modifiedBaseData = AugmentData.AugmentBaseData(plan.getRootOperator(), GlobalBaseData, syntheticTuples, derivedData, pigContext);
        
        {
        	LineageTracer lineage = new LineageTracer();
        	derivedData = DerivedData.CreateDerivedData(plan.getRootOperator(), modifiedBaseData, lineage, equivalenceClasses, OperatorToEqClasses, logicalToPhysicalKeys, physicalOpTable);
        	modifiedBaseData = ShapeLineage.PruneBaseData(modifiedBaseData, derivedData.get(plan.getRoot()), syntheticTuples, lineage, equivalenceClasses);

        }

        {
        	LineageTracer lineage = new LineageTracer();
        	derivedData = DerivedData.CreateDerivedData(plan.getRootOperator(), modifiedBaseData, lineage, equivalenceClasses, OperatorToEqClasses, logicalToPhysicalKeys, physicalOpTable);
        	modifiedBaseData = ShapeLineage.TrimLineages(plan.getRootOperator(), modifiedBaseData, derivedData, lineage, OperatorToEqClasses, logicalToPhysicalKeys, physicalOpTable);
        }
        
        {
        	LineageTracer lineage = new LineageTracer();
        	derivedData = DerivedData.CreateDerivedData(plan.getRootOperator(), modifiedBaseData, lineage, null, null, logicalToPhysicalKeys, physicalOpTable);
        
        }
        
        // Push finalized base data through query plan, to generate final answer
        // also mark the tuples being displayed non-omittable

        //derivedData = CreateDerivedData(plan.getRootOperator(), modifiedBaseData, logicalToPhysicalKeys, physicalOpTable);
        /*for(Map.Entry<LogicalOperator, DataBag> e : derivedData.entrySet()) {
        	DataBag bag = e.getValue();
        	for(Iterator<Tuple> it = bag.iterator(); it.hasNext(); ) {
        		ExampleTuple t = (ExampleTuple) it.next();
        		t.omittable = false;
        		
        	}
        }*/
        
        /*Result = PrintExamples(plan, derivedData, OperatorToEqClasses);
        System.out.println("Time taken : " + (System.currentTimeMillis() - time) + "ms");
        return Result;*/
        return derivedData;
    }
    
}
