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

package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.PigContext;

/**
 * A visitor to optimize plans that have a sample job that immediately follows a
 * load/store only MR job.  These kinds of plans are generated for order bys, and
 * will soon be generated for joins that need to sample their data first.  These
 * can be changed so that the RandomSampleLoader subsumes the loader used in the
 * first job and then removes the first job.
 */
public class SampleOptimizer extends MROpPlanVisitor {

    private static final Log log = LogFactory.getLog(SampleOptimizer.class);
    private PigContext pigContext;

    public SampleOptimizer(MROperPlan plan, PigContext pigContext) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        this.pigContext = pigContext;
    }

    private List<MapReduceOper> opsToRemove = new ArrayList<MapReduceOper>();

    @Override
    public void visit() throws VisitorException {
        
        super.visit();
        // remove operators marked for removal
        for (MapReduceOper op : opsToRemove) 
            this.mPlan.remove(op);
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        // See if this is a sampling job.
        List<PhysicalOperator> pos = mr.mapPlan.getRoots();
        if (pos == null || pos.size() == 0) {
            log.debug("Map of operator empty");
            return;
        }
        PhysicalOperator po = pos.get(0);
        if (!(po instanceof POLoad)) {
            log.debug("Root operator of map is not load.");
            return; // Huh?
        }
        POLoad load = (POLoad)po;
        String loadFunc = load.getLFile().getFuncName();
        String loadFile = load.getLFile().getFileName();
        if (!("org.apache.pig.impl.builtin.RandomSampleLoader".equals(loadFunc)) && !("org.apache.pig.impl.builtin.PoissonSampleLoader".equals(loadFunc))) {
            log.debug("Not a sampling job.");
            return;
        }
        if (loadFile == null) {
            log.debug("No load file");
            return;
        }

        // Get this job's predecessor.  There should be exactly one.;
        List<MapReduceOper> preds = mPlan.getPredecessors(mr);
        if (preds.size() != 1) {
            log.debug("Too many predecessors to sampling job.");
            return;
        }
        MapReduceOper pred = preds.get(0);

        // The predecessor should be a root.
        List<MapReduceOper> predPreds = mPlan.getPredecessors(pred);
        if (predPreds != null && predPreds.size() > 0) {
            log.debug("Predecessor should be a root of the plan");
            return; 
        }

        // The predecessor should have just a load and store in the map, and nothing
        // in the combine or reduce.
        if ( !(pred.reducePlan.isEmpty() && pred.combinePlan.isEmpty())) {
            log.debug("Predecessor has a combine or reduce plan");
            return;
        }

        // The MR job should have one successor.
        List<MapReduceOper> succs = mPlan.getSuccessors(mr);
        if (succs.size() != 1) {
            log.debug("Job has more than one successor.");
            return;
        }
        MapReduceOper succ = succs.get(0);
        if (pred.mapPlan == null || pred.mapPlan.size() != 2) {
            log.debug("Predecessor has more than just load+store in the map");
            return;
        }

        List<PhysicalOperator> loads = pred.mapPlan.getRoots();
        if (loads.size() != 1) {
            log.debug("Predecessor plan has more than one root.");
            return;
        }
        PhysicalOperator r = loads.get(0);
        if (!(r instanceof POLoad)) { // Huh?
            log.debug("Predecessor's map plan root is not a load.");
            return;
        }
        POLoad predLoad = (POLoad)r;

        // Find the load the correlates with the file the sampler is loading, and
        // check that it is using the temp file storage format.
        if (succ.mapPlan == null) { // Huh?
            log.debug("Successor has no map plan.");
            return;
        }
        loads = succ.mapPlan.getRoots();
        POLoad succLoad = null;
        for (PhysicalOperator root : loads) {
            if (!(root instanceof POLoad)) { // Huh?
                log.debug("Successor's roots are not loads");
                return;
            }
            POLoad sl = (POLoad)root;
            if (loadFile.equals(sl.getLFile().getFileName()) && 
                    Utils.getTmpFileCompressorName(pigContext).equals(sl.getLFile().getFuncName())) {
                succLoad = sl;
                break;
            }
        }

        if (succLoad == null) {
            log.debug("Could not find load that matched file we are sampling.");
            return;
        }

        // Okay, we're on.
        // First, replace our RandomSampleLoader with a RandomSampleLoader that uses
        // the load function from our predecessor.
        String[] rslargs = new String[2];
        FileSpec predFs = predLoad.getLFile();
        // First argument is FuncSpec of loader function to subsume, this we want to set for
        // ourselves.
        rslargs[0] = predFs.getFuncSpec().toString();
        // Add the loader's funcspec to the list of udf's associated with this mr operator
        mr.UDFs.add(rslargs[0]);
        // Second argument is the number of samples per block, read this from the original.
        rslargs[1] = load.getLFile().getFuncSpec().getCtorArgs()[1];
        FileSpec fs = new FileSpec(predFs.getFileName(),new FuncSpec(loadFunc, rslargs));
        POLoad newLoad = new POLoad(load.getOperatorKey(),load.getRequestedParallelism(), fs);
        newLoad.setSignature(predLoad.getSignature());
        newLoad.setLimit(predLoad.getLimit());
        try {
            mr.mapPlan.replace(load, newLoad);
            
            // check if it has PartitionSkewedKeys
            List<PhysicalOperator> ls = mr.reducePlan.getLeaves();
            for(PhysicalOperator op: ls) {
            	scan(mr, op, fs.getFileName());
            }        
        } catch (PlanException e) {
            throw new VisitorException(e);
        }

        // Second, replace the loader in our successor with whatever the originally used loader was.
        fs = new FileSpec(predFs.getFileName(), predFs.getFuncSpec());
        newLoad = new POLoad(succLoad.getOperatorKey(), succLoad.getRequestedParallelism(), fs);
        newLoad.setSignature(predLoad.getSignature());
        try {
            succ.mapPlan.replace(succLoad, newLoad);
            // Add the loader's funcspec to the list of udf's associated with this mr operator
            succ.UDFs.add(newLoad.getLFile().getFuncSpec().toString());
        } catch (PlanException e) {
            throw new VisitorException(e);
        }

        // Cannot delete the pred right now, because we are still traversing the graph. So, mark the pred and remove it from the
        // plan once the visit by this optimizer is complete.
        opsToRemove.add(pred);
    }

    // search for PartionSkewedKeys and update input file name
    // it is always under a POForEach operator in reduce plan
    private void scan(MapReduceOper mr, PhysicalOperator op, String fileName) {
    	
		if (op instanceof POUserFunc) {
			if (((POUserFunc)op).getFuncSpec().getClassName().equals(
					"org.apache.pig.impl.builtin.PartitionSkewedKeys")) {
				
				String[] ctorArgs = ((POUserFunc)op).getFuncSpec().getCtorArgs();
				ctorArgs[2] = fileName;    		
				return;
			}
		}else if (op instanceof POForEach) {
			List<PhysicalPlan> pl = ((POForEach)op).getInputPlans();
			for(PhysicalPlan plan: pl) {
				List<PhysicalOperator> list = plan.getLeaves();
				for (PhysicalOperator pp: list) {
					scan(mr, pp, fileName);
				}
			}
		}else{
			List<PhysicalOperator> preds = mr.reducePlan.getPredecessors(op);
	    	
	    	if (preds == null) {
	    		return;
	    	}
	    	
	    	for(PhysicalOperator p: preds) {	    		    	
	    		scan(mr, p, fileName);	    		
	    	}
		}
    }
}
