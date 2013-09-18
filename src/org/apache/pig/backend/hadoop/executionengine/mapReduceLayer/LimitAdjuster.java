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

import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Utils;

public class LimitAdjuster extends MROpPlanVisitor {
    ArrayList<MapReduceOper> opsToAdjust = new ArrayList<MapReduceOper>();  
    PigContext pigContext;
    NodeIdGenerator nig;
    private String scope;


    public LimitAdjuster(MROperPlan plan, PigContext pigContext) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        this.pigContext = pigContext;
        nig = NodeIdGenerator.getGenerator();
        List<MapReduceOper> roots = plan.getRoots();
        scope = roots.get(0).getOperatorKey().getScope();
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        // Look for map reduce operators which contains limit operator.
        // If so, add one additional map-reduce
        // operator with 1 reducer into the original plan.

        // TODO: This new MR job can be skipped if at runtime we discover that
        // its parent only has a single reducer (mr.requestedParallelism!=1).
        // This check MUST happen at runtime since that's when reducer estimation happens.
        if ((mr.limit!=-1 || mr.limitPlan!=null) )
        {
            opsToAdjust.add(mr);
        }
    }
    
    public void adjust() throws IOException, PlanException
    {
        for (MapReduceOper mr:opsToAdjust)
        {
            if (mr.reducePlan.isEmpty()) continue;
            List<PhysicalOperator> mpLeaves = mr.reducePlan.getLeaves();
            if (mpLeaves.size() != 1) {
                int errCode = 2024; 
                String msg = "Expected reduce to have single leaf. Found " + mpLeaves.size() + " leaves.";
                throw new MRCompilerException(msg, errCode, PigException.BUG);
            }
            PhysicalOperator mpLeaf = mpLeaves.get(0);
            if (!pigContext.inIllustrator) {
                if (!(mpLeaf instanceof POStore)) {
                    int errCode = 2025;
                    String msg = "Expected leaf of reduce plan to " +
                        "always be POStore. Found " + mpLeaf.getClass().getSimpleName();
                    throw new MRCompilerException(msg, errCode, PigException.BUG);
                }
            }
            FileSpec oldSpec = ((POStore)mpLeaf).getSFile();
            boolean oldIsTmpStore = ((POStore)mpLeaf).isTmpStore();
            
            FileSpec fSpec = new FileSpec(FileLocalizer.getTemporaryPath(pigContext).toString(),
                    new FuncSpec(Utils.getTmpFileCompressorName(pigContext)));
            POStore storeOp = (POStore) mpLeaf;
            storeOp.setSFile(fSpec);
            storeOp.setIsTmpStore(true);
            mr.setReduceDone(true);
            MapReduceOper limitAdjustMROp = new MapReduceOper(new OperatorKey(scope,nig.getNextNodeId(scope)));
            POLoad ld = new POLoad(new OperatorKey(scope,nig.getNextNodeId(scope)));
            ld.setPc(pigContext);
            ld.setLFile(fSpec);
            ld.setIsTmpLoad(true);
            limitAdjustMROp.mapPlan.add(ld);
            if (mr.isGlobalSort()) {
                connectMapToReduceLimitedSort(limitAdjustMROp, mr);
            } else {
                MRUtil.simpleConnectMapToReduce(limitAdjustMROp, scope, nig);
            }
            // Need to split the original reduce plan into two mapreduce job:
            // 1st: From the root(POPackage) to POLimit
            // 2nd: From POLimit to leaves(POStore), duplicate POLimit
            // The reason for doing that:
            // 1. We need to have two map-reduce job, otherwise, we will end up with
            //    N*M records, N is number of reducer, M is limit constant. We need 
            //    one extra mapreduce job with 1 reducer
            // 2. We don't want to move operator after POLimit into the first mapreduce
            //    job, because:
            //    * Foreach will shift the key type for second mapreduce job, see PIG-461
            //    * Foreach flatten may generating more than M records, which get cut 
            //      by POLimit, see PIG-2231
            splitReducerForLimit(limitAdjustMROp, mr);

            if (mr.isGlobalSort()) 
            {
                limitAdjustMROp.setLimitAfterSort(true);
                limitAdjustMROp.setSortOrder(mr.getSortOrder());
            }
            
            POStore st = new POStore(new OperatorKey(scope,nig.getNextNodeId(scope)));
            st.setSFile(oldSpec);
            st.setIsTmpStore(oldIsTmpStore);
            st.setSchema(((POStore)mpLeaf).getSchema());
            st.setSignature(((POStore)mpLeaf).getSignature());
            
            limitAdjustMROp.reducePlan.addAsLeaf(st);
            limitAdjustMROp.requestedParallelism = 1;
            limitAdjustMROp.setLimitOnly(true);
            
            List<MapReduceOper> successorList = mPlan.getSuccessors(mr);
            MapReduceOper successors[] = null;
            
            // Save a snapshot for successors, since we will modify MRPlan, 
            // use the list directly will be problematic
            if (successorList!=null && successorList.size()>0)
            {
                successors = new MapReduceOper[successorList.size()];
                int i=0;
                for (MapReduceOper op:successorList)
                    successors[i++] = op;
            }
            
            // Process UDFs
            for (String udf : mr.UDFs) {
                if (!limitAdjustMROp.UDFs.contains(udf)) {
                    limitAdjustMROp.UDFs.add(udf);
                }
            }
            
            mPlan.add(limitAdjustMROp);
            mPlan.connect(mr, limitAdjustMROp);
            
            if (successors!=null)
            {
                for (int i=0;i<successors.length;i++)
                {
                    MapReduceOper nextMr = successors[i];
                    if (nextMr!=null)
                        mPlan.disconnect(mr, nextMr);
                    
                    if (nextMr!=null)
                        mPlan.connect(limitAdjustMROp, nextMr);                        
                }
            }
        }
    }
    
    // Move all operators between POLimit and POStore in reducer plan 
    // from firstMROp to the secondMROp
    private void splitReducerForLimit(MapReduceOper secondMROp,
            MapReduceOper firstMROp) throws PlanException, VisitorException {
                    
        PhysicalOperator op = firstMROp.reducePlan.getRoots().get(0);
        assert(op instanceof POPackage);
        
        while (true) {
            List<PhysicalOperator> succs = firstMROp.reducePlan
                    .getSuccessors(op);
            if (succs==null) break;
            op = succs.get(0);
            if (op instanceof POLimit) {
                // find operator after POLimit
                op = firstMROp.reducePlan.getSuccessors(op).get(0);
                break;
            }
        }
        
        POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
        pLimit2.setLimit(firstMROp.limit);
        pLimit2.setLimitPlan(firstMROp.limitPlan);
        secondMROp.reducePlan.addAsLeaf(pLimit2);

        while (true) {
            if (op instanceof POStore) break;
            PhysicalOperator opToMove = op;
            List<PhysicalOperator> succs = firstMROp.reducePlan
                    .getSuccessors(op);
            op = succs.get(0);
            
            firstMROp.reducePlan.removeAndReconnect(opToMove);
            secondMROp.reducePlan.addAsLeaf(opToMove);
            
        }
    }
    
    private void connectMapToReduceLimitedSort(MapReduceOper mro, MapReduceOper sortMROp) throws PlanException, VisitorException
    {
        POLocalRearrange slr = (POLocalRearrange)sortMROp.mapPlan.getLeaves().get(0);
        
        POLocalRearrange lr = null;
        try {
            lr = slr.clone();
        } catch (CloneNotSupportedException e) {
            int errCode = 2147;
            String msg = "Error cloning POLocalRearrange for limit after sort";
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
        
        mro.mapPlan.addAsLeaf(lr);
        
        POPackage spkg = (POPackage)sortMROp.reducePlan.getRoots().get(0);

        POPackage pkg = null;
        try {
            pkg = spkg.clone();
        } catch (Exception e) {
            int errCode = 2148;
            String msg = "Error cloning POPackageLite for limit after sort";
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
        mro.reducePlan.add(pkg);
        mro.reducePlan.addAsLeaf(MRUtil.getPlainForEachOP(scope, nig));
    }
}