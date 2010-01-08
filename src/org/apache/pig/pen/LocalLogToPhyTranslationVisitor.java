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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogToPhyTranslationVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrangeForIllustrate;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.pen.physicalOperators.POCounter;
import org.apache.pig.pen.physicalOperators.POCogroup;
import org.apache.pig.pen.physicalOperators.POCross;
import org.apache.pig.pen.physicalOperators.POSplit;
import org.apache.pig.pen.physicalOperators.POSplitOutput;
import org.apache.pig.pen.physicalOperators.POStreamLocal;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DependencyOrderWalkerWOSeenChk;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;


public class LocalLogToPhyTranslationVisitor extends LogToPhyTranslationVisitor {

    private Log log = LogFactory.getLog(getClass());
    
    public LocalLogToPhyTranslationVisitor(LogicalPlan plan) {
	super(plan);
	// TODO Auto-generated constructor stub
    }
    
    public Map<LogicalOperator, PhysicalOperator> getLogToPhyMap() {
	return logToPhyMap;
    }
    
    @Override
    public void visit(LOCogroup cg) throws VisitorException {
	String scope = cg.getOperatorKey().scope;
        List<LogicalOperator> inputs = cg.getInputs();
        
        POCogroup poc = new POCogroup(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), cg.getRequestedParallelism());
        poc.setInner(cg.getInner());
        currentPlan.add(poc);
        
        int count = 0;
        Byte type = null;
        for(LogicalOperator lo : inputs) {
            List<LogicalPlan> plans = (List<LogicalPlan>) cg.getGroupByPlans().get(lo);
            
            POLocalRearrangeForIllustrate physOp = new POLocalRearrangeForIllustrate(new OperatorKey(
                    scope, nodeGen.getNextNodeId(scope)), cg
                    .getRequestedParallelism());
            List<PhysicalPlan> exprPlans = new ArrayList<PhysicalPlan>();
            currentPlans.push(currentPlan);
            for (LogicalPlan lp : plans) {
                currentPlan = new PhysicalPlan();
                PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
                        .spawnChildWalker(lp);
                pushWalker(childWalker);
                mCurrentWalker.walk(this);
                exprPlans.add(currentPlan);
                popWalker();

            }
            currentPlan = currentPlans.pop();
            try {
                physOp.setPlans(exprPlans);
            } catch (PlanException pe) {
                throw new VisitorException(pe);
            }
            try {
                physOp.setIndex(count++);
            } catch (ExecException e1) {
                throw new VisitorException(e1);
            }
            if (plans.size() > 1) {
                type = DataType.TUPLE;
                physOp.setKeyType(type);
            } else {
                type = exprPlans.get(0).getLeaves().get(0).getResultType();
                physOp.setKeyType(type);
            }
            physOp.setResultType(DataType.TUPLE);

            currentPlan.add(physOp);

            try {
                currentPlan.connect(logToPhyMap.get(lo), physOp);
                currentPlan.connect(physOp, poc);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
                throw new VisitorException(e);
            }
            
        }
        logToPhyMap.put(cg, poc);
    }
    
    @Override
    public void visit(LOJoin join) throws VisitorException {
        String scope = join.getOperatorKey().scope;
        List<LogicalOperator> inputs = join.getInputs();
        boolean[] innerFlags = join.getInnerFlags();

        // In local mode, LOJoin is achieved by POCogroup followed by a POForEach with flatten
        // Insert a POCogroup in the place of LOJoin
        POCogroup poc = new POCogroup(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), join.getRequestedParallelism());
        poc.setInner(innerFlags);
        
        currentPlan.add(poc);
        
        // Add innner plans to POCogroup
        int count = 0;
        Byte type = null;
        for(LogicalOperator lo : inputs) {
            List<LogicalPlan> plans = (List<LogicalPlan>) join.getJoinPlans().get(lo);
            
            POLocalRearrangeForIllustrate physOp = new POLocalRearrangeForIllustrate(new OperatorKey(
                    scope, nodeGen.getNextNodeId(scope)), join
                    .getRequestedParallelism());
            List<PhysicalPlan> exprPlans = new ArrayList<PhysicalPlan>();
            currentPlans.push(currentPlan);
            for (LogicalPlan lp : plans) {
                currentPlan = new PhysicalPlan();
                PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
                        .spawnChildWalker(lp);
                pushWalker(childWalker);
                mCurrentWalker.walk(this);
                exprPlans.add(currentPlan);
                popWalker();

            }
            currentPlan = currentPlans.pop();
            try {
                physOp.setPlans(exprPlans);
            } catch (PlanException pe) {
                throw new VisitorException(pe);
            }
            try {
                physOp.setIndex(count++);
            } catch (ExecException e1) {
                throw new VisitorException(e1);
            }
            if (plans.size() > 1) {
                type = DataType.TUPLE;
                physOp.setKeyType(type);
            } else {
                type = exprPlans.get(0).getLeaves().get(0).getResultType();
                physOp.setKeyType(type);
            }
            physOp.setResultType(DataType.TUPLE);

            currentPlan.add(physOp);

            try {
                currentPlan.connect(logToPhyMap.get(lo), physOp);
                currentPlan.connect(physOp, poc);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
                throw new VisitorException(e);
            }
            
        }
        
        // Append POForEach after POCogroup
        List<Boolean> flattened = new ArrayList<Boolean>();
        List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
        
        for (int i=0;i<join.getInputs().size();i++)
        {
            PhysicalPlan ep = new PhysicalPlan();
            POProject prj = new POProject(new OperatorKey(scope,nodeGen.getNextNodeId(scope)));
            prj.setResultType(DataType.BAG);
            prj.setColumn(i+1);
            prj.setOverloaded(false);
            prj.setStar(false);
            ep.add(prj);
            eps.add(ep);
            // the parser would have marked the side
            // where we need to keep empty bags on
            // non matched as outer (innerFlags[i] would be
            // false)
            if(!(innerFlags[i])) {
                LogicalOperator joinInput = inputs.get(i);
                // for outer join add a bincond
                // which will project nulls when bag is
                // empty
                try {
                    updateWithEmptyBagCheck(ep, joinInput);
                } catch (PlanException e) {
                    throw new VisitorException(e);
                }
            }
            flattened.add(true);
        }
        
        POForEach fe = new POForEach(new OperatorKey(scope,nodeGen.getNextNodeId(scope)),-1,eps,flattened);
        
        fe.setResultType(DataType.BAG);

        currentPlan.add(fe);
        logToPhyMap.put(join, fe);
        try {
            currentPlan.connect(poc, fe);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }
    
    @Override
    public void visit(LOSplit split) throws VisitorException {
	String scope = split.getOperatorKey().scope;
        PhysicalOperator physOp = new POSplit(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), split.getRequestedParallelism());
        
        logToPhyMap.put(split, physOp);

        currentPlan.add(physOp);
        PhysicalOperator from = logToPhyMap.get(split.getPlan()
                .getPredecessors(split).get(0));
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
            throw new VisitorException(e);
        }
    }
    
    @Override
    public void visit(LOSplitOutput split) throws VisitorException {
	String scope = split.getOperatorKey().scope;
        PhysicalOperator physOp = new POSplitOutput(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), split.getRequestedParallelism());
        logToPhyMap.put(split, physOp);

        currentPlan.add(physOp);
        currentPlans.push(currentPlan);
        currentPlan = new PhysicalPlan();
        PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
                .spawnChildWalker(split.getConditionPlan());
        pushWalker(childWalker);
        mCurrentWalker.walk(this);
        popWalker();

        ((POSplitOutput) physOp).setPlan(currentPlan);
        currentPlan = currentPlans.pop();
        currentPlan.add(physOp);
        PhysicalOperator from = logToPhyMap.get(split.getPlan()
                .getPredecessors(split).get(0));
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
            throw new VisitorException(e);
        }
    }
    
    @Override
    public void visit(LOStream stream) throws VisitorException {
        String scope = stream.getOperatorKey().scope;
        POStreamLocal poStream = new POStreamLocal(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), stream.getExecutableManager(), 
                stream.getStreamingCommand(), pc.getProperties());
        currentPlan.add(poStream);
        logToPhyMap.put(stream, poStream);
        
        List<LogicalOperator> op = stream.getPlan().getPredecessors(stream);

        PhysicalOperator from = logToPhyMap.get(op.get(0));
        try {
            currentPlan.connect(from, poStream);
        } catch (PlanException e) {
            log.error("Invalid physical operators in the physical plan"
                    + e.getMessage());
            throw new VisitorException(e);
        }
    }
    
    @Override
    public void visit(LOCross cross) throws VisitorException {
        String scope = cross.getOperatorKey().scope;
        
        POCross pocross = new POCross(new OperatorKey(scope, nodeGen.getNextNodeId(scope)));
        logToPhyMap.put(cross, pocross);
        currentPlan.add(pocross);
        
        
        for(LogicalOperator in : cross.getInputs()) {
            PhysicalOperator from = logToPhyMap.get(in);
            try {
                currentPlan.connect(from, pocross);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
                throw new VisitorException(e);
            }
        }
        //currentPlan.explain(System.out);
    }
    
    @Override
    public void visit(LOStore loStore) throws VisitorException {
        String scope = loStore.getOperatorKey().scope;
        POStore store = new POStore(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)));
        store.setSFile(loStore.getOutputFile());
        store.setInputSpec(loStore.getInputSpec());
        try {
            // create a new schema for ourselves so that when
            // we serialize we are not serializing objects that
            // contain the schema - apparently Java tries to
            // serialize the object containing the schema if
            // we are trying to serialize the schema reference in
            // the containing object. The schema here will be serialized
            // in JobControlCompiler
            store.setSchema(new Schema(loStore.getSchema()));
        } catch (FrontendException e1) {
            int errorCode = 1060;
            String message = "Cannot resolve Store output schema";  
            throw new VisitorException(message, errorCode, PigException.BUG, e1);    
        }
        //store.setPc(pc);
        currentPlan.add(store);
        PhysicalOperator from = logToPhyMap.get(loStore
                .getPlan().getPredecessors(loStore).get(0));
        
        POCounter counter = new POCounter(new OperatorKey(scope, nodeGen.getNextNodeId(scope)));
        currentPlan.add(counter);
        try {
            currentPlan.connect(from, counter);
            currentPlan.connect(counter, store);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
        logToPhyMap.put(loStore, store);
        
    }

}
