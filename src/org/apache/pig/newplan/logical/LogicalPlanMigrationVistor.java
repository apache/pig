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
package org.apache.pig.newplan.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOGenerate;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LONative;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LOCogroup.GROUPTYPE;
import org.apache.pig.impl.logicalLayer.LOJoin.JOINTYPE;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

/**
 * Translate old logical plan into new logical plan
 */
public class LogicalPlanMigrationVistor extends LOVisitor { 
    private org.apache.pig.newplan.logical.relational.LogicalPlan logicalPlan;
    private Map<LogicalOperator, LogicalRelationalOperator> opsMap;
    private Map<org.apache.pig.newplan.logical.expression.LogicalExpression, LogicalOperator> scalarAliasMap = 
        new HashMap<org.apache.pig.newplan.logical.expression.LogicalExpression, LogicalOperator>();
   
    public LogicalPlanMigrationVistor(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        logicalPlan = new org.apache.pig.newplan.logical.relational.LogicalPlan();
        opsMap = new HashMap<LogicalOperator, LogicalRelationalOperator>();
    }    
    
    private void translateConnection(LogicalOperator oldOp, org.apache.pig.newplan.Operator newOp) {       
        List<LogicalOperator> preds = mPlan.getPredecessors(oldOp); 
        
        if(preds != null) {            
            for(LogicalOperator pred: preds) {
                org.apache.pig.newplan.Operator newPred = opsMap.get(pred);
                newOp.getPlan().connect(newPred, newOp);                 
            }
        }        
    }      
    
    private LogicalExpressionPlan translateExpressionPlan(LogicalPlan lp, LogicalOperator oldOp, LogicalRelationalOperator op) throws VisitorException {
        PlanWalker<LogicalOperator, LogicalPlan> childWalker = 
            new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
        
        LogicalExpPlanMigrationVistor childPlanVisitor = new LogicalExpPlanMigrationVistor(lp, oldOp, op, mPlan, scalarAliasMap);
        
        childWalker.walk(childPlanVisitor);
        return childPlanVisitor.exprPlan;
    }
      
    public org.apache.pig.newplan.logical.relational.LogicalPlan getNewLogicalPlan() {
        return logicalPlan;
    }
    
    public void visit(LOCogroup cg) throws VisitorException {
        
        // Get the GroupType information
        org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE grouptype;
        if( cg.getGroupType() == GROUPTYPE.COLLECTED ) {
            grouptype = org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE.COLLECTED;
        } else if (cg.getGroupType() == GROUPTYPE.MERGE ){
            grouptype = org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE.MERGE;
        } else {
            grouptype = org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE.REGULAR;
        }
        
        // Convert the multimap of expressionplans to a new way
        ArrayList<LogicalOperator> inputs = (ArrayList<LogicalOperator>) cg.getInputs();
        MultiMap<Integer, LogicalExpressionPlan> newExpressionPlans = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        
        org.apache.pig.newplan.logical.relational.LOCogroup newCogroup =
            new org.apache.pig.newplan.logical.relational.LOCogroup
            (logicalPlan, newExpressionPlans, grouptype, cg.getInner(), 
                    cg.getRequestedParallelism() );
        
        for( int i = 0; i < inputs.size(); i++ ) {
            ArrayList<LogicalPlan> plans = 
                (ArrayList<LogicalPlan>) cg.getGroupByPlans().get(inputs.get(i));
            for( LogicalPlan plan : plans ) {
                LogicalExpressionPlan expPlan = translateExpressionPlan(plan, cg, newCogroup);
                newExpressionPlans.put(Integer.valueOf(i), expPlan);
            }
        }
        
        newCogroup.setAlias(cg.getAlias());
        newCogroup.setRequestedParallelism(cg.getRequestedParallelism());
        newCogroup.setCustomPartitioner(cg.getCustomPartitioner());
        
        logicalPlan.add(newCogroup);
        opsMap.put(cg, newCogroup);
        translateConnection(cg, newCogroup);
    }

    public void visit(LOJoin loj) throws VisitorException {
        // List of join predicates 
        List<LogicalOperator> inputs = loj.getInputs();
        
        // mapping of inner plans for each input
        MultiMap<Integer, LogicalExpressionPlan> joinPlans = 
                        new MultiMap<Integer, LogicalExpressionPlan>();
        
        JOINTYPE type = loj.getJoinType();
        org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE newType = org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE.HASH;;
        switch(type) {        
        case REPLICATED:
            newType = org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE.REPLICATED;
            break;        	
        case SKEWED:
            newType = org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE.SKEWED;
            break;
        case MERGE:
            newType = org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE.MERGE;
            break;        
        }
        
        boolean[] isInner = loj.getInnerFlags();
        org.apache.pig.newplan.logical.relational.LOJoin join = 
            new org.apache.pig.newplan.logical.relational.LOJoin(logicalPlan, joinPlans, newType, isInner);
        
        for (int i=0; i<inputs.size(); i++) {
            List<LogicalPlan> plans = (List<LogicalPlan>) loj.getJoinPlans().get(inputs.get(i));
            for (LogicalPlan lp : plans) {                               
                joinPlans.put(i, translateExpressionPlan(lp, loj, join));
            }        
        }
        
        join.setAlias(loj.getAlias());
        join.setRequestedParallelism(loj.getRequestedParallelism());
        join.setCustomPartitioner(join.getCustomPartitioner());
        
        logicalPlan.add(join);
        opsMap.put(loj, join);       
        translateConnection(loj, join);           
    }

    public void visit(LOCross cross) throws VisitorException {
        // List of join predicates 
        org.apache.pig.newplan.logical.relational.LOCross newCross = 
            new org.apache.pig.newplan.logical.relational.LOCross(logicalPlan);
     
        newCross.setAlias(cross.getAlias());
        newCross.setRequestedParallelism(cross.getRequestedParallelism());
        newCross.setCustomPartitioner(cross.getCustomPartitioner());
        
        logicalPlan.add(newCross);
        opsMap.put(cross, newCross);       
        translateConnection(cross, newCross);           
    }
    
    public void visit(LOForEach forEach) throws VisitorException {
        
        org.apache.pig.newplan.logical.relational.LOForEach newForeach = 
                new org.apache.pig.newplan.logical.relational.LOForEach(logicalPlan);
        
        org.apache.pig.newplan.logical.relational.LogicalPlan innerPlan = 
            new org.apache.pig.newplan.logical.relational.LogicalPlan();
        
        newForeach.setInnerPlan(innerPlan);
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        
        List<Boolean> fl = forEach.getFlatten();
        boolean[] flat = new boolean[fl.size()];
        for(int i=0; i<fl.size(); i++) {
            flat[i] = fl.get(i);
        }
        org.apache.pig.newplan.logical.relational.LOGenerate gen = 
            new org.apache.pig.newplan.logical.relational.LOGenerate(innerPlan, expPlans, flat);
        
        if (forEach.getUserDefinedSchema()!=null) {
            List<LogicalSchema> userDefinedSchema = new ArrayList<LogicalSchema>();
            for (Schema schema : forEach.getUserDefinedSchema()) {
                userDefinedSchema.add(Util.translateSchema(schema));
            }
            gen.setUserDefinedSchema(userDefinedSchema);
        }
        innerPlan.add(gen);                
        
        List<LogicalPlan> ll = forEach.getForEachPlans();
        try {
            for(int i=0; i<ll.size(); i++) {
                LogicalPlan lp = ll.get(i);
                ForeachInnerPlanVisitor v = new ForeachInnerPlanVisitor(newForeach, forEach, lp, mPlan, scalarAliasMap);
                v.visit();
                
                expPlans.add(v.exprPlan);
            }
        } catch (FrontendException e) {
            throw new VisitorException("Cannot create ForeachInnerPlanVisitor", e);
        }
        
        newForeach.setAlias(forEach.getAlias());
        newForeach.setRequestedParallelism(forEach.getRequestedParallelism());
        
        logicalPlan.add(newForeach);
        opsMap.put(forEach, newForeach);       
        translateConnection(forEach, newForeach);
    }

    public void visit(LOSort sort) throws VisitorException {
        List<LogicalPlan> sortPlans = sort.getSortColPlans();
        List<LogicalExpressionPlan> newSortPlans = new ArrayList<LogicalExpressionPlan>();
        
        org.apache.pig.newplan.logical.relational.LOSort newSort = 
            new org.apache.pig.newplan.logical.relational.LOSort(logicalPlan, 
                    newSortPlans, sort.getAscendingCols(), sort.getUserFunc());
        
        for (LogicalPlan sortPlan : sortPlans) {
            LogicalExpressionPlan newSortPlan = translateExpressionPlan(sortPlan, sort, newSort);
            newSortPlans.add(newSortPlan);
        }
        
        newSort.setAlias(sort.getAlias());
        newSort.setRequestedParallelism(sort.getRequestedParallelism());
        newSort.setLimit(sort.getLimit());
        logicalPlan.add(newSort);
        opsMap.put(sort, newSort);
        translateConnection(sort, newSort);
    }

    public void visit(LOLimit limit) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LOLimit newLimit = 
            new org.apache.pig.newplan.logical.relational.LOLimit(logicalPlan, limit.getLimit());
        
        newLimit.setAlias(limit.getAlias());
        newLimit.setRequestedParallelism(limit.getRequestedParallelism());
        
        logicalPlan.add(newLimit);
        opsMap.put(limit, newLimit);
        translateConnection(limit, newLimit);
    }
    
    public void visit(LOStream stream) throws VisitorException {
        
        LogicalSchema s;
        try {
            s = Util.translateSchema(stream.getSchema());
        }catch(Exception e) {
            throw new VisitorException("Failed to translate schema.", e);
        }
        
        org.apache.pig.newplan.logical.relational.LOStream newStream = 
            new org.apache.pig.newplan.logical.relational.LOStream(logicalPlan,
                    stream.getExecutableManager(), stream.getStreamingCommand(), s);
        
        newStream.setAlias(stream.getAlias());
        newStream.setRequestedParallelism(stream.getRequestedParallelism());
        
        logicalPlan.add(newStream);
        opsMap.put(stream, newStream);
        translateConnection(stream, newStream);
    }
    
    public void visit(LOFilter filter) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LOFilter newFilter = new org.apache.pig.newplan.logical.relational.LOFilter(logicalPlan);
        
        LogicalPlan filterPlan = filter.getComparisonPlan();
        LogicalExpressionPlan newFilterPlan = translateExpressionPlan(filterPlan, filter, newFilter);
      
        newFilter.setFilterPlan(newFilterPlan);
        newFilter.setAlias(filter.getAlias());
        newFilter.setRequestedParallelism(filter.getRequestedParallelism());
        
        logicalPlan.add(newFilter);
        opsMap.put(filter, newFilter);       
        translateConnection(filter, newFilter);
    }

    public void visit(LOSplit split) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LOSplit newSplit = 
            new org.apache.pig.newplan.logical.relational.LOSplit(logicalPlan);
     
        newSplit.setAlias(split.getAlias());
        newSplit.setRequestedParallelism(split.getRequestedParallelism());
        
        logicalPlan.add(newSplit);
        opsMap.put(split, newSplit);
        translateConnection(split, newSplit);
    }

    public void visit(LOGenerate g) throws VisitorException {
        throw new VisitorException("LOGenerate is not supported.");
    }
    
    public void visit(LOLoad load) throws VisitorException{      
        FileSpec fs = load.getInputFile();
        
        LogicalSchema s = null;
        try {
            s = Util.translateSchema(load.getSchema());
        }catch(Exception e) {
            throw new VisitorException("Failed to translate schema.", e);
        }
        
        org.apache.pig.newplan.logical.relational.LOLoad ld = 
            new org.apache.pig.newplan.logical.relational.LOLoad(fs, s, logicalPlan, load.getConfiguration());
        
        ld.setAlias(load.getAlias());
        ld.setRequestedParallelism(load.getRequestedParallelism());
        
        logicalPlan.add(ld);        
        opsMap.put(load, ld);
        translateConnection(load, ld);
    }
    

    public void visit(LOStore store) throws VisitorException{
        org.apache.pig.newplan.logical.relational.LOStore newStore = 
                new org.apache.pig.newplan.logical.relational.LOStore(logicalPlan, store.getOutputFile());    	
       
        newStore.setAlias(store.getAlias());
        newStore.setRequestedParallelism(store.getRequestedParallelism());
        newStore.setSignature(store.getSignature());
        newStore.setInputSpec(store.getInputSpec());
        newStore.setSortInfo(store.getSortInfo());
        newStore.setTmpStore(store.isTmpStore());
        
        logicalPlan.add(newStore);
        opsMap.put(store, newStore);       
        translateConnection(store, newStore);
    }    

    public void visit(LOUnion union) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LOUnion newUnion = 
            new org.apache.pig.newplan.logical.relational.LOUnion(logicalPlan);
        
        newUnion.setAlias(union.getAlias());
        newUnion.setRequestedParallelism(union.getRequestedParallelism());
        logicalPlan.add(newUnion);
        opsMap.put(union, newUnion);
        translateConnection(union, newUnion);
    }
    
    public void visit(LOSplitOutput splitOutput) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LOSplitOutput newSplitOutput = 
            new org.apache.pig.newplan.logical.relational.LOSplitOutput(logicalPlan);
        
        LogicalPlan filterPlan = splitOutput.getConditionPlan();
        LogicalExpressionPlan newFilterPlan = translateExpressionPlan(filterPlan, splitOutput, newSplitOutput);
      
        newSplitOutput.setFilterPlan(newFilterPlan);
        newSplitOutput.setAlias(splitOutput.getAlias());
        newSplitOutput.setRequestedParallelism(splitOutput.getRequestedParallelism());
        
        logicalPlan.add(newSplitOutput);
        opsMap.put(splitOutput, newSplitOutput);
        translateConnection(splitOutput, newSplitOutput);
    }

    public void visit(LODistinct distinct) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LODistinct newDistinct = 
            new org.apache.pig.newplan.logical.relational.LODistinct(logicalPlan);
        
        newDistinct.setAlias(distinct.getAlias());
        newDistinct.setRequestedParallelism(distinct.getRequestedParallelism());
        newDistinct.setCustomPartitioner(distinct.getCustomPartitioner());
        
        logicalPlan.add(newDistinct);
        opsMap.put(distinct, newDistinct);
        translateConnection(distinct, newDistinct);
    }

    
    public void visit(LONative nativeMR) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LONative newNativeMR = 
            new org.apache.pig.newplan.logical.relational.LONative(
                    logicalPlan,
                    nativeMR.getNativeMRJar(),
                    nativeMR.getParams()
            );
        newNativeMR.setAlias(nativeMR.getAlias());
        newNativeMR.setRequestedParallelism(nativeMR.getRequestedParallelism());
        newNativeMR.setCustomPartitioner(nativeMR.getCustomPartitioner());
        
        logicalPlan.add(newNativeMR);
        opsMap.put(nativeMR, newNativeMR);
        translateConnection(nativeMR, newNativeMR);
    }
    

    
    public void finish() {
        for(org.apache.pig.newplan.logical.expression.LogicalExpression exp: scalarAliasMap.keySet()) {
            ((org.apache.pig.newplan.logical.expression.UserFuncExpression)exp).setImplicitReferencedOperator(
                    opsMap.get(scalarAliasMap.get(exp)));
        }
    }
    
}
