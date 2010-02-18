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
package org.apache.pig.experimental.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.AndExpression;
import org.apache.pig.experimental.logical.expression.CastExpression;
import org.apache.pig.experimental.logical.expression.ConstantExpression;
import org.apache.pig.experimental.logical.expression.EqualExpression;
import org.apache.pig.experimental.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.experimental.logical.expression.GreaterThanExpression;
import org.apache.pig.experimental.logical.expression.LessThanEqualExpression;
import org.apache.pig.experimental.logical.expression.LessThanExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.OrExpression;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LOInnerLoad;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.LOAdd;
import org.apache.pig.impl.logicalLayer.LOAnd;
import org.apache.pig.impl.logicalLayer.LOBinCond;
import org.apache.pig.impl.logicalLayer.LOCast;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LODivide;
import org.apache.pig.impl.logicalLayer.LOEqual;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOGenerate;
import org.apache.pig.impl.logicalLayer.LOGreaterThan;
import org.apache.pig.impl.logicalLayer.LOGreaterThanEqual;
import org.apache.pig.impl.logicalLayer.LOIsNull;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LOLesserThan;
import org.apache.pig.impl.logicalLayer.LOLesserThanEqual;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOMapLookup;
import org.apache.pig.impl.logicalLayer.LOMod;
import org.apache.pig.impl.logicalLayer.LOMultiply;
import org.apache.pig.impl.logicalLayer.LONegative;
import org.apache.pig.impl.logicalLayer.LONot;
import org.apache.pig.impl.logicalLayer.LONotEqual;
import org.apache.pig.impl.logicalLayer.LOOr;
import org.apache.pig.impl.logicalLayer.LOProject;
import org.apache.pig.impl.logicalLayer.LORegexp;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LOSubtract;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LOUserFunc;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LOJoin.JOINTYPE;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

public class LogicalPlanMigrationVistor extends LOVisitor { 
    private org.apache.pig.experimental.logical.relational.LogicalPlan logicalPlan;
    private HashMap<LogicalOperator, LogicalRelationalOperator> opsMap;
   
    public LogicalPlanMigrationVistor(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        logicalPlan = new org.apache.pig.experimental.logical.relational.LogicalPlan();
        opsMap = new HashMap<LogicalOperator, LogicalRelationalOperator>();
    }    
    
    private LogicalSchema translateSchema(Schema schema) {    	
        if (schema == null) {
            return null;
        }
        
        LogicalSchema s2 = new LogicalSchema();
        List<Schema.FieldSchema> ll = schema.getFields();
        for (Schema.FieldSchema f: ll) {
            LogicalSchema.LogicalFieldSchema f2 = 
                new LogicalSchema.LogicalFieldSchema(f.alias, translateSchema(f.schema), f.type);
                       
            s2.addField(f2);
        }
        
        return s2;
    }
    
    private void translateConnection(LogicalOperator oldOp, org.apache.pig.experimental.plan.Operator newOp) {       
        List<LogicalOperator> preds = mPlan.getPredecessors(oldOp); 
        
        if(preds != null) {            
            for(LogicalOperator pred: preds) {
                org.apache.pig.experimental.plan.Operator newPred = opsMap.get(pred);
                newOp.getPlan().connect(newPred, newOp);                 
            }
        }        
    }      
    
    private LogicalExpressionPlan translateExpressionPlan(LogicalPlan lp) throws VisitorException {
        PlanWalker<LogicalOperator, LogicalPlan> childWalker = 
            new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
        
        LogicalExpPlanMigrationVistor childPlanVisitor = new LogicalExpPlanMigrationVistor(lp);
        
        childWalker.walk(childPlanVisitor);
        return childPlanVisitor.exprPlan;
    }
      
    public org.apache.pig.experimental.logical.relational.LogicalPlan getNewLogicalPlan() {
        return logicalPlan;
    }
    
    public void visit(LOCogroup cg) throws VisitorException {
        throw new VisitorException("LOCogroup is not supported.");
    }

    public void visit(LOJoin loj) throws VisitorException {
        // List of join predicates 
        List<LogicalOperator> inputs = loj.getInputs();
        
        // mapping of inner plans for each input
        MultiMap<Integer, LogicalExpressionPlan> joinPlans = 
                        new MultiMap<Integer, LogicalExpressionPlan>();
       
        for (int i=0; i<inputs.size(); i++) {
            List<LogicalPlan> plans = (List<LogicalPlan>) loj.getJoinPlans().get(inputs.get(i));
            for (LogicalPlan lp : plans) {                               
                joinPlans.put(i, translateExpressionPlan(lp));
            }        
        }
        
        JOINTYPE type = loj.getJoinType();
        org.apache.pig.experimental.logical.relational.LOJoin.JOINTYPE newType = org.apache.pig.experimental.logical.relational.LOJoin.JOINTYPE.HASH;;
        switch(type) {        
        case REPLICATED:
            newType = org.apache.pig.experimental.logical.relational.LOJoin.JOINTYPE.REPLICATED;
            break;        	
        case SKEWED:
            newType = org.apache.pig.experimental.logical.relational.LOJoin.JOINTYPE.SKEWED;
            break;
        case MERGE:
            newType = org.apache.pig.experimental.logical.relational.LOJoin.JOINTYPE.MERGE;
            break;        
        }
        
        boolean[] isInner = loj.getInnerFlags();
        org.apache.pig.experimental.logical.relational.LOJoin join = 
            new org.apache.pig.experimental.logical.relational.LOJoin(logicalPlan, joinPlans, newType, isInner);
     
        join.setAlias(loj.getAlias());
        join.setRequestedParallelism(loj.getRequestedParallelism());
        
        logicalPlan.add(join);
        opsMap.put(loj, join);       
        translateConnection(loj, join);           
    }

    public void visit(LOForEach forEach) throws VisitorException {
        
        org.apache.pig.experimental.logical.relational.LOForEach newForeach = 
                new org.apache.pig.experimental.logical.relational.LOForEach(logicalPlan);
        
        org.apache.pig.experimental.logical.relational.LogicalPlan innerPlan = 
            new org.apache.pig.experimental.logical.relational.LogicalPlan();
        
        newForeach.setInnerPlan(innerPlan);
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        
        List<Boolean> fl = forEach.getFlatten();
        boolean[] flat = new boolean[fl.size()];
        for(int i=0; i<fl.size(); i++) {
            flat[i] = fl.get(i);
        }
        org.apache.pig.experimental.logical.relational.LOGenerate gen = 
            new org.apache.pig.experimental.logical.relational.LOGenerate(innerPlan, expPlans, flat);
        
        innerPlan.add(gen);                
        
        List<LogicalPlan> ll = forEach.getForEachPlans();
        for(int i=0; i<ll.size(); i++) {
            LogicalPlan lp = ll.get(i);
            ForeachInnerPlanVisitor v = new ForeachInnerPlanVisitor(newForeach, forEach, lp);
            v.visit();
                        
            innerPlan.connect(v.lastOp, gen);                       
            expPlans.add(v.exprPlan);
        }
        
        newForeach.setAlias(forEach.getAlias());
        newForeach.setRequestedParallelism(forEach.getRequestedParallelism());
        
        logicalPlan.add(newForeach);
        opsMap.put(forEach, newForeach);       
        translateConnection(forEach, newForeach);     
    }

    public void visit(LOSort s) throws VisitorException {
        throw new VisitorException("LOSort is not supported.");
    }

    public void visit(LOLimit limOp) throws VisitorException {
        throw new VisitorException("LOLimit is not supported.");
    }
    
    public void visit(LOStream stream) throws VisitorException {
        throw new VisitorException("LOStream is not supported.");
    }
    
    public void visit(LOFilter filter) throws VisitorException {
        org.apache.pig.experimental.logical.relational.LOFilter newFilter = new org.apache.pig.experimental.logical.relational.LOFilter(logicalPlan);
        
        LogicalPlan filterPlan = filter.getComparisonPlan();
        LogicalExpressionPlan newFilterPlan = translateExpressionPlan(filterPlan);
      
        newFilter.setFilterPlan(newFilterPlan);
        newFilter.setAlias(filter.getAlias());
        newFilter.setRequestedParallelism(filter.getRequestedParallelism());
        
        logicalPlan.add(newFilter);
        opsMap.put(filter, newFilter);       
        translateConnection(filter, newFilter);
    }

    public void visit(LOSplit split) throws VisitorException {
        throw new VisitorException("LOSplit is not supported.");
    }


    public void visit(LOGenerate g) throws VisitorException {
        throw new VisitorException("LOGenerate is not supported.");
    }
    
    public void visit(LOLoad load) throws VisitorException{      
        FileSpec fs = load.getInputFile();
        
        LogicalSchema s = null;
        try {
            s = translateSchema(load.getSchema());
        }catch(Exception e) {
            throw new VisitorException("Failed to translate schema.", e);
        }
        
        org.apache.pig.experimental.logical.relational.LOLoad ld = 
            new org.apache.pig.experimental.logical.relational.LOLoad(fs, s, logicalPlan);
        
        ld.setAlias(load.getAlias());
        ld.setRequestedParallelism(load.getRequestedParallelism());
        
        logicalPlan.add(ld);        
        opsMap.put(load, ld);
        translateConnection(load, ld);
    }
    

    public void visit(LOStore store) throws VisitorException{
        org.apache.pig.experimental.logical.relational.LOStore newStore = 
                new org.apache.pig.experimental.logical.relational.LOStore(logicalPlan, store.getOutputFile());    	
       
        newStore.setAlias(store.getAlias());
        newStore.setRequestedParallelism(store.getRequestedParallelism());
        
        logicalPlan.add(newStore);
        opsMap.put(store, newStore);       
        translateConnection(store, newStore);
    }    

    public void visit(LOUnion u) throws VisitorException {
        throw new VisitorException("LOUnion is not supported.");
    }

    public void visit(LOSplitOutput sop) throws VisitorException {
        throw new VisitorException("LOSplitOutput is not supported.");
    }

    public void visit(LODistinct dt) throws VisitorException {
        throw new VisitorException("LODistinct is not supported.");
    }

    public void visit(LOCross cs) throws VisitorException {
        throw new VisitorException("LOCross is not supported.");
    }
    
    public class LogicalExpPlanMigrationVistor extends LOVisitor { 
        
        protected org.apache.pig.experimental.logical.expression.LogicalExpressionPlan exprPlan;
        protected HashMap<LogicalOperator, LogicalExpression> exprOpsMap;
        protected LogicalPlan oldLogicalPlan;
        
        public LogicalExpPlanMigrationVistor(LogicalPlan plan) {
            super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
            exprPlan = new org.apache.pig.experimental.logical.expression.LogicalExpressionPlan();
            exprOpsMap = new HashMap<LogicalOperator, LogicalExpression>();
            
            oldLogicalPlan = LogicalPlanMigrationVistor.this.mPlan;
        }    

        private void translateConnection(LogicalOperator oldOp, org.apache.pig.experimental.plan.Operator newOp) {       
           List<LogicalOperator> preds = mPlan.getPredecessors(oldOp); 
           
           // the dependency relationship of new expression plan is opposite to the old logical plan
           // for example, a+b, in old plan, "add" is a leave, and "a" and "b" are roots
           // in new plan, "add" is root, and "a" and "b" are leaves.
           if(preds != null) {            
               for(LogicalOperator pred: preds) {
                   org.apache.pig.experimental.plan.Operator newPred = exprOpsMap.get(pred);
                   newOp.getPlan().connect(newOp, newPred);                 
               }
           }        
       }
        
        public void visit(LOProject project) throws VisitorException {
            int col = project.getCol();
            LogicalOperator lg = project.getExpression();
            LogicalOperator succed = oldLogicalPlan.getSuccessors(lg).get(0);
            int input = oldLogicalPlan.getPredecessors(succed).indexOf(lg);
                        
            // get data type of projection
            byte t = DataType.BYTEARRAY;
            try {
                Schema s = lg.getSchema();
                if (s != null) {
                    t = s.getField(col).type;
                }
            }catch(Exception e) {
                throw new VisitorException(e);
            }
            ProjectExpression pe = new ProjectExpression(exprPlan, t, input, col);          
            
            exprPlan.add(pe);
            exprOpsMap.put(project, pe);       
            translateConnection(project, pe);                       
        }
        
        public void visit(LOConst con) throws VisitorException{

            ConstantExpression ce = new ConstantExpression(exprPlan, con.getType(), con.getValue());
            
             exprPlan.add(ce);
             exprOpsMap.put(con, ce);       
             translateConnection(con, ce);
        }
        
        public void visit(LOGreaterThan op) throws VisitorException {
            ExpressionOperator left = op.getLhsOperand();
            ExpressionOperator right = op.getRhsOperand();
                    
            GreaterThanExpression eq = new GreaterThanExpression
            (exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
            exprOpsMap.put(op, eq);
        }

        public void visit(LOLesserThan op) throws VisitorException {
            ExpressionOperator left = op.getLhsOperand();
            ExpressionOperator right = op.getRhsOperand();
                    
            LessThanExpression eq = new LessThanExpression
            (exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
            exprOpsMap.put(op, eq);
        }

        public void visit(LOGreaterThanEqual op) throws VisitorException {
            ExpressionOperator left = op.getLhsOperand();
            ExpressionOperator right = op.getRhsOperand();
                    
            GreaterThanEqualExpression eq = new GreaterThanEqualExpression
            (exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
            exprOpsMap.put(op, eq);
        }

        public void visit(LOLesserThanEqual op) throws VisitorException {
            ExpressionOperator left = op.getLhsOperand();
            ExpressionOperator right = op.getRhsOperand();
                    
            LessThanEqualExpression eq = new LessThanEqualExpression
            (exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
            exprOpsMap.put(op, eq);        }

        public void visit(LOEqual op) throws VisitorException {		
            ExpressionOperator left = op.getLhsOperand();
            ExpressionOperator right = op.getRhsOperand();
                    
            EqualExpression eq = new EqualExpression(exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
            exprOpsMap.put(op, eq);
        }

        public void visit(LOUserFunc func) throws VisitorException {
            throw new VisitorException("LOUserFunc is not supported.");
        }

        public void visit(LOBinCond binCond) throws VisitorException {
            throw new VisitorException("LOBinCond is not supported.");
        }

        public void visit(LOCast cast) throws VisitorException {
            byte b = cast.getType();
            ExpressionOperator exp = cast.getExpression();
            
            CastExpression c = new CastExpression(exprPlan, b, exprOpsMap.get(exp));
            c.setFuncSpec(cast.getLoadFuncSpec());
            exprOpsMap.put(cast, c);
        }
        
        public void visit(LORegexp regexp) throws VisitorException {
            throw new VisitorException("LORegexp is not supported.");
        }

        public void visit(LONotEqual op) throws VisitorException {
            throw new VisitorException("LONotEqual is not supported.");
        }

        public void visit(LOAdd op) throws VisitorException {		
            throw new VisitorException("LOAdd is not supported.");
        }

        public void visit(LOSubtract op) throws VisitorException {
            throw new VisitorException("LOSubtract is not supported.");
        }

        public void visit(LOMultiply op) throws VisitorException {
            throw new VisitorException("LOMultiply is not supported.");
        }

        public void visit(LODivide op) throws VisitorException {
            throw new VisitorException("LODivide is not supported.");
        }

        public void visit(LOMod op) throws VisitorException {
            throw new VisitorException("LOMod is not supported.");
        }

        
        public void visit(LONegative op) throws VisitorException {
            throw new VisitorException("LONegative is not supported.");
        }

        public void visit(LOMapLookup op) throws VisitorException {
            throw new VisitorException("LOMapLookup is not supported.");
        }

        public void visit(LOAnd binOp) throws VisitorException {
            ExpressionOperator left = binOp.getLhsOperand();
            ExpressionOperator right = binOp.getRhsOperand();
                    
            AndExpression ae = new AndExpression(exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
            exprOpsMap.put(binOp, ae);            
        }

        public void visit(LOOr binOp) throws VisitorException {
            ExpressionOperator left = binOp.getLhsOperand();
            ExpressionOperator right = binOp.getRhsOperand();
                    
            OrExpression ae = new OrExpression(exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
            exprOpsMap.put(binOp, ae);
        }

        public void visit(LONot uniOp) throws VisitorException {
            throw new VisitorException("LONot is not supported.");
        }

        public void visit(LOIsNull uniOp) throws VisitorException {
            throw new VisitorException("LOIsNull is not supported.");
        }
    }
    
    // visitor to translate the inner plan of foreach
    // it has all the operators allowed in the inner plan of foreach
    public class ForeachInnerPlanVisitor extends LogicalExpPlanMigrationVistor {
        private org.apache.pig.experimental.logical.relational.LOForEach foreach;
        private LOForEach oldForeach;
        private org.apache.pig.experimental.logical.relational.LOGenerate gen;
        private org.apache.pig.experimental.logical.relational.LogicalPlan newInnerPlan;
               
        private HashMap<LogicalOperator, LogicalRelationalOperator> innerOpsMap;
        private LogicalRelationalOperator lastOp;

        public ForeachInnerPlanVisitor(org.apache.pig.experimental.logical.relational.LOForEach foreach, LOForEach oldForeach, LogicalPlan plan) {
            super(plan);	
            this.foreach = foreach;
            newInnerPlan = foreach.getInnerPlan();
            gen = (org.apache.pig.experimental.logical.relational.LOGenerate)newInnerPlan.getSinks().get(0);
            this.oldForeach = oldForeach;
                        
            innerOpsMap = new HashMap<LogicalOperator, LogicalRelationalOperator>();
        }      
        
        public void visit(LOProject project) throws VisitorException {
            LogicalOperator op = project.getExpression();
            
            if (op == oldLogicalPlan.getPredecessors(oldForeach).get(0)) {
                // if this projection is to get a field from outer plan, change it
                // to LOInnerLoad
                
                LOInnerLoad innerLoad = new LOInnerLoad(newInnerPlan, foreach, project.getCol());               
                newInnerPlan.add(innerLoad);
                
                List<LogicalOperator> ll = mPlan.getSuccessors(project);
                if (ll == null || ll.get(0) instanceof ExpressionOperator || project.isStar()) {  
                    int size = 0;
                    try {
                        List<org.apache.pig.experimental.plan.Operator> suc = newInnerPlan.getPredecessors(gen);
                        if (suc != null) {
                            size = suc.size();
                        }
                    }catch(Exception e) {
                        throw new VisitorException(e);
                    }
                     
                    lastOp = innerLoad;
                    
                    ProjectExpression pe = new ProjectExpression(exprPlan, project.getType(), size, 0);                              
                    exprPlan.add(pe);
                    exprOpsMap.put(project, pe);       
                    translateConnection(project, pe);            
                } else {
                    innerOpsMap.put(project, innerLoad);
                }
            } else {
                super.visit(project);
            }
        }       
        
        public void visit(LOForEach foreach) throws VisitorException {
            throw new VisitorException("LOForEach is not supported as inner plan of foreach");
        }
        
        public void visit(LOSort s) throws VisitorException {
            throw new VisitorException("LOSort is not supported as inner plan of foreach.");
        }

        public void visit(LOLimit limOp) throws VisitorException {
            throw new VisitorException("LOLimit is not supported as inner plan of foreach.");
        }
        
    }
}
