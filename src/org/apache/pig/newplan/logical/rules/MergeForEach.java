/**
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

package org.apache.pig.newplan.logical.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.Utils;

public class MergeForEach extends Rule {

    private OperatorSubPlan subPlan;
    
    public MergeForEach(String name) {
        super( name, false );
    }

    @Override
    protected OperatorPlan buildPattern() {
        // match each foreach.
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator foreach1 = new LOForEach(plan);
        plan.add( foreach1 );
        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new MergeForEachTransformer();
    }
    
    public class MergeForEachTransformer extends Transformer {
        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            LOForEach foreach1 = (LOForEach)matched.getSources().get(0);
            List<Operator> succs = currentPlan.getSuccessors( foreach1 );
            if( succs == null || succs.size() != 1 || !( succs.get(0) instanceof LOForEach) )
                return false;
            
            LOForEach foreach2 = (LOForEach)succs.get(0);
            
            // Check if the second foreach has only LOGenerate and LOInnerLoad
            Iterator<Operator> it = foreach2.getInnerPlan().getOperators();
            while( it.hasNext() ) {
                Operator op = it.next();
                if(!(op instanceof LOGenerate) && !(op instanceof LOInnerLoad))
                    return false;
            }
            
            // Check if the first foreach has flatten in its generate statement.
            LOGenerate gen1 = (LOGenerate)foreach1.getInnerPlan().getSinks().get(0);
            for (boolean flatten : gen1.getFlattenFlags()) {
                if( flatten )
                    return false;
            }
            
            // Check if non of the 1st foreach output is referred more than once in second foreach.
            // Otherwise, we may do expression calculation more than once, defeat the benefit of this
            // optimization
            Set<Integer> inputs = new HashSet<Integer>();
            for (Operator op : foreach2.getInnerPlan().getSources()) {
                // If the source is not LOInnerLoad, then it must be LOGenerate. This happens when 
                // the 1st ForEach does not rely on any input of 2nd ForEach
                if (op instanceof LOInnerLoad) {
                    LOInnerLoad innerLoad = (LOInnerLoad)op;
                    int input = innerLoad.getProjection().getColNum();
                    if (inputs.contains(input))
                        return false;
                    else
                        inputs.add(input);
                    
                    if (innerLoad.getProjection().isProjectStar())
                        return false;
                }
            }
            
            return true;
        }

        @Override
        public OperatorPlan reportChanges() {
            return subPlan;
        }

        private void addBranchToPlan(LOGenerate gen, int branch, OperatorPlan newPlan) {
            Operator op = gen.getPlan().getPredecessors(gen).get(branch);
            newPlan.add(op);
            op.setPlan(newPlan);
            Operator pred;
            if (gen.getPlan().getPredecessors(op)!=null)
                pred = gen.getPlan().getPredecessors(op).get(0);
            else
                pred = null;
            while (pred!=null) {
                newPlan.add(pred);
                pred.setPlan(newPlan);
                newPlan.connect(pred, op);
                op = pred;
                if (gen.getPlan().getPredecessors(pred)!=null)
                    pred = gen.getPlan().getPredecessors(pred).get(0);
                else
                    pred = null;
            }
        }
        
        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            subPlan = new OperatorSubPlan(currentPlan);
            
            LOForEach foreach1 = (LOForEach)matched.getSources().get(0);
            LOGenerate gen1 = (LOGenerate)foreach1.getInnerPlan().getSinks().get(0);
            
            LOForEach foreach2 = (LOForEach)currentPlan.getSuccessors(foreach1).get(0);
            LOGenerate gen2 = (LOGenerate)foreach2.getInnerPlan().getSinks().get(0);
            
            LOForEach newForEach = new LOForEach(currentPlan);
            LogicalPlan newForEachInnerPlan = new LogicalPlan();
            newForEach.setInnerPlan(newForEachInnerPlan);
            newForEach.setAlias(foreach2.getAlias());
            newForEach.setRequestedParallelism(foreach1.getRequestedParallelisam());
            List<LogicalExpressionPlan> newExpList = new ArrayList<LogicalExpressionPlan>();
            LOGenerate newGen = new LOGenerate(newForEachInnerPlan, newExpList, gen2.getFlattenFlags());
            newForEachInnerPlan.add(newGen);
            
            for (LogicalExpressionPlan exp2 : gen2.getOutputPlans()) {
                LogicalExpressionPlan newExpPlan = new LogicalExpressionPlan();
                newExpPlan.merge(exp2);
                
                // Add expression plan in 2nd ForEach
                List<Operator> exp2Sinks = new ArrayList<Operator>();
                exp2Sinks.addAll(newExpPlan.getSinks());
                for (Operator exp2Sink : exp2Sinks) {
                    if (exp2Sink instanceof ProjectExpression) {
                        // Find referred expression plan in 1st ForEach
                        ProjectExpression proj = (ProjectExpression)exp2Sink;
                        LOInnerLoad innerLoad = (LOInnerLoad)foreach2.getInnerPlan().getPredecessors(gen2).get(proj.getInputNum());
                        int exp1Pos = innerLoad.getProjection().getColNum();
                        LogicalExpressionPlan exp1 = gen1.getOutputPlans().get(exp1Pos);
                        List<Operator> exp1Sources = newExpPlan.merge(exp1);
                        
                        // Copy expression plan to the new ForEach, connect to the expression plan of 2nd ForEach
                        Operator exp1Source = exp1Sources.get(0);
                        if (newExpPlan.getPredecessors(exp2Sink)!=null) {
                            Operator exp2NextToSink = newExpPlan.getPredecessors(exp2Sink).get(0);
                            Pair<Integer, Integer> pos = newExpPlan.disconnect(exp2NextToSink, exp2Sink);
                            newExpPlan.remove(exp2Sink);
                            newExpPlan.connect(exp2NextToSink, pos.first, exp1Source, pos.second);
                        }
                        else {
                            newExpPlan.remove(exp2Sink);
                        }
                    }
                }
                
                // Copy referred ForEach1 inner plan to new ForEach
                List<Operator> exp1Sinks = newExpPlan.getSinks();
                for (Operator exp1Sink : exp1Sinks) {
                    if (exp1Sink instanceof ProjectExpression) {
                        addBranchToPlan(gen1, ((ProjectExpression)exp1Sink).getInputNum(), newForEachInnerPlan);
                        Operator opNextToGen = foreach1.getInnerPlan().getPredecessors(gen1).get(((ProjectExpression)exp1Sink).getInputNum());
                        newForEachInnerPlan.connect(opNextToGen, newGen);
                        int input = newForEachInnerPlan.getPredecessors(newGen).indexOf(opNextToGen);
                        ((ProjectExpression)exp1Sink).setInputNum(input);
                    }
                }
                
                newExpList.add(newExpPlan);
            }
            
            // Adjust attachedOp
            for (LogicalExpressionPlan p : newGen.getOutputPlans()) {
                Iterator<Operator> iter = p.getOperators();
                while (iter.hasNext()) {
                    Operator op = iter.next();
                    if (op instanceof ProjectExpression) {
                        ((ProjectExpression)op).setAttachedRelationalOp(newGen);
                    }
                }
            }
            
            Iterator<Operator> iter = newForEach.getInnerPlan().getOperators();
            while (iter.hasNext()) {
                Operator op = iter.next();
                if (op instanceof LOInnerLoad) {
                    ((LOInnerLoad)op).getProjection().setAttachedRelationalOp(newForEach);
                }
            }
            // remove foreach1, foreach2, add new foreach
            Operator pred = currentPlan.getPredecessors(foreach1).get(0);
            Operator succ = currentPlan.getSuccessors(foreach2).get(0);
            
            // rebuild soft link
            Collection<Operator> newSoftLinkPreds = Utils.mergeCollection(currentPlan.getSoftLinkPredecessors(foreach1), 
                    currentPlan.getSoftLinkPredecessors(foreach2));
            
            Collection<Operator> foreach1SoftLinkPred = null;
            if (currentPlan.getSoftLinkPredecessors(foreach1)!=null) {
                foreach1SoftLinkPred = new ArrayList<Operator>();
                foreach1SoftLinkPred.addAll(currentPlan.getSoftLinkPredecessors(foreach1));
            }
            if (foreach1SoftLinkPred!=null) {
                for (Operator softPred : foreach1SoftLinkPred) {
                    currentPlan.removeSoftLink(softPred, foreach1);
                }
            }
            
            Collection<Operator> foreach2SoftLinkPred = null;
            if (currentPlan.getSoftLinkPredecessors(foreach2)!=null) {
                foreach2SoftLinkPred = new ArrayList<Operator>();
                foreach2SoftLinkPred.addAll(currentPlan.getSoftLinkPredecessors(foreach2));
            }
            if (foreach2SoftLinkPred!=null) {
                for (Operator softPred : foreach2SoftLinkPred) {
                    currentPlan.removeSoftLink(softPred, foreach2);
                }
            }
            
            Pair<Integer, Integer> pos = currentPlan.disconnect(pred, foreach1);
            currentPlan.disconnect(foreach1, foreach2);
            currentPlan.disconnect(foreach2, succ);
            currentPlan.remove(foreach1);
            currentPlan.remove(foreach2);

            currentPlan.add(newForEach);
            currentPlan.connect(pred, pos.first, newForEach, pos.second);
            currentPlan.connect(newForEach, succ);
            
            if (newSoftLinkPreds!=null) {
                for (Operator softPred : newSoftLinkPreds) {
                    currentPlan.createSoftLink(softPred, newForEach);
                }
            }
            
            subPlan.add(newForEach);
        }
    }
}
