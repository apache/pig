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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.DotPlanDumper;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;

/**
 * This class can print a logical plan in the DOT format. It uses
 * clusters to illustrate nesting. If "verbose" is off, it will skip
 * any nesting.
 */
public class DotLOPrinter extends DotPlanDumper {

    public DotLOPrinter(BaseOperatorPlan plan, PrintStream ps) {
        this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
             new HashSet<Operator>());
    }

    private DotLOPrinter(BaseOperatorPlan plan, PrintStream ps, boolean isSubGraph,
                         Set<Operator> subgraphs, 
                         Set<Operator> multiInSubgraphs,
                         Set<Operator> multiOutSubgraphs) {
        super(plan, ps, isSubGraph, subgraphs, 
              multiInSubgraphs, multiOutSubgraphs);
    }

    @Override
    protected DotPlanDumper makeDumper(BaseOperatorPlan plan, PrintStream ps) {
        return new DotLOPrinter(plan, ps, true, mSubgraphs, 
                                mMultiInputSubgraphs,
                                mMultiOutputSubgraphs);
    }

    @Override
    protected String getName(Operator op) {
        StringBuffer info = new StringBuffer(op.getName());
        if (op instanceof ProjectExpression) {
            ProjectExpression pr = (ProjectExpression)op;
            info.append(pr.getInputNum());
            info.append(":");
            if (pr.isProjectStar())
                info.append("(*)");
            else if (pr.isRangeProject())
                info.append("[").append(pr.getStartCol()).append(" .. ").append(pr.getEndCol()).append("]");
            else
                info.append(pr.getColNum());
        }
        return info.toString();
    }

    @Override
    protected String[] getAttributes(Operator op) {
        if (op instanceof LOStore || op instanceof LOLoad) {
            String[] attributes = new String[3];
            attributes[0] = "label=\""+getName(op).replace(":",",\\n")+"\"";
            attributes[1] = "style=\"filled\"";
            attributes[2] = "fillcolor=\"gray\"";
            return attributes;
        }
        else {
            return super.getAttributes(op);
        }
    }

    @Override
    protected MultiMap<Operator, BaseOperatorPlan> 
        getMultiInputNestedPlans(Operator op) {
        
        if(op instanceof LOCogroup){
            MultiMap<Operator, BaseOperatorPlan> planMap = new MultiMap<Operator, BaseOperatorPlan>();
            for (Integer i : ((LOCogroup)op).getExpressionPlans().keySet()) {
                List<BaseOperatorPlan> plans = new ArrayList<BaseOperatorPlan>();
                plans.addAll(((LOCogroup)op).getExpressionPlans().get(i));
                Operator pred = plan.getPredecessors(op).get(i);
                planMap.put(pred, plans);
            }
            return  planMap;
        }
        else if(op instanceof LOJoin){
            MultiMap<Operator, BaseOperatorPlan> planMap = new MultiMap<Operator, BaseOperatorPlan>();
            for (Integer i : ((LOJoin)op).getExpressionPlans().keySet()) {
                List<BaseOperatorPlan> plans = new ArrayList<BaseOperatorPlan>();
                plans.addAll(((LOJoin)op).getExpressionPlans().get(i));
                Operator pred = plan.getPredecessors(op).get(i);
                planMap.put(pred, plans);
            }
            return  planMap;
        }
        return new MultiMap<Operator, BaseOperatorPlan>();
    }

    @Override
    protected Collection<BaseOperatorPlan> getNestedPlans(Operator op) {
        Collection<BaseOperatorPlan> plans = new LinkedList<BaseOperatorPlan>();

        if(op instanceof LOFilter){
            plans.add(((LOFilter)op).getFilterPlan());
        }
        else if(op instanceof LOLimit){
            plans.add(((LOLimit)op).getLimitPlan());
        }
        else if(op instanceof LOForEach){
            plans.add(((LOForEach)op).getInnerPlan());
        }
        else if(op instanceof LOGenerate){
            plans.addAll(((LOGenerate)op).getOutputPlans());
        }
        else if(op instanceof LOSort){
            plans.addAll(((LOSort)op).getSortColPlans()); 
        }
        else if(op instanceof LOSplitOutput){
            plans.add(((LOSplitOutput)op).getFilterPlan());
        }
        
        return plans;
    }
    
    @Override
    protected boolean reverse(BaseOperatorPlan plan) {
        if (plan instanceof LogicalExpressionPlan)
            return true;
        return false;
    }
}
