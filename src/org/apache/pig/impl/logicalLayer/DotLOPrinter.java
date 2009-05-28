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
package org.apache.pig.impl.logicalLayer;

import java.io.PrintStream;
import java.util.List;
import java.util.LinkedList;
import java.util.Collection;
import org.apache.pig.impl.util.MultiMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.impl.plan.DotPlanDumper;
import org.apache.pig.impl.plan.Operator;

/**
 * This class can print a logical plan in the DOT format. It uses
 * clusters to illustrate nesting. If "verbose" is off, it will skip
 * any nesting.
 */
public class DotLOPrinter extends DotPlanDumper<LogicalOperator, LogicalPlan,
                                  LogicalOperator, LogicalPlan> {

    public DotLOPrinter(LogicalPlan plan, PrintStream ps) {
        this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
             new HashSet<Operator>());
    }

    private DotLOPrinter(LogicalPlan plan, PrintStream ps, boolean isSubGraph,
                         Set<Operator> subgraphs, 
                         Set<Operator> multiInSubgraphs,
                         Set<Operator> multiOutSubgraphs) {
        super(plan, ps, isSubGraph, subgraphs, 
              multiInSubgraphs, multiOutSubgraphs);
    }

    @Override
    protected DotPlanDumper makeDumper(LogicalPlan plan, PrintStream ps) {
        return new DotLOPrinter(plan, ps, true, mSubgraphs, 
                                mMultiInputSubgraphs,
                                mMultiOutputSubgraphs);
    }

    @Override
    protected String getName(LogicalOperator op) {
        String info = (op.name().split("\\d+-\\d+"))[0];
        if (op instanceof LOProject) {
            LOProject pr = (LOProject)op;
            info += pr.isStar()?" [*]": pr.getProjection();
        }
        return info;
    }

    @Override
    protected String[] getAttributes(LogicalOperator op) {
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
    protected MultiMap<LogicalOperator, LogicalPlan> 
        getMultiInputNestedPlans(LogicalOperator op) {
        
        if(op instanceof LOCogroup){
            return  ((LOCogroup)op).getGroupByPlans();
        }
        else if(op instanceof LOFRJoin){
            return ((LOFRJoin)op).getJoinColPlans();
        }
        return new MultiMap<LogicalOperator, LogicalPlan>();
    }

    @Override
    protected Collection<LogicalPlan> getNestedPlans(LogicalOperator op) {
        Collection<LogicalPlan> plans = new LinkedList<LogicalPlan>();

        if(op instanceof LOFilter){
            plans.add(((LOFilter)op).getComparisonPlan());
        }
        else if(op instanceof LOForEach){
            plans.addAll(((LOForEach)op).getForEachPlans());
        }
        else if(op instanceof LOGenerate){
            plans.addAll(((LOGenerate)op).getGeneratePlans());
        }
        else if(op instanceof LOSort){
            plans.addAll(((LOSort)op).getSortColPlans()); 
        }
        else if(op instanceof LOSplitOutput){
            plans.add(((LOSplitOutput)op).getConditionPlan());
        }
        
        return plans;
    }
}
