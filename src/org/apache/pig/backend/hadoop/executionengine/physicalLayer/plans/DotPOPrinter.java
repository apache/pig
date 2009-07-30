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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.io.PrintStream;
import java.util.List;
import java.util.LinkedList;
import java.util.Collection;
import org.apache.pig.impl.util.MultiMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.impl.plan.DotPlanDumper;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.*;

/**
 * This class can print a physical plan in the DOT format. It uses
 * clusters to illustrate nesting. If "verbose" is off, it will skip
 * any nesting.
 */
public class DotPOPrinter extends DotPlanDumper<PhysicalOperator, PhysicalPlan, 
                                  PhysicalOperator, PhysicalPlan> {

    public DotPOPrinter(PhysicalPlan plan, PrintStream ps) {
        this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
             new HashSet<Operator>());
    }

    public DotPOPrinter(PhysicalPlan plan, PrintStream ps, boolean isSubGraph,
                        Set<Operator> subgraphs, 
                        Set<Operator> multiInputSubgraphs,
                        Set<Operator> multiOutputSubgraphs) {
        super(plan, ps, isSubGraph, subgraphs, multiInputSubgraphs,
              multiOutputSubgraphs);
    }

    @Override
    protected DotPlanDumper makeDumper(PhysicalPlan plan, PrintStream ps) {
        DotPOPrinter dumper = new DotPOPrinter(plan, ps, true, mSubgraphs, 
                                               mMultiInputSubgraphs,
                                               mMultiOutputSubgraphs);
        dumper.setVerbose(this.isVerbose());
        return dumper;
    }

    @Override
    protected String getName(PhysicalOperator op) {
        return (op.name().split(" - "))[0];
    }


    @Override
    protected String[] getAttributes(PhysicalOperator op) {
        if (op instanceof POStore || op instanceof POLoad) {
            String[] attributes = new String[3];
            String name = getName(op);
            int idx = name.lastIndexOf(":");
            if (idx != -1) {
                String part1 = name.substring(0,idx);
                String part2 = name.substring(idx+1,name.length());
                name = part1+",\\n"+part2;
            }
            attributes[0] = "label=\""+name+"\"";
            attributes[1] = "style=\"filled\"";
            attributes[2] = "fillcolor=\"gray\"";
            return attributes;
        }
        else {
            return super.getAttributes(op);
        }
    }

    @Override
    protected Collection<PhysicalPlan> getMultiOutputNestedPlans(PhysicalOperator op) {
        Collection<PhysicalPlan> plans = new LinkedList<PhysicalPlan>();
        
        if (op instanceof POSplit) {
            plans.addAll(((POSplit)op).getPlans());
        }
        else if(op instanceof PODemux) {
            Set<PhysicalPlan> pl = new HashSet<PhysicalPlan>();
            pl.addAll(((PODemux)op).getPlans());
            plans.addAll(pl);
        }
        
        return plans;
    }

    @Override
    protected Collection<PhysicalPlan> getNestedPlans(PhysicalOperator op) {
        Collection<PhysicalPlan> plans = new LinkedList<PhysicalPlan>();

        if(op instanceof POFilter){
            plans.add(((POFilter)op).getPlan());
        }
        else if(op instanceof POForEach){
            plans.addAll(((POForEach)op).getInputPlans());
        }
        else if(op instanceof POSort){
            plans.addAll(((POSort)op).getSortPlans()); 
        }
        else if(op instanceof POLocalRearrange){
            plans.addAll(((POLocalRearrange)op).getPlans());
        }
        else if(op instanceof POFRJoin) {
            POFRJoin frj = (POFRJoin)op;
            List<List<PhysicalPlan>> joinPlans = frj.getJoinPlans();
            if(joinPlans!=null) {
                for (List<PhysicalPlan> list : joinPlans) {
                    plans.addAll(list);
                }
            }
        }
        else if(op instanceof POSkewedJoin) {
        	POSkewedJoin skewed = (POSkewedJoin)op;
            Collection<PhysicalPlan> joinPlans = skewed.getJoinPlans().values();
            if(joinPlans!=null) {
                plans.addAll(joinPlans);
            }
        }

        return plans;
    }
}
