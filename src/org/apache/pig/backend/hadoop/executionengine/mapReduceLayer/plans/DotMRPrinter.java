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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans;

import java.io.PrintStream;
import java.util.List;
import java.util.LinkedList;
import java.util.Collection;
import org.apache.pig.impl.util.MultiMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.impl.plan.DotPlanDumper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.DotPOPrinter;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanException;

/**
 * This class can print an MR plan in the DOT format. It uses
 * clusters to illustrate nesting. If "verbose" is off, it will skip
 * any nesting in the associated physical plans.
 */
public class DotMRPrinter extends DotPlanDumper<MapReduceOper, MROperPlan, 
                                  DotMRPrinter.InnerOperator, 
                                  DotMRPrinter.InnerPlan> {

    static int counter = 0;
    boolean isVerboseNesting = true;

    public DotMRPrinter(MROperPlan plan, PrintStream ps) {
        this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
             new HashSet<Operator>());
    }

    private DotMRPrinter(MROperPlan plan, PrintStream ps, boolean isSubGraph,
                         Set<Operator> subgraphs, 
                         Set<Operator> multiInputSubgraphs,
                         Set<Operator> multiOutputSubgraphs) {
        super(plan, ps, isSubGraph, subgraphs, 
              multiInputSubgraphs, multiOutputSubgraphs);
    }

    @Override
    public void setVerbose(boolean verbose) {
        // leave the parents verbose set to true
        isVerboseNesting = verbose;
    }

    @Override
    protected DotPlanDumper makeDumper(InnerPlan plan, PrintStream ps) {
        return new InnerPrinter(plan, ps, mSubgraphs, mMultiInputSubgraphs, 
                                mMultiOutputSubgraphs);
    }

    @Override
    protected String getName(MapReduceOper op) {
        String name = "Map";
        if (!op.combinePlan.isEmpty()) {
            name += " - Combine";
        }
        if (!op.reducePlan.isEmpty()) {
            name += " - Reduce";
        }
        if (op.getRequestedParallelism()!=-1) {
            name += " Parallelism: "+op.getRequestedParallelism();
        }
        name += ", Global Sort: "+op.isGlobalSort();
        return name;
    }

    @Override
    protected Collection<InnerPlan> getNestedPlans(MapReduceOper op) {
        Collection<InnerPlan> plans = new LinkedList<InnerPlan>();
        plans.add(new InnerPlan(op.mapPlan, op.combinePlan, op.reducePlan));
        return plans;
    }
    
    @Override
    protected String[] getAttributes(MapReduceOper op) {
        String[] attributes = new String[3];
        attributes[0] = "label=\""+getName(op)+"\"";
        attributes[1] = "style=\"filled\"";
        attributes[2] = "fillcolor=\"#EEEEEE\"";
        return attributes;
    }


    /**
     * Helper class to represent the relationship of map, reduce and
     * combine phases in an MR operator.
     */
    public static class InnerOperator extends Operator<PlanVisitor> {

        private static final long serialVersionUID = 1L;
        String name;
        PhysicalPlan plan;
        int code;
        
        public InnerOperator(PhysicalPlan plan, String name) {
            super(new OperatorKey());
            this.name = name;
            this.plan = plan;
            this.code = counter++;
        }
        
        @Override public void visit(PlanVisitor v) {}
        @Override public boolean supportsMultipleInputs() {return false;}
        @Override public boolean supportsMultipleOutputs() {return false;}
        @Override public String name() {return name;}
        public PhysicalPlan getPlan() {return plan;}
        @Override public int hashCode() {return code;}
    }

    /**
     * Helper class to represent the relationship of map, reduce and
     * combine phases in an MR operator. Each MR operator will have
     * an inner plan of map -> (combine)? -> (reduce)? inner
     * operators. The inner operators contain the physical plan of the
     * execution phase.
     */
    public static class InnerPlan extends OperatorPlan<InnerOperator> {

        private static final long serialVersionUID = 1L;

        public InnerPlan(PhysicalPlan mapPlan, PhysicalPlan combinePlan, 
                         PhysicalPlan reducePlan) {
            try {
                InnerOperator map = new InnerOperator(mapPlan, "Map");
                
                this.add(map);
                if (!combinePlan.isEmpty()) {
                    InnerOperator combine = 
                        new InnerOperator(combinePlan, "Combine");
                    InnerOperator reduce = 
                        new InnerOperator(reducePlan, "Reduce");
                    this.add(combine);
                    this.connect(map, combine);
                    this.add(reduce);
                    this.connect(combine, reduce);
                } 
                else if (!reducePlan.isEmpty()){
                    InnerOperator reduce = 
                        new InnerOperator(reducePlan, "Reduce");
                    this.add(reduce);
                    this.connect(map, reduce);
                }
            } catch (PlanException e) {}
        }
    }

    /**
     * Helper class to represent the relationship of map, reduce and
     * combine phases in an MR operator.
     */    
    private class InnerPrinter extends DotPlanDumper<InnerOperator, InnerPlan,
                                       PhysicalOperator, PhysicalPlan> {

        public InnerPrinter(InnerPlan plan, PrintStream ps,
                            Set<Operator> subgraphs, 
                            Set<Operator> multiInputSubgraphs,
                            Set<Operator> multiOutputSubgraphs) {
            super(plan, ps, true, subgraphs, multiInputSubgraphs,
                  multiOutputSubgraphs);
        }

        @Override
        protected String[] getAttributes(InnerOperator op) {
            String[] attributes = new String[3];
            attributes[0] = "label=\""+super.getName(op)+"\"";
            attributes[1] = "style=\"filled\"";
            attributes[2] = "fillcolor=\"white\"";
            return attributes;
        }

        @Override
        protected Collection<PhysicalPlan> getNestedPlans(InnerOperator op) {
            Collection<PhysicalPlan> l = new LinkedList<PhysicalPlan>();
            l.add(op.getPlan());
            return l;
        }

        @Override
        protected DotPOPrinter makeDumper(PhysicalPlan plan, PrintStream ps) {
            DotPOPrinter printer = new DotPOPrinter(plan, ps, true, 
                                                    mSubgraphs, 
                                                    mMultiInputSubgraphs,
                                                    mMultiOutputSubgraphs);
            printer.setVerbose(isVerboseNesting);
            return printer;
        }
    }
}
