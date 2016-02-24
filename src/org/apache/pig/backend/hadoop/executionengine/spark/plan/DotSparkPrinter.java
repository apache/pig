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
package org.apache.pig.backend.hadoop.executionengine.spark.plan;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.impl.plan.DotPlanDumper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.DotPOPrinter;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;

/**
 * This class can print Spark plan in the DOT format. It uses
 * clusters to illustrate nesting. If "verbose" is off, it will skip
 * any nesting in the associated physical plans.
 */
public class DotSparkPrinter extends DotPlanDumper<SparkOperator, SparkOperPlan,
        DotSparkPrinter.InnerOperator,
        DotSparkPrinter.InnerPlan> {

    static int counter = 0;
    boolean isVerboseNesting = true;

    public DotSparkPrinter(SparkOperPlan plan, PrintStream ps) {
        this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
                new HashSet<Operator>());
    }

    private DotSparkPrinter(SparkOperPlan plan, PrintStream ps, boolean isSubGraph,
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
    protected String getName(SparkOperator op) {
        String name = op.name();
        // Cut of the part of the name specifying scope.
        String delimiter = " - ";
        String[] temp;
        temp = name.split(delimiter);
        return temp[0];
    }

    @Override
    protected Collection<InnerPlan> getNestedPlans(SparkOperator op) {
        Collection<InnerPlan> plans = new LinkedList<InnerPlan>();
        plans.add(new InnerPlan(op.physicalPlan));
        return plans;
    }

    @Override
    protected String[] getAttributes(SparkOperator op) {
        String[] attributes = new String[3];
        attributes[0] = "label=\""+getName(op)+"\"";
        attributes[1] = "style=\"filled\"";
        attributes[2] = "fillcolor=\"#EEEEEE\"";
        return attributes;
    }


    /**
     * Helper class to represent the relationship of inner operators
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
     * Each spark operator will have and an inner plan of inner
     * operators. The inner operators contain the physical plan of the
     * execution phase.
     */
    public static class InnerPlan extends OperatorPlan<InnerOperator> {

        private static final long serialVersionUID = 1L;

        public InnerPlan(PhysicalPlan plan) {
            InnerOperator sparkInnerOp = new InnerOperator(plan, "spark");
            this.add(sparkInnerOp);
        }
    }

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
