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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.PrintStream;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class MRPrinter extends MROpPlanVisitor {

    private PrintStream mStream = null;
    private boolean isVerbose = true;

    /**
     * @param ps PrintStream to output plan information to
     * @param plan MR plan to print
     */
    public MRPrinter(PrintStream ps, MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        mStream = ps;
        mStream.println("#--------------------------------------------------");
        mStream.println("# Map Reduce Plan                                  ");
        mStream.println("#--------------------------------------------------");
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        mStream.println("MapReduce node " + mr.getOperatorKey().toString());
        if(mr instanceof NativeMapReduceOper) {
            mStream.println(((NativeMapReduceOper)mr).getCommandString());
            mStream.println("--------");
            mStream.println();
            return;
        }
        if (mr.mapPlan != null && mr.mapPlan.size() > 0) {
            mStream.println("Map Plan");
            PlanPrinter<PhysicalOperator, PhysicalPlan> printer = new PlanPrinter<PhysicalOperator, PhysicalPlan>(mr.mapPlan, mStream);
            printer.setVerbose(isVerbose);
            printer.visit();
            mStream.println("--------");
        }
        if (mr.combinePlan != null && mr.combinePlan.size() > 0) {
            mStream.println("Combine Plan");
            PlanPrinter<PhysicalOperator, PhysicalPlan> printer = new PlanPrinter<PhysicalOperator, PhysicalPlan>(mr.combinePlan, mStream);
            printer.setVerbose(isVerbose);
            printer.visit();
            mStream.println("--------");
        }
        if (mr.reducePlan != null && mr.reducePlan.size() > 0) {
            mStream.println("Reduce Plan");
            PlanPrinter<PhysicalOperator, PhysicalPlan> printer = new PlanPrinter<PhysicalOperator, PhysicalPlan>(mr.reducePlan, mStream);
            printer.setVerbose(isVerbose);
            printer.visit();
            mStream.println("--------");
        }
        mStream.println("Global sort: " + mr.isGlobalSort());
        if (mr.getQuantFile() != null) {
            mStream.println("Quantile file: " + mr.getQuantFile());
        }
        if (mr.getUseSecondaryKey())
            mStream.println("Secondary sort: " + mr.getUseSecondaryKey());
        mStream.println("----------------");
        mStream.println("");
    }
}

