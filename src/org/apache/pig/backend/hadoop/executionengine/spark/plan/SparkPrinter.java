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
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.NativeSparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class SparkPrinter extends SparkOpPlanVisitor {

    private PrintStream mStream = null;
    private boolean isVerbose = true;

    public SparkPrinter(PrintStream ps, SparkOperPlan plan) {
        super(plan, new DepthFirstWalker<SparkOperator, SparkOperPlan>(plan));
        mStream = ps;
        mStream.println("#--------------------------------------------------");
        mStream.println("# Spark Plan");
        mStream.println("#--------------------------------------------------");
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {
        mStream.println("");
        mStream.println("Spark node " + sparkOp.getOperatorKey().toString());
        if (sparkOp instanceof NativeSparkOperator) {
            mStream.println(((NativeSparkOperator)sparkOp).getCommandString());
            mStream.println("--------");
            mStream.println();
            return;
        }
        if (sparkOp.physicalPlan != null && sparkOp.physicalPlan.size() > 0) {
            PlanPrinter<PhysicalOperator, PhysicalPlan> printer = new PlanPrinter<PhysicalOperator, PhysicalPlan>(
                    sparkOp.physicalPlan, mStream);
            printer.setVerbose(isVerbose);
            printer.visit();
            mStream.println("--------");
        }
        List<POGlobalRearrangeSpark> glrList = PlanHelper.getPhysicalOperators(sparkOp.physicalPlan, POGlobalRearrangeSpark.class);
        for (POGlobalRearrangeSpark glr : glrList) {
            if (glr.isUseSecondaryKey()) {
                mStream.println("POGlobalRearrange(" + glr.getOperatorKey() + ") uses secondaryKey");
            }
        }
    }
}
