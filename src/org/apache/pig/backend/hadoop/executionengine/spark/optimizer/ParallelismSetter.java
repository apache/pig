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

package org.apache.pig.backend.hadoop.executionengine.spark.optimizer;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.NativeSparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.builtin.GFCross;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

public class ParallelismSetter extends SparkOpPlanVisitor {
    private JobConf jobConf;

    public ParallelismSetter(SparkOperPlan plan, JobConf jobConf) {
        super(plan, new DependencyOrderWalker<SparkOperator, SparkOperPlan>(plan));
        this.jobConf = jobConf;
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {
        if (sparkOp instanceof NativeSparkOperator) {
            return;
        }

        if (sparkOp.getCrossKeys() != null) {
            for (String key : sparkOp.getCrossKeys()) {
                jobConf.set(PigImplConstants.PIG_CROSS_PARALLELISM + "." + key,
                        // TODO: Estimate parallelism. For now we are hard-coding GFCross.DEFAULT_PARALLELISM
                        Integer.toString(96));
            }
        }
    }
}