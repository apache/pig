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
package org.apache.pig.spark;

import org.apache.pig.backend.hadoop.executionengine.optimizer.SecondaryKeyOptimizer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.SecondaryKeyOptimizerSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkCompiler;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.test.MiniGenericCluster;
import org.apache.pig.test.TestSecondarySort;
import org.apache.pig.test.Util;

/**
 * TestSecondarySortSpark.
 */
public class TestSecondarySortSpark extends TestSecondarySort {

    public TestSecondarySortSpark() {
        super();
    }

    @Override
    public MiniGenericCluster getCluster() {
        return MiniGenericCluster.buildCluster(MiniGenericCluster.EXECTYPE_SPARK);
    }

    @Override
    public SecondaryKeyOptimizer visitSecondaryKeyOptimizer(String query) throws Exception {
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        SparkCompiler comp = new SparkCompiler(pp, pc);
        comp.compile();
        SparkOperPlan sparkPlan = comp.getSparkPlan();
        SecondaryKeyOptimizerSpark optimizer = new SecondaryKeyOptimizerSpark(sparkPlan);
        optimizer.visit();
        return optimizer;
    }
}
