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
package org.apache.pig.backend.hadoop.executionengine.tez;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

/**
 * Descriptor for Tez edge. It holds combine plan as well as edge properties.
 */
public class TezEdgeDescriptor {
    // Combiner runs on both input and output of Tez edge.
    public PhysicalPlan combinePlan;

    public String inputClassName;
    public String outputClassName;
    public SchedulingType schedulingType;
    public DataSourceType dataSourceType;
    public DataMovementType dataMovementType;

    public TezEdgeDescriptor() {
        combinePlan = new PhysicalPlan();

        // The default is shuffle edge.
        inputClassName = ShuffledMergedInput.class.getName();
        outputClassName = OnFileSortedOutput.class.getName();
        schedulingType = SchedulingType.SEQUENTIAL;
        dataSourceType = DataSourceType.PERSISTED;
        dataMovementType = DataMovementType.SCATTER_GATHER;
    }
}
