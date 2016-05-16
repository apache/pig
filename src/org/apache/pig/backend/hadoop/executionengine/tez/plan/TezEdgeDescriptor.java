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
package org.apache.pig.backend.hadoop.executionengine.tez.plan;

import java.io.Serializable;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;

/**
 * Descriptor for Tez edge. It holds combine plan as well as edge properties.
 */
public class TezEdgeDescriptor implements Serializable {
    // Combiner runs on both input and output of Tez edge.
    transient public PhysicalPlan combinePlan;
    private boolean needsDistinctCombiner;

    public String inputClassName;
    public String outputClassName;

    public SchedulingType schedulingType;
    public DataSourceType dataSourceType;
    public DataMovementType dataMovementType;

    public Class<? extends Partitioner> partitionerClass;

    // If true, we will use secondary key sort in the job
    private boolean useSecondaryKey = false;

    // Sort order for secondary keys;
    private boolean[] secondarySortOrder;

    private String intermediateOutputKeyClass;
    private String intermediateOutputValueClass;
    private String intermediateOutputKeyComparatorClass;

    public TezEdgeDescriptor() {
        combinePlan = new PhysicalPlan();

        // The default is shuffle edge.
        inputClassName = OrderedGroupedKVInput.class.getName();
        outputClassName = OrderedPartitionedKVOutput.class.getName();
        partitionerClass = null;
        schedulingType = SchedulingType.SEQUENTIAL;
        dataSourceType = DataSourceType.PERSISTED;
        dataMovementType = DataMovementType.SCATTER_GATHER;
    }

    public boolean needsDistinctCombiner() {
        return needsDistinctCombiner;
    }

    public void setNeedsDistinctCombiner(boolean nic) {
        needsDistinctCombiner = nic;
    }

    public boolean isUseSecondaryKey() {
        return useSecondaryKey;
    }

    public void setUseSecondaryKey(boolean useSecondaryKey) {
        this.useSecondaryKey = useSecondaryKey;
    }

    public boolean[] getSecondarySortOrder() {
        return secondarySortOrder;
    }

    public void setSecondarySortOrder(boolean[] secondarySortOrder) {
        if(null == secondarySortOrder) return;
        this.secondarySortOrder = new boolean[secondarySortOrder.length];
        for(int i = 0; i < secondarySortOrder.length; ++i) {
            this.secondarySortOrder[i] = secondarySortOrder[i];
        }
    }

    public String getIntermediateOutputKeyClass() {
        return intermediateOutputKeyClass;
    }

    public void setIntermediateOutputKeyClass(String intermediateOutputKeyClass) {
        this.intermediateOutputKeyClass = intermediateOutputKeyClass;
    }

    public String getIntermediateOutputValueClass() {
        return intermediateOutputValueClass;
    }

    public void setIntermediateOutputValueClass(String intermediateOutputValueClass) {
        this.intermediateOutputValueClass = intermediateOutputValueClass;
    }

    public String getIntermediateOutputKeyComparatorClass() {
        return intermediateOutputKeyComparatorClass;
    }

    public void setIntermediateOutputKeyComparatorClass(
            String intermediateOutputKeyComparatorClass) {
        this.intermediateOutputKeyComparatorClass = intermediateOutputKeyComparatorClass;
    }

}
