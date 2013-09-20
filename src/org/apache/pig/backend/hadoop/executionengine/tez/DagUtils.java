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

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.engine.lib.input.ShuffledMergedInput;
import org.apache.tez.engine.lib.output.OnFileSortedOutput;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;

public class DagUtils {

    /**
     * Given two vertices and their respective configuration objects createEdge
     * will create an Edge object that connects the two. Currently the edge will
     * always be a stable bi-partite edge.
     *
     * @param vConf JobConf of the first vertex
     * @param v The first vertex (source)
     * @param wConf JobConf of the second vertex
     * @param w The second vertex (sink)
     * @return
     */
    public static Edge createEdge(JobConf vConf, Vertex v, JobConf wConf, Vertex w) throws IOException {

        // Tez needs to setup output subsequent input pairs correctly
        MultiStageMRConfToTezTranslator.translateVertexConfToTez(wConf, vConf);

        // update payloads (configuration for the vertices might have changed)
        v.getProcessorDescriptor().setUserPayload(MRHelpers.createUserPayloadFromConf(vConf));
        w.getProcessorDescriptor().setUserPayload(MRHelpers.createUserPayloadFromConf(wConf));

        // all edges are of the same type right now
        EdgeProperty edgeProperty =
                new EdgeProperty(
                        DataMovementType.SCATTER_GATHER,
                        DataSourceType.PERSISTED,
                        SchedulingType.SEQUENTIAL,
                        new OutputDescriptor(OnFileSortedOutput.class.getName()),
                        new InputDescriptor(ShuffledMergedInput.class.getName()));
        return new Edge(v, w, edgeProperty);
    }
}
