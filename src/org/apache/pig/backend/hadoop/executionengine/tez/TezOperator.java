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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.Vertex;

/**
 * An operator model for a Tez job. Acts as a host to the plans that will
 * execute in Tez vertices.
 */
public abstract class TezOperator extends Operator<TezOpPlanVisitor> {

    private static final long serialVersionUID = 1L;
    private PhysicalPlan plan;

    public TezOperator(OperatorKey k) {
        super(k);
        // TODO Auto-generated constructor stub
    }

    public abstract String getProcessorName();

    public abstract void configureVertex(Vertex operVertex, Configuration operConf,
            Map<String, LocalResource> commonLocalResources, Path remoteStagingDir);

    public void visit(TezOpPlanVisitor v) throws VisitorException {
        v.visitTezOp(this);
    }

    public boolean supportsMultipleInputs() {
        return true;
    }

    public boolean supportsMultipleOutputs() {
        return true;
    }

    public PhysicalPlan getPlan() {
        return plan;
    }

    public String name() {
        return "Tez - " + mKey.toString();
    }
}

