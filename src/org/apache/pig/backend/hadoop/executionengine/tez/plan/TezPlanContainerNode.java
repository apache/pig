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

import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class TezPlanContainerNode extends Operator<TezPlanContainerVisitor>{
    private static final long serialVersionUID = 1L;
    TezOperPlan node;
    public TezPlanContainerNode(OperatorKey k, TezOperPlan node) {
        super(k);
        this.node = node;
    }

    @Override
    public void visit(TezPlanContainerVisitor v) throws VisitorException {
        v.visitTezPlanContainerNode(this);
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    @Override
    public String name() {
        return "DAG " + mKey;
    }

    public TezOperPlan getNode() {
        return node;
    }

    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof TezPlanContainerNode) {
            return ((TezPlanContainerNode)o).getNode().equals(getNode());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getNode().hashCode();
    }

    @Override
    public String toString() {
        return getNode().toString();
    }
}

