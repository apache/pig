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
package org.apache.pig.backend.hadoop.executionengine.tez.util;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezEdgeDescriptor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.UDFContextSeparator;
import org.apache.pig.impl.util.UDFContextSeparator.UDFType;

public class TezUDFContextSeparator extends TezOpPlanVisitor{

    private UDFContextSeparator udfContextSeparator;

    public TezUDFContextSeparator(TezOperPlan plan,
            PlanWalker<TezOperator, TezOperPlan> walker) {
        super(plan, walker);
        udfContextSeparator = new UDFContextSeparator();
    }

    @Override
    public void visitTezOp(TezOperator tezOperator) throws VisitorException {
        if (!tezOperator.isVertexGroup()) {
            udfContextSeparator.setPlan(tezOperator.plan, tezOperator.getOperatorKey().toString());
            udfContextSeparator.visit();

            for (Entry<OperatorKey, TezEdgeDescriptor> entry : tezOperator.outEdges.entrySet()) {
                PhysicalPlan combinePlan = entry.getValue().combinePlan;
                if (!combinePlan.isEmpty()) {
                    udfContextSeparator.setPlan(combinePlan,
                            tezOperator.getOperatorKey().toString() + "-" + entry.getKey().toString());
                    udfContextSeparator.visit();
                }
            }
        }
    }

    public void serializeUDFContext(Configuration conf, TezOperator tezOp) throws IOException {
        // Serialize all - LoadFunc, StoreFunc, UserFunc
        udfContextSeparator.serializeUDFContext(conf, tezOp.getOperatorKey().toString(), UDFType.values());
    }

    public void serializeUDFContext(Configuration conf, TezOperator tezOp,
            UDFType udfType) throws IOException {
        udfContextSeparator.serializeUDFContext(conf, tezOp.getOperatorKey().toString(), udfType);
    }

    public void serializeUDFContextForEdge(Configuration conf,
            TezOperator from, TezOperator to, UDFType udfType) throws IOException {
        udfContextSeparator.serializeUDFContext(conf,
                from.getOperatorKey().toString() + "-" + to.getOperatorKey().toString(), udfType);
    }

    public void serializeUDFContext(Configuration conf, TezOperator tezOp,
            POStore store) throws IOException {
        udfContextSeparator.serializeUDFContext(conf, tezOp.getOperatorKey().toString(), store);
    }

}
