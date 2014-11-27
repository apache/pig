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
package org.apache.pig.backend.hadoop.executionengine.tez.plan.operator;

import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezCompiler;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.Pair;

import com.google.common.collect.Lists;

public class POLocalRearrangeTezFactory {
    public static enum LocalRearrangeType {
        STAR,
        NULL,
        NORMAL,
        WITHPLAN
    };

    private String scope;
    private NodeIdGenerator nig;

    public POLocalRearrangeTezFactory(String scope, NodeIdGenerator nig) {
        this.scope = scope;
        this.nig = nig;
    }

    public POLocalRearrangeTez create() throws PlanException {
        return create(0, LocalRearrangeType.STAR, null, DataType.UNKNOWN);
    }

    public POLocalRearrangeTez create(LocalRearrangeType type) throws PlanException {
        return create(0, type, null, DataType.UNKNOWN);
    }

    public POLocalRearrangeTez create(int index, LocalRearrangeType type) throws PlanException {
        return create(index, type, null, DataType.UNKNOWN);
    }

    public POLocalRearrangeTez create(int index, LocalRearrangeType type, List<PhysicalPlan> plans,
            byte keyType) throws PlanException {
        ExpressionOperator keyExpression = null;

        if (type == LocalRearrangeType.STAR) {
            keyExpression = new POProject(new OperatorKey(scope, nig.getNextNodeId(scope)));
            keyExpression.setResultType(DataType.TUPLE);
            ((POProject)keyExpression).setStar(true);
        } else if (type == LocalRearrangeType.NULL) {
            keyExpression = new ConstantExpression(new OperatorKey(scope, nig.getNextNodeId(scope)));
            ((ConstantExpression)keyExpression).setValue(null);
            keyExpression.setResultType(DataType.BYTEARRAY);
        }

        PhysicalPlan addPlan = new PhysicalPlan();
        List<PhysicalPlan> addPlans = Lists.newArrayList();
        if (type == LocalRearrangeType.STAR || type == LocalRearrangeType.NULL) {
            addPlan.add(keyExpression);
            addPlans.add(addPlan);
        } else if (type == LocalRearrangeType.WITHPLAN) {
            addPlans.addAll(plans);
        }

        POLocalRearrangeTez lr = new POLocalRearrangeTez(new OperatorKey(scope, nig.getNextNodeId(scope)));
        try {
            lr.setIndex(index);
        } catch (ExecException e) {
            int errCode = 2058;
            String msg = "Unable to set index on the newly created POLocalRearrange.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
        if (type == LocalRearrangeType.STAR) {
            lr.setKeyType(DataType.TUPLE);
        } else if (type == LocalRearrangeType.NULL) {
            lr.setKeyType(DataType.BYTEARRAY);
        } else if (type == LocalRearrangeType.WITHPLAN) {
            Pair<POProject, Byte>[] fields = TezCompiler.getSortCols(plans);
            lr.setKeyType((fields == null || fields.length>1) ? DataType.TUPLE : keyType);
        }
        lr.setResultType(DataType.TUPLE);
        lr.setPlans(addPlans);
        return lr;
    }

}
