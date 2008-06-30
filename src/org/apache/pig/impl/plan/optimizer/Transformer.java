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
package org.apache.pig.impl.plan.optimizer;

import java.util.List;

import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;

/**
 * Transformer represents one tranform that the optimizer can
 * apply to a graph.  This class is a special case of a PlanVisitor,so it
 * can navigate the plan.
 */
public abstract class Transformer<O extends Operator, P extends OperatorPlan<O>> extends PlanVisitor<O, P> {

    /**
     * @param plan OperatorPlan to be optimized.
     */
    protected Transformer(P plan, PlanWalker<O, P> walker) {
        super(plan, walker);
    }

    /**
     * check if the transform should be done.  If this is being called then
     * the pattern matches, but there may be other criteria that must be met
     * as well.
     * @param nodes - List of nodes declared in transform ($1 = nodes[0],
     * etc.)  Remember that somes entries in node[] may be NULL since they may
     * not be created until after the transform.
     * @return true if the transform should be done.
     * @throws OptimizerException
     */
    public abstract boolean check(List<O> nodes) throws OptimizerException;

    /**
     * Transform the tree
     * @param nodes - List of nodes declared in transform ($1 = nodes[0],
     * etc.)  This call must destruct any nodes that are being removed as part
     * of the transform and remove them from the nodes vector and construct
     * any that are being created as part of the transform and add them at the
     * appropriate point to the nodes vector.
     * @throws OptimizerException
     */
    public abstract void transform(List<O> nodes) throws OptimizerException;

}

