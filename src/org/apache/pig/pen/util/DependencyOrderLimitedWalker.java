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

package org.apache.pig.pen.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

//This modifies the dependencyOrderWalker by limiting the walking to the predecessors of a given operator
public class DependencyOrderLimitedWalker<O extends Operator, P extends OperatorPlan<O>>
        extends DependencyOrderWalker<O, P> {

    private O operator;

    public DependencyOrderLimitedWalker(O operator, P plan) {
        super(plan);
        this.operator = operator;
    }

    @Override
    public void walk(PlanVisitor<O, P> visitor) throws VisitorException {
        List<O> fifo = new ArrayList<O>();

        Set<O> seen = new HashSet<O>();
        doAllPredecessors(operator, seen, fifo);

        for (O op : fifo) {
            op.visit(visitor);
        }
    }

}
