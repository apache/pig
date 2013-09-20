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

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The compiler that compiles a given physical plan into a DAG of Tez
 * operators which can then be converted into the JobControl structure.
 */
public class TezCompiler extends PhyPlanVisitor {

    public TezCompiler(PhysicalPlan plan,
            PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
        super(plan, walker);
        // TODO Auto-generated constructor stub
    }

    /**
     * The front-end method that the user calls to compile the plan. Assumes
     * that all submitted plans have a Store operators as the leaf.
     * @return A Tez plan
     * @throws IOException
     * @throws PlanException
     * @throws VisitorException
     */
    public TezOperPlan compile() throws IOException, PlanException, VisitorException {
        // TODO Auto-generated method stub
        return null;
    }
}
