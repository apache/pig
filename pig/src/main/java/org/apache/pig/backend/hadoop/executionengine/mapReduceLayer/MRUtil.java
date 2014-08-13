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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;

public class MRUtil {
    // simpleConnectMapToReduce is a utility to end a map phase and start a reduce phase in
    //     a mapreduce operator:
    // 1. mro only contains map plan
    // 2. need to add POLocalRearrange to end map plan, and add
    //    POPackage to start a reduce plan
    // 3. POLocalRearrange/POPackage are trivial
    static public void simpleConnectMapToReduce(MapReduceOper mro, String scope, NodeIdGenerator nig) throws PlanException
    {
        PhysicalPlan ep = new PhysicalPlan();
        POProject prjStar = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prjStar.setResultType(DataType.TUPLE);
        prjStar.setStar(true);
        ep.add(prjStar);

        List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
        eps.add(ep);

        POLocalRearrange lr = new POLocalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)));
        try {
            lr.setIndex(0);
        } catch (ExecException e) {
            int errCode = 2058;
            String msg = "Unable to set index on the newly created POLocalRearrange.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
        lr.setKeyType(DataType.TUPLE);
        lr.setPlans(eps);
        lr.setResultType(DataType.TUPLE);

        mro.mapPlan.addAsLeaf(lr);

        POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
        pkg.getPkgr().setKeyType(DataType.TUPLE);
        pkg.setNumInps(1);
        boolean[] inner = {false};
        pkg.getPkgr().setInner(inner);
        mro.reducePlan.add(pkg);

        mro.reducePlan.addAsLeaf(getPlainForEachOP(scope, nig));
    }

    // Get a simple POForEach: ForEach X generate flatten($1)
    static public POForEach getPlainForEachOP(String scope, NodeIdGenerator nig)
    {
        List<PhysicalPlan> eps1 = new ArrayList<PhysicalPlan>();
        List<Boolean> flat1 = new ArrayList<Boolean>();
        PhysicalPlan ep1 = new PhysicalPlan();
        POProject prj1 = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prj1.setResultType(DataType.TUPLE);
        prj1.setStar(false);
        prj1.setColumn(1);
        prj1.setOverloaded(true);
        ep1.add(prj1);
        eps1.add(ep1);
        flat1.add(true);
        POForEach fe = new POForEach(new OperatorKey(scope, nig
                .getNextNodeId(scope)), -1, eps1, flat1);
        fe.setResultType(DataType.BAG);
        return fe;
    }
}
