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
package org.apache.pig.backend.hadoop.executionengine.tez.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezEdgeDescriptor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POLocalRearrangeTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POStoreTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POValueOutputTez;
import org.apache.pig.builtin.RoundRobinPartitioner;
import org.apache.pig.data.DataType;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedPartitionedKVOutput;

import com.google.common.collect.Lists;

public class TezCompilerUtil {

    public static String TUPLE_CLASS = TupleFactory.getInstance().tupleClass().getName();

    private TezCompilerUtil() {
    }

    // simpleConnectTwoVertex is a utility to end a vertex equivalent to map and start vertex equivalent to
    // reduce in a tez operator:
    // 1. op1 is open
    // 2. op2 is blank
    //    POPackage to start a reduce vertex
    // 3. POLocalRearrange/POPackage are trivial
    // 4. User need to connect op1 to op2 themselves
    static public void simpleConnectTwoVertex(TezOperPlan tezPlan, TezOperator op1, TezOperator op2, String scope, NodeIdGenerator nig) throws PlanException
    {
        PhysicalPlan ep = new PhysicalPlan();
        POProject prjStar = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prjStar.setResultType(DataType.TUPLE);
        prjStar.setStar(true);
        ep.add(prjStar);

        List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
        eps.add(ep);

        POLocalRearrangeTez lr = new POLocalRearrangeTez(new OperatorKey(scope,nig.getNextNodeId(scope)));
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
        lr.setOutputKey(op2.getOperatorKey().toString());

        op1.plan.addAsLeaf(lr);

        POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
        pkg.getPkgr().setKeyType(DataType.TUPLE);
        pkg.setNumInps(1);
        boolean[] inner = {false};
        pkg.getPkgr().setInner(inner);
        op2.plan.add(pkg);

        op2.plan.addAsLeaf(getForEachPlain(scope, nig));

        connect(tezPlan, op1, op2);
    }

    static public TezEdgeDescriptor connect(TezOperPlan plan, TezOperator from, TezOperator to) throws PlanException {
        plan.connect(from, to);
        if (!from.plan.isEmpty()) {
            PhysicalOperator leaf = from.plan.getLeaves().get(0);
            // It could be POStoreTez incase of sampling job in order by
            if (leaf instanceof POLocalRearrangeTez) {
                POLocalRearrangeTez lr = (POLocalRearrangeTez) leaf;
                lr.setOutputKey(to.getOperatorKey().toString());
            }
        }
        // Add edge descriptors to old and new operators
        TezEdgeDescriptor edge = new TezEdgeDescriptor();
        to.inEdges.put(from.getOperatorKey(), edge);
        from.outEdges.put(to.getOperatorKey(), edge);
        return edge;
    }

    static public void connect(TezOperPlan plan, TezOperator from, TezOperator to, TezEdgeDescriptor edge) throws PlanException {
        plan.connect(from, to);
        if (from.plan.getLeaves()!=null && !from.plan.getLeaves().isEmpty()) {
            PhysicalOperator leaf = from.plan.getLeaves().get(0);
            // It could be POStoreTez incase of sampling job in order by
            if (leaf instanceof POLocalRearrangeTez) {
                POLocalRearrangeTez lr = (POLocalRearrangeTez) leaf;
                lr.setOutputKey(to.getOperatorKey().toString());
            }
        }
        // Add edge descriptors to old and new operators
        to.inEdges.put(from.getOperatorKey(), edge);
        from.outEdges.put(to.getOperatorKey(), edge);
    }

    static public POForEach getForEach(POProject project, int rp, String scope, NodeIdGenerator nig) {
        PhysicalPlan forEachPlan = new PhysicalPlan();
        forEachPlan.add(project);

        List<PhysicalPlan> forEachPlans = Lists.newArrayList();
        forEachPlans.add(forEachPlan);

        List<Boolean> flatten = Lists.newArrayList();
        flatten.add(true);

        POForEach forEach = new POForEach(new OperatorKey(scope, nig.getNextNodeId(scope)), rp, forEachPlans, flatten);
        forEach.setResultType(DataType.BAG);
        return forEach;
    }

    // Get a plain POForEach: ForEach X generate flatten($1)
    static public POForEach getForEachPlain(String scope, NodeIdGenerator nig) {
        POProject project = new POProject(new OperatorKey(scope, nig.getNextNodeId(scope)));
        project.setResultType(DataType.TUPLE);
        project.setStar(false);
        project.setColumn(1);
        project.setOverloaded(true);
        return getForEach(project, -1, scope, nig);
    }

    static public POStore getStore(String scope, NodeIdGenerator nig) {
        POStore st = new POStoreTez(new OperatorKey(scope, nig.getNextNodeId(scope)));
        // mark store as tmp store. These could be removed by the
        // optimizer, because it wasn't the user requesting it.
        st.setIsTmpStore(true);
        return st;
    }

    static public void setCustomPartitioner(String customPartitioner, TezOperator tezOp) throws IOException {
        if (customPartitioner != null) {
            for (TezEdgeDescriptor edge : tezOp.inEdges.values()) {
                edge.partitionerClass = PigContext.resolveClassName(customPartitioner);
            }
        }
    }

    // Used with POValueOutputTez
    static public void configureValueOnlyTupleOutput(TezEdgeDescriptor edge, DataMovementType dataMovementType) {
        edge.dataMovementType = dataMovementType;
        if (dataMovementType == DataMovementType.BROADCAST || dataMovementType == DataMovementType.ONE_TO_ONE) {
            edge.outputClassName = UnorderedKVOutput.class.getName();
            edge.inputClassName = UnorderedKVInput.class.getName();
        } else if (dataMovementType == DataMovementType.SCATTER_GATHER) {
            edge.outputClassName = UnorderedPartitionedKVOutput.class.getName();
            edge.inputClassName = UnorderedKVInput.class.getName();
            edge.partitionerClass = RoundRobinPartitioner.class;
        }
        edge.setIntermediateOutputKeyClass(POValueOutputTez.EmptyWritable.class.getName());
        edge.setIntermediateOutputValueClass(TUPLE_CLASS);
    }

    /**
     * Returns true if there are no loads or stores in a TezOperator.
     * To be called only after LoaderProcessor is called
     */
    static public boolean isIntermediateReducer(TezOperator tezOper) throws VisitorException {
        boolean intermediateReducer = false;
        LinkedList<POStore> stores = tezOper.getStores();
        // Not map and not final reducer
        if (stores.size() <= 0 &&
                (tezOper.getLoaderInfo().getLoads() == null || tezOper.getLoaderInfo().getLoads().size() <= 0)) {
            intermediateReducer = true;
        }
        return intermediateReducer;
    }

}
