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
package org.apache.pig.backend.hadoop.executionengine.spark.operator;

import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Collapse POLocalRearrange,POGlobalRearrange and POPackage to POJoinGroupSpark to reduce unnecessary map operations in the join/group
 */
public class POJoinGroupSpark extends PhysicalOperator {
    private List<POLocalRearrange> lraOps;
    private POGlobalRearrangeSpark glaOp;
    private POPackage pkgOp;
    private List<PhysicalOperator> predecessors;

    public POJoinGroupSpark(List<POLocalRearrange> lraOps, POGlobalRearrangeSpark glaOp, POPackage pkgOp){
        super(glaOp.getOperatorKey());
        this.lraOps = lraOps;
        this.glaOp = glaOp;
        this.pkgOp = pkgOp;
    }

    public List<POLocalRearrange> getLROps() {
        return lraOps;
    }

    public POGlobalRearrangeSpark getGROp() {
        return glaOp;
    }

    public POPackage getPkgOp() {
        return pkgOp;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {

    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public String name() {
        return getAliasString() + "POJoinGroupSpark"+ "["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return null;
    }

    public void setPredecessors(List<PhysicalOperator> predecessors) {
        this.predecessors = predecessors;
    }

    public List<PhysicalOperator> getPredecessors() {
        return predecessors;
    }
}
