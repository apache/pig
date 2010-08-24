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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Utils;

public class PONative extends PhysicalOperator {
    
    private static final long serialVersionUID = 1L;

    PhysicalPlan physInnerPlan;
    String nativeMRjar;
    String[] params;
    POStore innerStore;
    POLoad innerLoad;
    
    public PONative(OperatorKey k) {
        super(k);
    }

    public PhysicalPlan getPhysInnerPlan() {
        return physInnerPlan;
    }

    public void setPhysInnerPlan(PhysicalPlan physInnerPlan) {
        this.physInnerPlan = physInnerPlan;
        for(PhysicalOperator innerOp : physInnerPlan) {
            if(innerOp instanceof POStore) {
                innerStore = (POStore) innerOp;
            }
            if(innerOp instanceof POLoad) {
                innerLoad = (POLoad) innerOp;
            }
        }
    }
    
    public POStore getInnerStore() {
        return innerStore;
    }

    public POLoad getInnerLoad() {
        return innerLoad;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitNative(this);
    }

    @Override
    public String name() {
        return getAliasString() + "Native" + "('hadoop jar "
        + nativeMRjar + " " + Utils.getStringFromArray(params) + "')" + " - " + mKey.toString();
    }

    public String getNativeMRjar() {
        return nativeMRjar;
    }

    public void setNativeMRjar(String nativeMRjar) {
        this.nativeMRjar = nativeMRjar;
    }

    public String[] getParams() {
        return params;
    }

    public void setParams(String[] params) {
        this.params = params;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

}
