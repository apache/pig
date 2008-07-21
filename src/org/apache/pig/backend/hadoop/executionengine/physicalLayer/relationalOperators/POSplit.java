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

import java.util.List;

import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The MapReduce Split operator.
 * The assumption here is that
 * the logical to physical translation
 * will create this dummy operator with
 * just the filename using which the input
 * branch will be stored and used for loading
 * Also the translation should make sure that
 * appropriate filter operators are configured
 * as outputs of this operator using the conditions
 * specified in the LOSplit. So LOSplit will be converted
 * into:
 * 
 *     |        |           |
 *  Filter1  Filter2 ... Filter3
 *     |        |    ...    |
 *     |        |    ...    |
 *     ---- POSplit -... ----
 * This is different than the existing implementation
 * where the POSplit writes to sidefiles after filtering
 * and then loads the appropirate file.
 * 
 * The approach followed here is as good as the old
 * approach if not better in many cases because
 * of the availablity of attachinInputs. An optimization
 * that can ensue is if there are multiple loads that
 * load the same file, they can be merged into one and 
 * then the operators that take input from the load 
 * can be stored. This can be used when
 * the mapPlan executes to read the file only once and
 * attach the resulting tuple as inputs to all the 
 * operators that take input from this load.
 * 
 * In some cases where the conditions are exclusive and
 * some outputs are ignored, this approach can be worse.
 * But this leads to easier management of the Split and
 * also allows to reuse this data stored from the split
 * job whenever necessary.
 */
public class POSplit extends PhysicalOperator {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    //The filespec that is used to store
    //and load the output of the split job
    //which is the job containing the split
    FileSpec splitStore;

    public POSplit(OperatorKey k) {
        this(k,-1,null);
    }

    public POSplit(OperatorKey k, int rp) {
        this(k,rp,null);
    }

    public POSplit(OperatorKey k, List<PhysicalOperator> inp) {
        this(k,-1,null);
    }

    public POSplit(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitSplit(this);
    }

    @Override
    public String name() {
        return "Split - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    public FileSpec getSplitStore() {
        return splitStore;
    }

    public void setSplitStore(FileSpec splitStore) {
        this.splitStore = splitStore;
    }

}
