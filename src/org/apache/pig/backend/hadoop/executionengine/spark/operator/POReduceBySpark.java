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

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;

/**
 * ReduceBy operator that maps to Sparks ReduceBy.
 * Extends ForEach and adds packager, secondary sort and partitioner support.
 */
public class POReduceBySpark extends POForEach {
    private String customPartitioner;
    protected POLocalRearrange lr;
    protected POPackage pkg;

    public POReduceBySpark(OperatorKey k, int rp, List<PhysicalPlan> inp, List<Boolean> isToBeFlattened, POPackage
            pkg, POLocalRearrange lr){
        super(k, rp, inp, isToBeFlattened);
        this.pkg = pkg;
        this.lr = lr;
        this.addOriginalLocation(lr.getAlias(), lr.getOriginalLocations());
    }

    public POPackage getPkg() {
        return pkg;
    }

    @Override
    public String name() {
        return getAliasString() + "Reduce By" + "(" + getFlatStr() + ")" + "["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }

    protected String getFlatStr() {
        if(isToBeFlattenedArray ==null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Boolean b : isToBeFlattenedArray) {
            sb.append(b);
            sb.append(',');
        }
        if(sb.length()>0){
            sb.deleteCharAt(sb.length()-1);
        }
        return sb.toString();
    }

    // Use secondary key
    private boolean useSecondaryKey;
    // Sort order for secondary keys;
    private boolean[] secondarySortOrder;

    public boolean isUseSecondaryKey() {
        return useSecondaryKey;
    }

    public void setUseSecondaryKey(boolean useSecondaryKey) {
        this.useSecondaryKey = useSecondaryKey;
    }

    public boolean[] getSecondarySortOrder() {
        return secondarySortOrder;
    }

    public void setSecondarySortOrder(boolean[] secondarySortOrder) {
        this.secondarySortOrder = secondarySortOrder;
    }

    public String getCustomPartitioner() {
        return customPartitioner;
    }

    public void setCustomPartitioner(String customPartitioner) {
        this.customPartitioner = customPartitioner;
    }

    public POLocalRearrange getLgr() {
        return lr;
    }

}
