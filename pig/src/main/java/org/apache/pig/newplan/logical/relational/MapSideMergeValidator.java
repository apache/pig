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
package org.apache.pig.newplan.logical.relational;

import java.util.List;

import org.apache.pig.SortColInfo;
import org.apache.pig.SortColInfo.Order;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.visitor.UDFFinder;

public class MapSideMergeValidator {

    public boolean validateMapSideMerge(List<Operator> preds, OperatorPlan lp)
            throws LogicalToPhysicalTranslatorException{
        int errCode = 1103;
        String errMsg = "Merge join/Cogroup only supports Filter, Foreach, " +
                "Ascending Sort, or Load as its predecessors. Found : ";
        if (preds != null) {
            for(Operator lo : preds) {
                if (!(lo instanceof LOFilter
                        || lo instanceof LOGenerate || lo instanceof LOInnerLoad
                        || lo instanceof LOLoad || lo instanceof LOSplitOutput
                        || lo instanceof LOSplit
                        || isAcceptableSortOp(lo)
                        || isAcceptableForEachOp(lo))) {
                    throw new LogicalToPhysicalTranslatorException(errMsg, errCode);
                }

                // Repeat until LOSort or top of the tree is reached.
                if (! (lo instanceof LOSort)) {
                    validateMapSideMerge(lp.getPredecessors(lo),lp);
                }
            }
        }
        // We visited everything and all is good.
        return true;
    }

    private boolean isAcceptableForEachOp(Operator lo) throws LogicalToPhysicalTranslatorException {
        if (lo instanceof LOForEach) {
            OperatorPlan innerPlan = ((LOForEach) lo).getInnerPlan();
            validateMapSideMerge(innerPlan.getSinks(), innerPlan);
            return !containsUDFs((LOForEach) lo);
        } else {
            return false;
        }
    }

    private boolean isAcceptableSortOp(Operator op) throws LogicalToPhysicalTranslatorException {
        if (!(op instanceof LOSort)) {
            return false;
        }
        LOSort sort = (LOSort) op;
        try {
            for (SortColInfo colInfo : sort.getSortInfo().getSortColInfoList()) {
                // TODO: really, we should check that the sort is on the join keys, in the same order!
                if (colInfo.getSortOrder() != Order.ASCENDING) {
                    return false;
                }
            }
        } catch (FrontendException e) {
           throw new LogicalToPhysicalTranslatorException(e);
        }
        return true;
    }

    private boolean containsUDFs(LOForEach fo) throws LogicalToPhysicalTranslatorException {
        LogicalPlan logExpPlan = fo.getInnerPlan();
        UDFFinder udfFinder;
        try {
            udfFinder = new UDFFinder(logExpPlan);
            udfFinder.visit();
            // TODO (dvryaboy): in the future we could relax this rule by tracing what fields
            // are being passed into the UDF, and only refusing if the UDF is working on the
            // join key. Transforms of other fields should be ok.
            if (udfFinder.getUDFList().size() != 0) {
                return true;
            }
        } catch (FrontendException e) {
            throw new LogicalToPhysicalTranslatorException(e);
        }
        return false;
    }
}
