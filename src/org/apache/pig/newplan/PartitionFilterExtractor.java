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

package org.apache.pig.newplan;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UnaryExpression;

/**
 * This is a rewrite of {@code PColFilterExtractor}
 *
 * We traverse the expression plan bottom up and separate it into two plans
 * - pushdownExprPlan, plan that can be pushed down to the loader and
 * - filterExprPlan, remaining plan that needs to be evaluated by pig
 *
 */
public class PartitionFilterExtractor extends FilterExtractor {

    /**
     * partition columns associated with the table
     * present in the load on which the filter whose
     * inner plan is being visited is applied
     */
    private List<String> partitionCols;


    /**
     * @param plan logical plan corresponding the filter's comparison condition
     * @param partitionCols list of partition columns of the table which is
     * being loaded in the LOAD statement which is input to the filter
     */
    public PartitionFilterExtractor(LogicalExpressionPlan plan,
            List<String> partitionCols) {
        super(plan);
        this.partitionCols = new ArrayList<String>(partitionCols);
    }

    @Override
    protected KeyState checkPushDown(ProjectExpression project) throws FrontendException {
        String fieldName = project.getFieldSchema().alias;
        KeyState state = new KeyState();
        if(partitionCols.contains(fieldName)) {
            state.filterExpr = null;
            state.pushdownExpr = project;
        } else {
            state.filterExpr = addToFilterPlan(project);
            state.pushdownExpr = null;
        }
        return state;
    }

    @Override
    protected boolean isSupportedOpType(BinaryExpression binOp) {
        return true;
    }

    @Override
    protected boolean isSupportedOpType(UnaryExpression unaryOp) {
        return false;
    }

}
