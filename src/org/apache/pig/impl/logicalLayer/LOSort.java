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
package org.apache.pig.impl.logicalLayer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;

public class LOSort extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private List<Integer> mSortCols;
    private List<Boolean> mAscCols;
    private LOUserFunc mSortFunc;

    /**
     * @param plan LogicalPlan this operator is a part of.
     * @param key OperatorKey for this operator
     * @param sortCols Array of column numbers that will be used for sorting
     * data.
     * @param ascCols Array of booleans.  Should be same size as sortCols.  True
     * indicates sort ascending (default), false sort descending.  If this array
     * is null, then all columns will be sorted ascending.
     * @param rp Requested level of parallelism to be used in the sort.
     */
    public LOSort(LogicalPlan plan,
                  OperatorKey key,
                  List<Integer> sortCols,
                  List<Boolean> ascCols,
                  LOUserFunc sortFunc,
                  int rp) {
        super(plan, key, rp);
        mSortCols = sortCols;
        mAscCols = ascCols;
        mSortFunc = sortFunc;
    }

    public List<Integer> getSortCols() {
        return mSortCols;
    }

    public List<Boolean> getAscendingCols() {
        return mAscCols;
    }

    public LOUserFunc getUserFunc() {
        return mSortFunc;
    }
    
    @Override
    public String name() {
        return "SORT " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public String typeName() {
        return "LOSort";
    }

    @Override
    public Schema getSchema() {
        if (mSchema == null) {
            // get our parent's schema
            Collection<LogicalOperator> s = mPlan.getSuccessors(this);
            mSchema = s.iterator().next().getSchema();
        }
        return mSchema;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public void visit(PlanVisitor v) throws ParseException {
        if (!(v instanceof LOVisitor)) {
            throw new RuntimeException("You can only visit LogicalOperators "
                + "with an LOVisitor!");
        }
        ((LOVisitor)v).visitSort(this);
    }
}
