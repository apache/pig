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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;

/**
 * RANK operator implementation.
 * Operator Syntax:
 * <pre>
 * {@code alias = RANK rel ( BY (col_ref) (ASC|DESC)? ( DENSE )? )?;}
 * alias - output alias
 * RANK - operator
 * rel - input relation
 * BY - operator
 * col_ref - STAR or Column References or a range in the schema of rel
 * DENSE - dense rank means a sequential value without gasp among different tuple values.
 * </pre>
 */

public class LORank extends LogicalRelationalOperator{

    private final static String RANK_COL_NAME = "rank";
    private final static String SEPARATOR = "_";

    /**
     * A List within logical expression plans in case of RANK BY
     */
    private List<LogicalExpressionPlan> rankColPlans;

    /**
     * A List within ascending columns on a RANK BY
     */
    private List<Boolean> ascCols;

    /**
     * In case of RANK BY, it could by dense or not.
     * Being a dense rank means to assign consecutive ranking
     * to different tuples.
     */
    private boolean isDenseRank = false;

    /**
     * In case of simple RANK, namely row number mode
     * which is a consecutive number assigned to each tuple.
     */
    private boolean isRowNumber = false;

    /**
     * This is a uid which has been generated for the rank column. It is
     * important to keep this so that the uid will be persistent between calls
     * of resetSchema and getSchema.
     */
    private long rankColumnUid;

    public LORank( OperatorPlan plan) {
        super("LORank", plan);
        this.rankColumnUid = -1;
    }

    public LORank( OperatorPlan plan, List<LogicalExpressionPlan> rankColPlans, List<Boolean> ascCols) {
        this( plan );
        this.rankColPlans = rankColPlans;
        this.ascCols = ascCols;
        this.rankColumnUid = -1;
    }

    public List<LogicalExpressionPlan> getRankColPlans() {
        return rankColPlans;
    }

    public void setRankColPlan(List<LogicalExpressionPlan> rankColPlans) {
        this.rankColPlans = rankColPlans;
    }

    public List<Boolean> getAscendingCol() {
        return ascCols;
    }

    public void setAscendingCol(List<Boolean> ascCols) {
        this.ascCols = ascCols;
    }

    /**
     * Get the schema for the output of LORank.
     * Composed by long value prepended to the
     * rest of the input schema
     * @return the schema
     * @throws FrontendException
     */
    @Override
    public LogicalSchema getSchema() throws FrontendException {

        // if schema is calculated before, just return
        if (schema != null) {
            return schema;
        }

        LogicalRelationalOperator input = null;

        //Same schema of previous predecessor
        input = (LogicalRelationalOperator)plan.getPredecessors(this).get(0);
        if (input == null) {
            return null;
        }

        LogicalSchema inputSchema = input.getSchema();

        // the schema of one input is unknown, so the rank schema is unknown, just return
        if (inputSchema == null) {
            schema = null;
            return schema;
        }

        //Complete copy from previous schema for each LogicalFieldSchema
        List<LogicalSchema.LogicalFieldSchema> fss = new ArrayList<LogicalSchema.LogicalFieldSchema>();

        for (int i=0; i<inputSchema.size(); i++) {
            LogicalSchema.LogicalFieldSchema fs = inputSchema.getField(i);
            LogicalSchema.LogicalFieldSchema newFS = null;
            newFS = new LogicalSchema.LogicalFieldSchema(fs.alias, fs.schema, fs.type, fs.uid);
            fss.add(newFS);
        }

        schema = new LogicalSchema();

        rankColumnUid = rankColumnUid == -1 ? LogicalExpression.getNextUid() : rankColumnUid;
        schema.addField(new LogicalSchema.LogicalFieldSchema(RANK_COL_NAME + SEPARATOR + input.getAlias(),
                null, DataType.LONG, rankColumnUid));

        for(LogicalSchema.LogicalFieldSchema fieldSchema: fss) {
            schema.addField(fieldSchema);
        }

        return schema;

    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LORank) {
            LORank oR = (LORank)other;
            if (!rankColPlans.equals(oR.rankColPlans))
                return false;
        } else {
            return false;
        }

        return checkEquality((LogicalRelationalOperator)other);
    }

    /**
     * Get if it is a dense RANK BY
     * @return boolean
     */
    public boolean isDenseRank() {
        return isDenseRank;
    }

    /**
     * Set if it is a dense RANK BY
     * @param isDenseRank if is dense rank or not
     */
    public void setIsDenseRank(boolean isDenseRank) {
        this.isDenseRank = isDenseRank;
    }

    /**
     * Get if it is a simple RANK operation.
     * Which means a row number attached to each tuple.
     * @return boolean
     */
    public boolean isRowNumber() {
        return isRowNumber;
    }

    /**
     * Set if it is a simple RANK operation.
     * @param rowNumber if is a row number operation
     */
    public void setIsRowNumber(boolean rowNumber) {
        this.isRowNumber = rowNumber;
    }
}
