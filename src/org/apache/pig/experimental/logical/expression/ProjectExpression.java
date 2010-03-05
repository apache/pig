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

package org.apache.pig.experimental.logical.expression;

import java.io.IOException;
import java.util.List;

import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;

/**
 * Projection of columns in an expression.
 *
 */
public class ProjectExpression extends ColumnExpression {
    
    private int input; // Which input of the relational operator this project
                       // is projecting from.  Count is zero based.  So if this
                       // project is in a filter the input number will always
                       // be 0 (since filter has only one input).  If it is
                       // in a join, cross, cogroup, or union it could be
                       // greater than 0.
    private int col; // The column in the input which the project references.
                     // Count is zero based.
                      
    
    /**
     * Adds projection to the plan.
     * @param plan LogicalExpressionPlan this projection will be a part of
     * @param type type of this projection, can be unknown
     * @param inputNum Input number this project references.
     * @param colNum Column number this project references.
     */
    public ProjectExpression(OperatorPlan plan,
                             byte type,
                             int inputNum,
                             int colNum) {
        super("Project", plan, type);
        input = inputNum;
        col = colNum;
        plan.add(this);
    }

    /**
     * @link org.apache.pig.experimental.plan.Operator#accept(org.apache.pig.experimental.plan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new IOException("Expected LogicalExpressionVisitor");
        }
        ((LogicalExpressionVisitor)v).visitProject(this);

    }

    /**
     * Input number this project references.  This is the input number for the
     * relational operator that contains this expression.  The count is zero
     * based.
     * @return input number
     */
    public int getInputNum() {
        return input;
    }
    
   
    /**
     * Column number this project references.  The column number is the column
     * in the relational operator that contains this expression.  The count
     * is zero based.
     * @return column number
     */
    public int getColNum() {
        return col;
    }
    
    /**
     * Set the column number for this project.  This should only be called by
     * ProjectionPatcher.  Stupid Java needs friends.  
     * @param colNum new column number for projection
     */
    public void setColNum(int colNum) {
        col = colNum;
    }
    
     /**
     * Set the type of the projection.
     * @param type to set this projection to
     */
    public void setType(byte type) {
        this.type = type;
    }

    @Override
    public void setUid(LogicalRelationalOperator currentOp) throws IOException {
        LogicalRelationalOperator referent = findReferent(currentOp);
        
        LogicalSchema schema = referent.getSchema();
        if (schema != null) {
            uid = schema.getField(col).uid;
        } else {
            // If the schema of referent is null, we kindof create a uid so we 
            // can track it in remaining plan
            uid = getNextUid();
        }
    }
    
    /**
     * Find the LogicalRelationalOperator that this projection refers to.
     * @param currentOp Current operator this projection is attached to
     * @return LRO this projection refers to
     * @throws IOException
     */
    public LogicalRelationalOperator findReferent(LogicalRelationalOperator currentOp) throws IOException {
        List<Operator> preds;
        preds = currentOp.getPlan().getPredecessors(currentOp);
        if (preds == null || preds.size() - 1 < input) {
            throw new IOException("Projection with nothing to reference!");
        }
            
        LogicalRelationalOperator pred =
            (LogicalRelationalOperator)preds.get(input);
        if (pred == null) {
            throw new IOException("Found bad operator in logical plan");
        }
        return pred;
    }
    
    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof ProjectExpression) {
            ProjectExpression po = (ProjectExpression)other;
            return po.input == input && po.col == col;
        } else {
            return false;
        }
    }
    
    public String toString() {
        StringBuilder msg = new StringBuilder();

        msg.append("(Name: " + name + " Type: " + type + " Uid: " + uid + " Input: " + input + " Column: " + col + ")");

        return msg.toString();
    }
}
