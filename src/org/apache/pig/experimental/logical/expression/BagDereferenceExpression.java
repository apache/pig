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
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;

/**
 * 
 * This is a special case Expression and violates some of the rules of an
 * Expression.
 * Violation:
 *   It has multiple Uids ( though not tracked there can be multiple uids
 *      for this expression )
 * 
 * This is a special operator which handles the case described below
 * Tuple( a:int, b:bag{ b_a:int, b_b:float } ) --> 
 * BagDereference ( 0 ) --> project( 0, 1 ) --> bag{ b_a:float }
 * 
 * 
 * 
 * i.e. First input ( 0 ), second column ( 1 ) and first column of the bag.
 *
 */
public class BagDereferenceExpression extends ColumnExpression {
    
    private List<Integer> columns;// The column in the input bag which the project references.
    // Count is zero based.
    
    public BagDereferenceExpression(OperatorPlan plan, byte type, int colNum, ProjectExpression exp) {
        super( "BagDereference", plan, type );
        plan.add(this);
        plan.connect(this, exp);
        columns = new ArrayList<Integer>();
        columns.add(colNum);
    }

    public BagDereferenceExpression(OperatorPlan plan, byte type, List<Integer> columnNums, ProjectExpression exp) {
        super( "BagDereference", plan, type );
        plan.add(this);
        plan.connect(this, exp);
        columns = new ArrayList<Integer>();
        columns.addAll(columnNums);
    }

    /**
     * @link org.apache.pig.experimental.plan.Operator#accept(org.apache.pig.experimental.plan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new IOException("Expected LogicalExpressionVisitor");
        }
        ((LogicalExpressionVisitor)v).visitBagDereference(this);
    }
    
    /**
     * Column number in the bag at column <code>col</code>
     * this Dereference References.  The column number is the column
     * in the relational operator that contains this expression.  The count
     * is zero based.
     * @return column number
     */
    public int getBagColNum() {
        if( columns.size() != 1 ) {
            throw new RuntimeException(
                    "Internal error: improper use of getBagColNum in "
                            + BagDereferenceExpression.class.getName());
        }
        return columns.get(0);
    }
    
    public List<Integer> getBagColumns() {
        return columns;
    }
    
    public void setBagColumns(List<Integer> columns) {
        this.columns.clear();
        this.columns.addAll(columns);
    }
    
    public void setBagColNum(int colNum) {
        columns.clear();
        columns.add(colNum);
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
        
        // We generate a new Uid so it is easy to compute schema
        uid = getNextUid();
//        ProjectExpression prj = getProjectExpression();
//        LogicalRelationalOperator referent = prj.findReferent(currentOp);
//        
//        LogicalSchema schema = referent.getSchema();
//        if (schema != null) {
//            LogicalFieldSchema field = schema.getField(prj.getColNum());
//            if( field.schema == null || field.schema.size() == 0 || 
//                    ( field.schema.getField(0).type == DataType.TUPLE && 
//                            field.schema.getField(0).schema == null ) ) {
//                throw new IOException("No schema found in bag for BagDereference!");
//            }
//            if( field.schema.getField(0).type == DataType.TUPLE ) {
//                uid = field.schema.getField(0).schema.getField(columns.get(0)).uid;
//            } else {
//                uid = field.schema.getField(columns.get(0)).uid;
//            }
//        } else {
//            // If the schema of referent is null, we kindof create a uid so we 
//            // can track it in remaining plan
//            uid = getNextUid();
//        }
    }

    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof BagDereferenceExpression) {
            BagDereferenceExpression po = (BagDereferenceExpression)other;
            try {
                if( po.columns.size() != columns.size() ) {
                    return false;
                }
                return po.columns.containsAll(columns) && getProjectExpression().isEqual(po.getProjectExpression());
            } catch (IOException e) {
                return false;
            }
        } else {
            return false;
        }
    }
    
    public ProjectExpression getProjectExpression() throws IOException {
        if( plan.getSuccessors(this).size() < 1 ) {
            throw new IOException("Could not find a related project Expression for BagDereference");
        }
        return (ProjectExpression) plan.getSuccessors(this).get(0);
    }
    
    public String toString() {
        StringBuilder msg = new StringBuilder();

        msg.append("(Name: " + name + " Type: " + type + " Uid: " + uid + 
                " Column: " + columns + ")");

        return msg.toString();
    }
}
