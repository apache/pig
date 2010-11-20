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

package org.apache.pig.newplan.logical.expression;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

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
public class DereferenceExpression extends ColumnExpression {
    
    private List<Integer> columns;// The column in the input bag which the project references.
    // Count is zero based.
    
    public DereferenceExpression(OperatorPlan plan, int colNum) {
        super( "Dereference", plan );
        columns = new ArrayList<Integer>();
        columns.add(colNum);
        plan.add(this);
    }

    public DereferenceExpression(OperatorPlan plan, List<Integer> columnNums) {
        super( "Dereference", plan );
        columns = new ArrayList<Integer>();
        columns.addAll(columnNums);
        plan.add(this);
    }

    /**
     * @link org.apache.pig.newplan.Operator#accept(org.apache.pig.newplan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new FrontendException("Expected LogicalExpressionVisitor", 2222);
        }
        ((LogicalExpressionVisitor)v).visit(this);
    }
    
    public List<Integer> getBagColumns() {
        return columns;
    }
    
    public void setBagColumns(List<Integer> columns) {
        this.columns = columns;
    }
    
    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof DereferenceExpression) {
            DereferenceExpression po = (DereferenceExpression)other;
            if( po.columns.size() != columns.size() ) {
                return false;
            }
            return po.columns.containsAll(columns) && getReferredExpression().isEqual(po.getReferredExpression());
        } else {
            return false;
        }
    }
    
    public LogicalExpression getReferredExpression() throws FrontendException {
        if( plan.getSuccessors(this).size() < 1 ) {
            throw new FrontendException("Could not find a related project Expression for Dereference", 2228);
        }
        return (LogicalExpression) plan.getSuccessors(this).get(0);
    }
    
    public String toString() {
        StringBuilder msg = new StringBuilder();
        msg.append("(Name: " + name + " Type: ");
        if (fieldSchema!=null)
            msg.append(DataType.findTypeName(fieldSchema.type));
        else
            msg.append("null");
        msg.append(" Uid: ");
        if (fieldSchema!=null)
            msg.append(fieldSchema.uid);
        else
            msg.append("null");
        msg.append(" Column:" + columns);
        msg.append(")");

        return msg.toString();
    }
    
    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema() throws FrontendException {
        if (fieldSchema!=null)
            return fieldSchema;
        LogicalExpression successor = (LogicalExpression)plan.getSuccessors(this).get(0);
        LogicalFieldSchema predFS = successor.getFieldSchema();
        if (predFS!=null) {
            if (columns.size()>1 || predFS.type==DataType.BAG) {
                LogicalSchema innerSchema = null;
                if (predFS.schema!=null) {
                    innerSchema = new LogicalSchema();
                    LogicalSchema realSchema;
                    if (predFS.schema.isTwoLevelAccessRequired()) {
                        realSchema = predFS.schema.getField(0).schema;
                    }
                    else {
                        realSchema = predFS.schema;
                    }
                    if (realSchema!=null) {
                        for (int column:columns) {
                            innerSchema.addField(realSchema.getField(column));
                        }
                    }
                }
                fieldSchema = new LogicalSchema.LogicalFieldSchema(null, innerSchema, predFS.type, 
                        LogicalExpression.getNextUid());
            }
            else { // Dereference a field out of a tuple
                if (predFS.schema!=null)
                    fieldSchema = predFS.schema.getField(columns.get(0));
            }
        }
        return fieldSchema;
    }

    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        List<Integer> columnsCopy = new ArrayList<Integer>(this.getBagColumns());
        LogicalExpression copy = new DereferenceExpression(
                lgExpPlan,
                columnsCopy);
        
        // Only one input is expected.
        LogicalExpression input = (LogicalExpression) plan.getSuccessors( this ).get( 0 );
        LogicalExpression inputCopy = input.deepCopy( lgExpPlan );
        lgExpPlan.add( inputCopy );
        lgExpPlan.connect( copy, inputCopy );
        
        return copy;
    }

}
