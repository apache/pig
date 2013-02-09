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
import org.apache.pig.parser.SourceLocation;

/**
 * 
 * get one or elements out of a tuple or a bag
 * 
 * in case of Tuple( a#2:int, b#3:bag{ b_a#4:int, b_b#5:float }, c#6:int ) # 1
 * (the number after # represents the uid)
 * 
 * Dereference ( 0 ) --> a:int
 * - dereference of single column in a tuple gives the field
 * 
 * Dereference ( 0,2 ) --> Tuple(a#2:int, c#6:int) #7 
 * - dereference of more than one column gives a tuple 
 * 
 * Dereference ( 1 ) --> Dereference ( 1 ) --> b:bag{b_b#5:float}#8 
 * - dereference of a bag gives a bag
 * 
 *
 */
public class DereferenceExpression extends ColumnExpression {
    private List<Object> rawColumns = new ArrayList<Object>();
    
    private List<Integer> columns = new ArrayList<Integer>();// The column in the input bag which the project references.
    // Count is zero based.

    public DereferenceExpression(OperatorPlan plan) {
        super( "Dereference", plan );
        plan.add( this );
    }
    
    public DereferenceExpression(OperatorPlan plan, int colNum) {
        this( plan );
        columns.add(colNum);
    }

    public DereferenceExpression(OperatorPlan plan, List<Integer> columnNums) {
        this( plan );
        columns.addAll(columnNums);
    }
    
    public void setRawColumns(List<Object> cols) {
        rawColumns.addAll( cols );
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
        this.rawColumns.clear(); // We don't need this any more.
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
            if (predFS.type==DataType.BAG) {
                LogicalSchema innerSchema = null;
                if (predFS.schema!=null) {
                    innerSchema = new LogicalSchema();
                    // Get the tuple inner schema
                    LogicalSchema origSchema = predFS.schema.getField(0).schema;;
                    // Slice the tuple inner schema
                    if (!rawColumns.isEmpty()) {
                        columns = translateAliasToPos(origSchema, rawColumns);
                    }
                    for (int column:columns) {
                        if (origSchema!=null && origSchema.size()!=0) {
                            innerSchema.addField(origSchema.getField(column));
                        }
                        else {
                            innerSchema.addField(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
                        }
                    }
                }
                LogicalSchema bagSchema = new LogicalSchema();
                bagSchema.addField(new LogicalSchema.LogicalFieldSchema(null, innerSchema, DataType.TUPLE, 
                        LogicalExpression.getNextUid()));
                fieldSchema = new LogicalSchema.LogicalFieldSchema(null, bagSchema, DataType.BAG, LogicalExpression.getNextUid());
                uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
            }
            else { // Dereference a field out of a tuple
                if (predFS.schema!=null) {
                    if (!rawColumns.isEmpty()) {
                        columns = translateAliasToPos(predFS.schema, rawColumns);
                    }
                    if (predFS.schema!=null && predFS.schema.size()!=0) {
                        fieldSchema = predFS.schema.getField(columns.get(0));
                    }
                    else {
                        fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null, DataType.BYTEARRAY);
                        uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
                    }
                } else{
                    fieldSchema = new LogicalFieldSchema(null, null, DataType.BYTEARRAY);
                    uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
                }
            }
        }
        return fieldSchema;
    }
        
    private List<Integer> translateAliasToPos(LogicalSchema schema, List<Object> rawColumns) throws FrontendException {
        List<Integer> columns = new ArrayList<Integer>();
        for( Object rawColumn : rawColumns ) {
            if( rawColumn instanceof Integer ) {
            	if (schema!=null && ((Integer)rawColumn>=schema.size() || (Integer)rawColumn<0)) {
            	    throw new FrontendException("Index "+rawColumn + " out of range in schema:" + schema.toString(false), 1127);
            	}
                columns.add( (Integer)rawColumn );
            } else {
                int pos = schema.getFieldPosition((String)rawColumn);
                if( pos != -1) {
                    columns.add( pos );
                    continue;
                } else {
                    throw new FrontendException("Cannot find field " + rawColumn + " in " + schema.toString(false), 1128);
                }
            }
        }
        return columns;
    }

    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        List<Integer> columnsCopy = new ArrayList<Integer>(this.getBagColumns());
        DereferenceExpression copy = new DereferenceExpression(
                lgExpPlan,
                columnsCopy);
        List<Object> rawColumnsCopy = new ArrayList<Object>( this.rawColumns );
        copy.setRawColumns( rawColumnsCopy );
        
        // Only one input is expected.
        LogicalExpression input = (LogicalExpression) plan.getSuccessors( this ).get( 0 );
        LogicalExpression inputCopy = input.deepCopy( lgExpPlan );
        lgExpPlan.add( inputCopy );
        lgExpPlan.connect( copy, inputCopy );
        
        copy.setLocation( new SourceLocation( location ) );
        return copy;
    }

    public List<Object> getRawColumns() {
        return this.rawColumns;
    }

}
