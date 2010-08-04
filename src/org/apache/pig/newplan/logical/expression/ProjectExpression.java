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

import java.io.IOException;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

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
    private LogicalRelationalOperator attachedRelationalOp;
                      
    
    /**
     * Adds projection to the plan.
     * @param plan LogicalExpressionPlan this projection will be a part of
     * @param inputNum Input number this project references.
     * @param colNum Column number this project references.
     */
    public ProjectExpression(OperatorPlan plan,
                             int inputNum,
                             int colNum, LogicalRelationalOperator attachedRelationalOp) {
        super("Project", plan);
        input = inputNum;
        col = colNum;
        plan.add(this);
        this.attachedRelationalOp = attachedRelationalOp;
    }

    /**
     * @link org.apache.pig.experimental.plan.Operator#accept(org.apache.pig.experimental.plan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new IOException("Expected LogicalExpressionVisitor");
        }
        ((LogicalExpressionVisitor)v).visit(this);

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
    
   
    public void setInputNum(int inputNum) {
        input = inputNum;
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
    
    public boolean isProjectStar() {
        return col<0;
    }
    
    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema() throws IOException {
        if (fieldSchema!=null)
            return fieldSchema;
        LogicalRelationalOperator referent = findReferent();
        
        LogicalSchema schema = referent.getSchema();
        
        if (schema == null) {
            fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null, DataType.BYTEARRAY);
            uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
        } 
        else {
            if (attachedRelationalOp instanceof LOGenerate && plan.getSuccessors(this)==null) {
                if (!(findReferent() instanceof LOInnerLoad)||
                        ((LOInnerLoad)findReferent()).sourceIsBag()) {
                    String alias = findReferent().getAlias();
                    List<LOInnerLoad> innerLoads = LOForEach.findReacheableInnerLoadFromBoundaryProject(this);
                    
                    // pull tuple information from innerload
                    if (innerLoads.get(0).getProjection().getFieldSchema().schema.isTwoLevelAccessRequired()) {
                        LogicalFieldSchema originalTupleFieldSchema = innerLoads.get(0).getProjection().getFieldSchema().schema.getField(0);
                        LogicalFieldSchema newTupleFieldSchema = new LogicalFieldSchema(originalTupleFieldSchema.alias,
                                schema, DataType.TUPLE);
                        newTupleFieldSchema.uid = originalTupleFieldSchema.uid;
                        LogicalSchema newTupleSchema = new LogicalSchema();
                        newTupleSchema.setTwoLevelAccessRequired(true);
                        newTupleSchema.addField(newTupleFieldSchema);
                        fieldSchema = new LogicalSchema.LogicalFieldSchema(alias, newTupleSchema, DataType.BAG);
                    }
                    else {
                        fieldSchema = new LogicalSchema.LogicalFieldSchema(alias, schema, DataType.BAG);
                    }
                    fieldSchema.uid = innerLoads.get(0).getProjection().getFieldSchema().uid;
                }
                else {
                    fieldSchema = findReferent().getSchema().getField(0);
                }
                uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
            }
            else {
                int index = -1;
                if (!isProjectStar() && uidOnlyFieldSchema!=null) {
                    long uid = uidOnlyFieldSchema.uid;
                    for (int i=0;i<schema.size();i++) {
                        LogicalFieldSchema fs = schema.getField(i);
                        if (fs.uid==uid) {
                            index = i;
                        }
                    }
                }
                if (index==-1)
                    index = col;
                
                if (!isProjectStar()) {
                    fieldSchema = schema.getField(index);
                    uidOnlyFieldSchema = fieldSchema.cloneUid();
                }
                else {
                    fieldSchema = new LogicalSchema.LogicalFieldSchema(null, schema.deepCopy(), DataType.TUPLE);
                    uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
                }
            }
        }

        return fieldSchema;
    }
    
    /**
     * Find the LogicalRelationalOperator that this projection refers to.
     * @param currentOp Current operator this projection is attached to
     * @return LRO this projection refers to
     * @throws IOException
     */
    public LogicalRelationalOperator findReferent() throws IOException {
        List<Operator> preds;
        preds = attachedRelationalOp.getPlan().getPredecessors(attachedRelationalOp);
        if (preds == null || input >= preds.size()) {
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
        try {
            msg.append("(Name: " + name + " Type: ");
            if (fieldSchema!=null)
                msg.append(DataType.findTypeName(getFieldSchema().type));
            else
                msg.append("null");
            msg.append(" Uid: ");
            if (fieldSchema!=null)
                msg.append(getFieldSchema().uid);
            else
                msg.append("null");
            msg.append(" Input: " + input + " Column: ");
            if (isProjectStar())
                msg.append("(*)");
            else
                msg.append(col);
            msg.append(")");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return msg.toString();
    }
    
    public LogicalRelationalOperator getAttachedRelationalOp() {
        return attachedRelationalOp;
    }
    
    public void setAttachedRelationalOp(LogicalRelationalOperator attachedRelationalOp) {
        this.attachedRelationalOp = attachedRelationalOp;
    }
}
