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

import java.util.List;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
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
    private String alias; // The alias of the projected field.
    
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

    public ProjectExpression(OperatorPlan plan, int inputNum, String alias,
            LogicalRelationalOperator attachedRelationalOp) {
        super("Project", plan);
        input = inputNum;
        this.alias = alias;
        plan.add(this);
        this.attachedRelationalOp = attachedRelationalOp;
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
    
    public String getColAlias() {
        return alias;
    }
    
    /**
     * Set the column number for this project.  This should only be called by
     * ProjectionPatcher.  Stupid Java needs friends.  
     * @param colNum new column number for projection
     */
    public void setColNum(int colNum) {
        col = colNum;
        alias = null; // Once the column number is set, alias is no longer needed.
    }
    
    public boolean isProjectStar() {
        return col<0;
    }
    
    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema() throws FrontendException {
        if (fieldSchema!=null)
            return fieldSchema;
        LogicalRelationalOperator referent = findReferent();
        
        LogicalSchema schema = referent.getSchema();
        
        if (attachedRelationalOp instanceof LOGenerate && plan.getSuccessors(this)==null) {
            if (!(findReferent() instanceof LOInnerLoad)||
                    ((LOInnerLoad)findReferent()).sourceIsBag()) {
                String alias = findReferent().getAlias();
                List<LOInnerLoad> innerLoads = LOForEach.findReacheableInnerLoadFromBoundaryProject(this);
                
                // pull tuple information from innerload
                if (innerLoads.get(0).getProjection().getFieldSchema().schema!=null &&
                        innerLoads.get(0).getProjection().getFieldSchema().type==DataType.BAG) {
                    LogicalFieldSchema originalTupleFieldSchema = innerLoads.get(0).getProjection().getFieldSchema().schema.getField(0);
                    LogicalFieldSchema newTupleFieldSchema = new LogicalFieldSchema(originalTupleFieldSchema.alias,
                            schema, DataType.TUPLE);
                    newTupleFieldSchema.uid = originalTupleFieldSchema.uid;
                    LogicalSchema newTupleSchema = new LogicalSchema();
                    newTupleSchema.addField(newTupleFieldSchema);
                    fieldSchema = new LogicalSchema.LogicalFieldSchema(alias, newTupleSchema, DataType.BAG);
                }
                else {
                    fieldSchema = new LogicalSchema.LogicalFieldSchema(alias, schema, DataType.BAG);
                }
                fieldSchema.uid = innerLoads.get(0).getProjection().getFieldSchema().uid;
            }
            else {
                if (findReferent().getSchema()!=null)
                    fieldSchema = findReferent().getSchema().getField(0);
            }
            if (fieldSchema!=null)
                uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
        }
        else {
            if (schema == null) {
                fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null, DataType.BYTEARRAY);
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
                    if (schema!=null && schema.size()>index)
                        fieldSchema = schema.getField(index);
                    else
                        fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null, DataType.BYTEARRAY);
                    uidOnlyFieldSchema = fieldSchema.cloneUid();
                }
                else {
                    LogicalSchema newTupleSchema = null;
                    if (schema!=null)
                        newTupleSchema = schema.deepCopy();
                    fieldSchema = new LogicalSchema.LogicalFieldSchema(null, newTupleSchema, DataType.TUPLE);
                    uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
                }
            }
        }

        return fieldSchema;
    }
    
    /**
     * Find the LogicalRelationalOperator that this projection refers to.
     * @return LRO this projection refers to
     * @throws FrontendException
     */
    public LogicalRelationalOperator findReferent() throws FrontendException {
        List<Operator> preds;
        preds = attachedRelationalOp.getPlan().getPredecessors(attachedRelationalOp);
        if (preds == null || input >= preds.size()) {
            throw new FrontendException("Projection with nothing to reference!", 2225);
        }
        
        LogicalRelationalOperator pred =
            (LogicalRelationalOperator)preds.get(input);
        if (pred == null) {
            throw new FrontendException("Cannot fine reference for " + this, 2226);
        }
        return pred;
    }
    
    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof ProjectExpression) {
            ProjectExpression po = (ProjectExpression)other;
            return po.input == input && po.col == col;
        } else {
            return false;
        }
    }
    
    public String toString() {
        StringBuilder msg = new StringBuilder();
        if (fieldSchema!=null && fieldSchema.alias!=null)
            msg.append(fieldSchema.alias+":");
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
        msg.append(" Input: " + input + " Column: ");
        if( alias != null )
            msg.append( alias );
        else if (isProjectStar())
            msg.append("(*)");
        else
            msg.append(col);
        msg.append(")");

        return msg.toString();
    }
    
    public LogicalRelationalOperator getAttachedRelationalOp() {
        return attachedRelationalOp;
    }
    
    public void setAttachedRelationalOp(LogicalRelationalOperator attachedRelationalOp) {
        this.attachedRelationalOp = attachedRelationalOp;
    }
    
    @Override
    public byte getType() throws FrontendException {
        // for boundary project, if 
        if (getFieldSchema()==null) {
            if (attachedRelationalOp instanceof LOGenerate && findReferent() instanceof
                    LOInnerLoad) {
                if (((LOInnerLoad)findReferent()).getProjection().getColNum()==-1)
                    return DataType.TUPLE;
            }
            return DataType.BYTEARRAY;
        }
        return super.getType();
    }

    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        LogicalExpression copy = new ProjectExpression(
                lgExpPlan,
                this.getInputNum(),
                this.getColNum(),
                this.getAttachedRelationalOp());
        return copy;
    }

}
