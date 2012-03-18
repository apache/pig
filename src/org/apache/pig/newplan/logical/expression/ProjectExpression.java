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

import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.parser.SourceLocation;

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

    //fields for range projection. 
    private boolean isRangeProject = false;
    //start and end columns in range. endCol value of -1 represents everything upto end
    private int startCol = -1;
    private int endCol = -2;
    
    private String startAlias;
    private String endAlias;

    
    
    /**
     * Adds projection to the plan.
     * @param plan LogicalExpressionPlan this projection will be a part of
     * @param inputNum Input number this project references.
     * @param colNum Column number this project references.
     * @param attachedRelationalOp
     */
    public ProjectExpression(OperatorPlan plan,
                             int inputNum,
                             int colNum, LogicalRelationalOperator attachedRelationalOp) {
        super("Project", plan);
        this.input = inputNum;
        this.col = colNum;
        plan.add(this);
        this.attachedRelationalOp = attachedRelationalOp;
    }

    /**
     * Adds projection to the plan.
     * @param plan
     * @param inputNum
     * @param alias
     * @param attachedRelationalOp
     * @throws FrontendException 
     */
    public ProjectExpression(OperatorPlan plan, int inputNum, String alias,
            LogicalRelationalOperator attachedRelationalOp) {
        super("Project", plan);
        this.input = inputNum;
        this.alias = alias;
        plan.add(this);
        this.attachedRelationalOp = attachedRelationalOp;
    }

    /**
     * Constructor for range projection
     * Adds projection to the plan.
     * The start and end alias/column-number should be set separately. 
     * @param plan
     * @param inputNum
     * @param attachedRelationalOp
     */
    public ProjectExpression(OperatorPlan plan, int inputNum, LogicalRelationalOperator attachedRelationalOp) {
        super("Project", plan);
        input = inputNum;
        isRangeProject = true;
        plan.add(this);
        this.attachedRelationalOp = attachedRelationalOp;
    }

    /**
     * like a copy constructor, but with a plan argument
     * @param projExpr
     * @param plan
     */
    public ProjectExpression(ProjectExpression projExpr, OperatorPlan plan) {
        super("Project", plan);
        this.input = projExpr.input;
        this.col = projExpr.col;
        this.alias = projExpr.alias;
        this.attachedRelationalOp = projExpr.attachedRelationalOp;
        this.isRangeProject = projExpr.isRangeProject;
        this.startCol = projExpr.startCol;
        this.endCol = projExpr.endCol;
        this.startAlias = projExpr.startAlias;
        this.endAlias = projExpr.endAlias;
        plan.add(this);
        
    }

    /**
     * If there is an alias, finds the column number from it.
     * @throws FrontendException if there is no such alias
     */
    public void setColumnNumberFromAlias() throws FrontendException{
        if(isRangeProject){
            if(startAlias != null){
                startCol = findColNum(startAlias);
                startAlias = null;
            }
            if(endAlias != null){
                endCol = findColNum(endAlias);
                endAlias = null;
            }
            if(startCol < 0){
                String msg = "Invalid start column position in " +
                "range projection (..) " + startCol;
                throw new PlanValidationException(this, msg, 2270, PigException.BUG);
            }
            
            if(endCol > 0 && startCol > endCol){
                String msg = "start column appears after end column in " +
                "range projection (..) . Start column position " + startCol +
                " End column position " + endCol;
                throw new PlanValidationException(this, msg, 1127, PigException.INPUT);
            }
        }else{
            setColNum(findColNum(alias));
        }
    }
    
    private int findColNum(String alias) throws FrontendException {
        LogicalPlan lp = (LogicalPlan)attachedRelationalOp.getPlan();
        List<Operator> inputs = lp.getPredecessors( attachedRelationalOp );
        LogicalRelationalOperator input = (LogicalRelationalOperator)inputs.get( getInputNum() );
        LogicalSchema inputSchema = input.getSchema();
        
        if( alias != null ) {
            int colNum = inputSchema == null ? -1 : inputSchema.getFieldPosition( alias );
            if( colNum == -1 ) {
            	String msg = "Invalid field projection. Projected field [" + alias + "] does not exist";
                if( inputSchema != null )
                	msg += " in schema: " + inputSchema.toString( false );
                msg += ".";
                throw new PlanValidationException( this, msg, 1025 );
            }
            return colNum;
        } else {
            int col = getColNum();
            if( inputSchema != null && col >= inputSchema.size() ) {
                throw new PlanValidationException( this,
                        "Out of bound access. Trying to access non-existent column: " + 
                        col + ". Schema " +  inputSchema.toString(false) + 
                        " has " + inputSchema.size() + " column(s)." , 1000);
            }
            return col;
        }
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
        if(isRangeProject){
            throw new AssertionError("getColNum should not be called on range project");
        }
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

    public boolean isRangeProject() {
        return isRangeProject;
    }
    
    public boolean isRangeOrStarProject(){
        return isProjectStar() || isRangeProject();
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

                Pair<List<LOInnerLoad>, Boolean> innerLoadsPair = LOForEach.findReacheableInnerLoadFromBoundaryProject(this);
                List<LOInnerLoad> innerLoads = innerLoadsPair.first;
                boolean needNewUid = innerLoadsPair.second;
                
                // pull tuple information from innerload
                if (innerLoads.get(0).getProjection().getFieldSchema().schema!=null &&
                        innerLoads.get(0).getProjection().getFieldSchema().type==DataType.BAG) {
                    LogicalFieldSchema originalTupleFieldSchema = innerLoads.get(0).getProjection().getFieldSchema().schema.getField(0);
                    LogicalFieldSchema newTupleFieldSchema = new LogicalFieldSchema(originalTupleFieldSchema.alias,
                            schema, DataType.TUPLE);
                    if (needNewUid) {
                        newTupleFieldSchema.uid = LogicalExpression.getNextUid();
                    }
                    else {
                        newTupleFieldSchema.uid = originalTupleFieldSchema.uid;
                    }
                    LogicalSchema newTupleSchema = new LogicalSchema();
                    newTupleSchema.addField(newTupleFieldSchema);
                    fieldSchema = new LogicalSchema.LogicalFieldSchema(alias, newTupleSchema, DataType.BAG);
                }
                else {
                    fieldSchema = new LogicalSchema.LogicalFieldSchema(alias, schema, DataType.BAG);
                }
                if (needNewUid)
                    fieldSchema.uid = LogicalExpression.getNextUid();
                else
                    fieldSchema.uid = innerLoads.get(0).getProjection().getFieldSchema().uid;
            }
            else {
                // InnerLoad and source is not bag
                if(schema == null){
                    // if we get here, it is range or starProject, otherwise, innerLoad will convert schema to non-null
                    if (isRangeProject && endCol!=-1){
                        LogicalSchema innerSchema = new LogicalSchema();
                        for(int i = startCol; i <= endCol; i++){
                            //schema is null, so null alias
                            innerSchema.addField(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
                        }
                        fieldSchema = new LogicalSchema.LogicalFieldSchema(null, innerSchema, DataType.TUPLE);
                    } else {
                        fieldSchema = null;
                    }
                }
                else{
                    fieldSchema = schema.getField(0);
                }
            }
            if (fieldSchema!=null)
                uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
        }
        else {
            if (schema == null) {
                if(isRangeOrStarProject()) {
                    if (isRangeProject && endCol!=-1){
                        LogicalSchema innerSchema = new LogicalSchema();
                        for(int i = startCol; i <= endCol; i++){
                            //schema is null, so null alias
                            innerSchema.addField(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
                        }
                        fieldSchema = new LogicalSchema.LogicalFieldSchema(null, innerSchema, DataType.TUPLE);
                    }
                    else {
                        fieldSchema = null;
                    }
                } else {
                    fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null, DataType.BYTEARRAY);
                }
                
                if (fieldSchema!=null)
                    uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
            } 
            else {
                int index = -1;
                if (!isRangeOrStarProject() && uidOnlyFieldSchema!=null) {
                    long uid = uidOnlyFieldSchema.uid;
                    for (int i=0;i<schema.size();i++) {
                        LogicalFieldSchema fs = schema.getField(i);
                        if (fs.uid==uid) {
                            index = i;
                        }
                    }
                }
                if (index==-1) {
                    if (alias!=null) {
                        index = schema.getFieldPosition(alias);
                    }
                }
                if (index==-1)
                    index = col;
                
                if (!isRangeOrStarProject()) {
                    if (schema!=null && schema.size()>index)
                        fieldSchema = schema.getField(index);
                    else
                        fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null, DataType.BYTEARRAY);
                    uidOnlyFieldSchema = fieldSchema.cloneUid();
                }
                else {
                    LogicalSchema newTupleSchema = null;
                    if(isProjectStar())
                        newTupleSchema = schema.deepCopy();
                    else{
                        //project range
                        newTupleSchema = new LogicalSchema();
                        for(int i = startCol; i <= endCol; i++){
                            newTupleSchema.addField(schema.getField(i).deepCopy());
                        }
                    }
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
            if (po.input != input || po.col != col)
                return false;
            
            Operator mySucc = getPlan().getSuccessors(this)!=null?
                    getPlan().getSuccessors(this).get(0):null;
            Operator theirSucc = other.getPlan().getSuccessors(other)!=null?
                    other.getPlan().getSuccessors(other).get(0):null;
            if (mySucc!=null && theirSucc!=null)
                return mySucc.isEqual(theirSucc);
            if (mySucc==null && theirSucc==null)
                return true;
            return false;
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
        else if (isRangeProject)
            msg.append("[").append(startCol).append(" .. ").append(endCol).append("]");
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
                if (((LOInnerLoad)findReferent()).getProjection().isRangeOrStarProject())
                    return DataType.TUPLE;
            }
            return DataType.BYTEARRAY;
        }
        return super.getType();
    }

    /**
     * @return the startCol
     */
    public int getStartCol() {
        return startCol;
    }

    /**
     * @param startCol the startCol to set
     */
    public void setStartCol(int startCol) {
        this.startCol = startCol;
    }

    /**
     * @return the endCol
     */
    public int getEndCol() {
        return endCol;
    }

    /**
     * @param endCol the endCol to set
     */
    public void setEndCol(int endCol) {
        this.endCol = endCol;
    }

    /**
     * @param startAlias
     * @throws FrontendException 
     */
    public void setStartAlias(String startAlias) throws FrontendException {
       this.startAlias = startAlias;
    }

    /**
     * @param endAlias
     * @throws FrontendException 
     */
    public void setEndAlias(String endAlias) throws FrontendException {
        this.endAlias = endAlias;
    }

    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        ProjectExpression copy = new ProjectExpression(
                lgExpPlan,
                this.getInputNum(),
                this.getColNum(),
                this.getAttachedRelationalOp());
        copy.setLocation( new SourceLocation( location ) );
        copy.alias = alias; 
        copy.isRangeProject = this.isRangeProject;
        copy.startCol = this.startCol;
        copy.endCol = this.endCol;
        copy.startAlias = this.startAlias;
        copy.endAlias = this.endAlias;
        
        return copy;
    }

}
