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
import java.util.List;

import org.apache.pig.Expression;
import org.apache.pig.PigException;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.Expression.OpType;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This Visitor works on the filter condition of a LOFilter which immediately 
 * follows a LOLoad that interacts with a metadata system (currently OWL) to 
 * read table data. The visitor looks for conditions on partition columns in the
 * filter condition and extracts those conditions out of the filter condition.
 * The condition on partition cols will be used to prune partitions of the table.
 *
 */
public class PColFilterExtractor extends LOVisitor {

    /**
     * partition columns associated with the table
     * present in the load on which the filter whose
     * inner plan is being visited is applied
     */
    private List<String> partitionCols;
    
    /**
     * will contain the partition column filter conditions
     * accumulated during the visit - the final condition will an expression
     * built from these sub expressions connected with AND
     */
    private ArrayList<Expression> pColConditions = new ArrayList<Expression>();
    
    /**
     * flag used during visit to indicate if a partition key
     * was seen
     */
    private boolean sawKey;
    
    private boolean sawNonKeyCol;
    
    private enum Side { LEFT, RIGHT, NONE };
    private Side replaceSide = Side.NONE;
    
    private boolean filterRemovable = false;
    
    @Override
    public void visit() throws VisitorException {
        // we will visit the leaf and it will recursively walk the plan
        try {
            ExpressionOperator leaf = (ExpressionOperator)mPlan.getLeaves().get(0);
            // if the leaf is a unary operator it should be a FilterFunc in 
            // which case we don't try to extract partition filter conditions
            if(leaf instanceof BinaryExpressionOperator) {
                visit((BinaryExpressionOperator)leaf);
                replaceChild(leaf);
                // if the entire expression is to be removed, then the above
                // replaceChild will not set sawKey to false (sawKey is set to
                // false only in replaceChild()
                if(sawKey == true) {
                    //there are only conditions on partition columns in the filter
                    //extract it
                    pColConditions.add(getExpression(leaf));
                    filterRemovable = true;
                }
            }
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }
        
    }
    
    /**
     * 
     * @param plan logical plan corresponding the filter's comparison condition
     * @param partitionCols list of partition columns of the table which is
     * being loaded in the LOAD statement which is input to the filter
     */
    public PColFilterExtractor(LogicalPlan plan,
            List<String> partitionCols) {
        // though we configure a DepthFirstWalker to be the walker, we will not
        // use it - we will visit the leaf and it will recursively walk the
        // plan
        super(plan, new DepthFirstWalker<LogicalOperator, 
                LogicalPlan>(plan));
        this.partitionCols = new ArrayList<String>(partitionCols);
    }
    
    @Override
    protected void visit(LOProject project) throws VisitorException {
        try {
            String fieldName = project.getFieldSchema().alias;
            if(partitionCols.contains(fieldName)) {
                sawKey = true;
                // The condition on partition column will be used to prune the
                // scan and removed from the filter condition. Hence the condition 
                // on the partition column will not be re applied when data is read,
                // so the following cases should throw error until that changes.
                List<Class<?>> opsToCheckFor = new 
                ArrayList<Class<?>>();
                opsToCheckFor.add(LORegexp.class);
                int errCode = 1110;
                if(checkSuccessors(project, opsToCheckFor)) {
                    throw new FrontendException("Unsupported query: " +
                            "You have an partition column (" 
                            + fieldName + ") inside a regexp operator in the " +
                            		"filter condition.", errCode, PigException.INPUT);
                } 
                opsToCheckFor.set(0, LOUserFunc.class);
                if(checkSuccessors(project, opsToCheckFor)) {
                    throw new FrontendException("Unsupported query: " +
                            "You have an partition column (" 
                            + fieldName + ") inside a function in the " +
                                    "filter condition.", errCode, PigException.INPUT);
                }
                opsToCheckFor.set(0, LOCast.class);
                if(checkSuccessors(project, opsToCheckFor)) {
                    throw new FrontendException("Unsupported query: " +
                            "You have an partition column (" 
                            + fieldName + ") inside a cast in the " +
                                    "filter condition.", errCode, PigException.INPUT);                }
                
                opsToCheckFor.set(0, LOIsNull.class);
                if(checkSuccessors(project, opsToCheckFor)) {
                    throw new FrontendException("Unsupported query: " +
                            "You have an partition column (" 
                            + fieldName + ") inside a null check operator in the " +
                                    "filter condition.", errCode, PigException.INPUT);                }
                opsToCheckFor.set(0, LOBinCond.class);
                if(checkSuccessors(project, opsToCheckFor)) {
                    throw new FrontendException("Unsupported query: " +
                            "You have an partition column (" 
                            + fieldName + ") inside a bincond operator in the " +
                                    "filter condition.", errCode, PigException.INPUT);
                }
                opsToCheckFor.set(0, LOAnd.class);
                opsToCheckFor.add(LOOr.class);
                if(checkSuccessors(project, opsToCheckFor)) {
                    errCode = 1112;
                    throw new FrontendException("Unsupported query: " +
                            "You have an partition column (" + fieldName +
                            " ) in a construction like: " +
                            "(pcond  and ...) or (pcond and ...) " +
                            "where pcond is a condition on a partition column.",
                            errCode, PigException.INPUT);
                }
            } else {
                sawNonKeyCol = true;
            }
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }
    }
    
    @Override
    protected void visit(BinaryExpressionOperator binOp)
            throws VisitorException {

        try {
            boolean lhsSawKey = false;        
            boolean rhsSawKey = false;        
            boolean lhsSawNonKeyCol = false;        
            boolean rhsSawNonKeyCol = false;        
            
            sawKey = false;
            sawNonKeyCol = false;
            binOp.getLhsOperand().visit(this);
            replaceChild(binOp.getLhsOperand());
            lhsSawKey = sawKey;
            lhsSawNonKeyCol = sawNonKeyCol;
            

            sawKey = false;
            sawNonKeyCol = false;
            binOp.getRhsOperand().visit(this);
            replaceChild(binOp.getRhsOperand());
            rhsSawKey = sawKey;
            rhsSawNonKeyCol = sawNonKeyCol;
            
            // only in the case of an AND, we potentially split the AND to 
            // remove conditions on partition columns out of the AND. For this 
            // we set replaceSide accordingly so that when we reach a predecessor
            // we can trim the appropriate side. If both sides of the AND have 
            // conditions on partition columns, we will remove the AND completely - 
            // in this case, we will not set replaceSide, but sawKey will be 
            // true so that as we go to higher predecessor ANDs we can trim later.
            if(binOp instanceof LOAnd) {
                if(lhsSawKey && rhsSawNonKeyCol){
                    replaceSide = Side.LEFT;
                }else if(rhsSawKey && lhsSawNonKeyCol){
                    replaceSide = Side.RIGHT;
                }
            } else if(lhsSawKey && rhsSawNonKeyCol || rhsSawKey && lhsSawNonKeyCol){
                int errCode = 1111;
                String errMsg = "Use of partition column/condition with" +
                " non partition column/condition in filter expression is not " +
                "supported." ;
                throw new FrontendException(errMsg, errCode, PigException.INPUT);
            }

            sawKey = lhsSawKey || rhsSawKey;
            sawNonKeyCol = lhsSawNonKeyCol || rhsSawNonKeyCol;
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }        
    }
    
    
    
    /**
     * @return the condition on partition columns extracted from filter
     */
    public  Expression getPColCondition(){
        if(pColConditions.size() == 0)
            return null;
        Expression cond =  pColConditions.get(0);
        for(int i=1; i<pColConditions.size(); i++){
            //if there is more than one condition expression
            // connect them using "AND"s
            cond = new BinaryExpression(cond, pColConditions.get(i),
                    OpType.OP_AND);
        }
        return cond;
    }

    /**
     * @return the filterRemovable
     */
    public boolean isFilterRemovable() {
        return filterRemovable;
    }
    
    //////// helper methods /////////////////////////
    /**
     * check for the presence of a certain operator type in the Successors
     * @param opToStartFrom
     * @param opsToCheckFor operators to be checked for at each level of 
     * Successors - the ordering in the list is the order in which the ops 
     * will be checked.
     * @return true if opsToCheckFor are found
     * @throws FrontendException 
     */
    private boolean checkSuccessors(LogicalOperator opToStartFrom, 
            List<Class<?>> opsToCheckFor) throws FrontendException {
        boolean done = checkSuccessorsHelper(opToStartFrom, opsToCheckFor);
        if(!done && !opsToCheckFor.isEmpty()) {
            // continue checking if there is more to check
            while(!done) {
                opToStartFrom = mPlan.getSuccessors(opToStartFrom).get(0);
                done = checkSuccessorsHelper(opToStartFrom, opsToCheckFor);
            }
        }
        return opsToCheckFor.isEmpty();
    }

    private boolean checkSuccessorsHelper(LogicalOperator opToStartFrom, 
            List<Class<?>> opsToCheckFor) throws FrontendException {
        List<LogicalOperator> successors = mPlan.getSuccessors(
                opToStartFrom);
        if(successors == null || successors.size() == 0) {
            return true; // further checking cannot be done
        }
        if(successors.size() == 1) {
            LogicalOperator suc  = successors.get(0);
            if(suc.getClass().getCanonicalName().equals(
                    opsToCheckFor.get(0).getCanonicalName())) {
                // trim the list of operators to check
                opsToCheckFor.remove(0);
                if(opsToCheckFor.isEmpty()) {
                    return true; //no further checks required
                }
            }
        } else {
            throwException();
        }
        return false; // more checking can be done
    }
    
    private void replaceChild(ExpressionOperator childExpr) throws 
    FrontendException {
        
        if(replaceSide == Side.NONE) {
            // the child is trimmed when the appropriate
            // flag is set to indicate that it needs to be trimmed.
            return;
        }
        
        // eg if replaceSide == Side.LEFT
        //    binexpop
        //   /   \ \ 
        // child (this is the childExpr argument send in)
        //  /  \
        // Lt   Rt 
        //
        // gets converted to 
        //  binexpop
        //  /
        // Rt
        
        if(! (childExpr instanceof BinaryExpressionOperator)){
            throwException();
        }
        // child's lhs operand
        ExpressionOperator childLhs = 
            ((BinaryExpressionOperator)childExpr).getLhsOperand();
        // child's rhs operand
        ExpressionOperator childRhs = 
            ((BinaryExpressionOperator)childExpr).getRhsOperand();
        
        mPlan.disconnect(childLhs, childExpr);
        mPlan.disconnect(childRhs, childExpr);
        
        if(replaceSide == Side.LEFT) {
            // remove left child and replace childExpr with its right child
            remove(childLhs);
            mPlan.replace(childExpr, childRhs);
        } else if(replaceSide == Side.RIGHT){
            // remove right child and replace childExpr with its left child
            remove(childRhs);
            mPlan.replace(childExpr, childLhs);
        }else {
            throwException();
        }
        //reset 
        replaceSide = Side.NONE;
        sawKey = false;

    }
    
    /**
     * @param op
     * @throws FrontendException 
     */
    private void remove(ExpressionOperator op) throws FrontendException {
        pColConditions.add(getExpression(op));
        mPlan.trimAbove(op);
        mPlan.remove(op);
    }

    public static Expression getExpression(ExpressionOperator op) throws 
    FrontendException {
        if(op instanceof LOConst) {
            return new Expression.Const(((LOConst)op).getValue());
        } else if (op instanceof LOProject) {
            String fieldName = ((LOProject)op).getFieldSchema().alias;
            return new Expression.Column(fieldName);
        } else {
            if(!(op instanceof BinaryExpressionOperator)) {
                throwException();
            }
            BinaryExpressionOperator binOp = (BinaryExpressionOperator)op;
            if(binOp instanceof LOAdd) {
                return getExpression(binOp, OpType.OP_PLUS);
            } else if(binOp instanceof LOSubtract) {
                return getExpression(binOp, OpType.OP_MINUS);
            } else if(binOp instanceof LOMultiply) {
                return getExpression(binOp, OpType.OP_TIMES);
            } else if(binOp instanceof LODivide) {
                return getExpression(binOp, OpType.OP_DIV);
            } else if(binOp instanceof LOMod) {
                return getExpression(binOp, OpType.OP_MOD);
            } else if(binOp instanceof LOAnd) {
                return getExpression(binOp, OpType.OP_AND);
            } else if(binOp instanceof LOOr) {
                return getExpression(binOp, OpType.OP_OR);
            } else if(binOp instanceof LOEqual) {
                return getExpression(binOp, OpType.OP_EQ);
            } else if(binOp instanceof LONotEqual) {
                return getExpression(binOp, OpType.OP_NE);
            } else if(binOp instanceof LOGreaterThan) {
                return getExpression(binOp, OpType.OP_GT);
            } else if(binOp instanceof LOGreaterThanEqual) {
                return getExpression(binOp, OpType.OP_GE);
            } else if(binOp instanceof LOLesserThan) {
                return getExpression(binOp, OpType.OP_LT);
            } else if(binOp instanceof LOLesserThanEqual) {
                return getExpression(binOp, OpType.OP_LE);
            } else {
                throwException();
            }
        }
        return null;
    }
    
    private static Expression getExpression(BinaryExpressionOperator binOp, OpType 
            opType) throws FrontendException {
        return new BinaryExpression(getExpression(binOp.getLhsOperand())
                ,getExpression(binOp.getRhsOperand()), opType);
    }

    public static void throwException() throws FrontendException {
        int errCode = 2209;
        throw new FrontendException(
                "Internal error while processing any partition filter " +
                "conditions in the filter after the load" ,
                errCode,
                PigException.BUG
        );
    }
    
    // unfortunately LOVisitor today has each visit() method separately defined
    // so just implementing visit(BinaryExpressionOperator) will not result in 
    // that method being call when LOAdd (say) is encountered (sigh! - we should
    // fix that at some point) - for now, let's define visit() on each specific 
    // BinaryExpressionOperator that we want to visit to inturn call the
    // visit(BinaryExpressionOperator) method
    @Override
    public void visit(LOAdd op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LOAnd op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LODivide op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LOEqual op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LOGreaterThan op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LOGreaterThanEqual op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
  
    @Override
    public void visit(LOLesserThan op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LOLesserThanEqual op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LOMod op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LOMultiply op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LONotEqual op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LOOr op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    @Override
    public void visit(LOSubtract op) throws VisitorException {
        visit((BinaryExpressionOperator)op);
    }
    
    // this might get called from some visit() - in that case, delegate to
    // the other visit()s which we have defined here 
    @Override
    protected void visit(ExpressionOperator op) throws VisitorException {
        if(op instanceof LOProject) {
            visit((LOProject)op);
        } else if (op instanceof BinaryExpressionOperator) {
            visit((BinaryExpressionOperator)op);
        } else if (op instanceof LOCast) {
            visit((LOCast)op);
        } else if (op instanceof LOBinCond) {
            visit((LOBinCond)op);
        } else if (op instanceof LOUserFunc) {
            visit((LOUserFunc)op);
        } else if (op instanceof LOIsNull) {
            visit((LOIsNull)op);
        }
        
    }
    
    // some specific operators which are of interest to catch some
    // unsupported scenarios
    @Override
    protected void visit(LOCast cast) throws VisitorException {
        visit(cast.getExpression());
    }
    
    @Override
    public void visit(LONot not) throws VisitorException {
        visit(not.getOperand());   
    }
    
    @Override
    protected void visit(LORegexp regexp) throws VisitorException {
        visit((BinaryExpressionOperator)regexp);    
    }
    
    @Override
    protected void visit(LOBinCond binCond) throws VisitorException {
        visit(binCond.getCond());
        visit(binCond.getLhsOp());
        visit(binCond.getRhsOp());
    }
    
    @Override
    protected void visit(LOUserFunc udf) throws VisitorException {
        for (ExpressionOperator op : udf.getArguments()) {
            visit(op);
        }
    }
    
    @Override
    public void visit(LOIsNull isNull) throws VisitorException {
        visit(isNull.getOperand());
    }
}
