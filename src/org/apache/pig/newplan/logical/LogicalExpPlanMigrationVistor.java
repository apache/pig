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
package org.apache.pig.newplan.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOAdd;
import org.apache.pig.impl.logicalLayer.LOAnd;
import org.apache.pig.impl.logicalLayer.LOBinCond;
import org.apache.pig.impl.logicalLayer.LOCast;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LODivide;
import org.apache.pig.impl.logicalLayer.LOEqual;
import org.apache.pig.impl.logicalLayer.LOGreaterThan;
import org.apache.pig.impl.logicalLayer.LOGreaterThanEqual;
import org.apache.pig.impl.logicalLayer.LOIsNull;
import org.apache.pig.impl.logicalLayer.LOLesserThan;
import org.apache.pig.impl.logicalLayer.LOLesserThanEqual;
import org.apache.pig.impl.logicalLayer.LOMapLookup;
import org.apache.pig.impl.logicalLayer.LOMod;
import org.apache.pig.impl.logicalLayer.LOMultiply;
import org.apache.pig.impl.logicalLayer.LONegative;
import org.apache.pig.impl.logicalLayer.LONot;
import org.apache.pig.impl.logicalLayer.LONotEqual;
import org.apache.pig.impl.logicalLayer.LOOr;
import org.apache.pig.impl.logicalLayer.LOProject;
import org.apache.pig.impl.logicalLayer.LORegexp;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOSubtract;
import org.apache.pig.impl.logicalLayer.LOUserFunc;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

//visitor to translate expressions
public class LogicalExpPlanMigrationVistor extends LOVisitor { 
    
    protected org.apache.pig.newplan.logical.expression.LogicalExpressionPlan exprPlan;
    protected HashMap<LogicalOperator, LogicalExpression> exprOpsMap;
    protected LogicalRelationalOperator attachedRelationalOp;
    protected LogicalOperator oldAttachedRelationalOp;
    protected LogicalPlan outerPlan;
    private Map<LogicalOperator, LogicalRelationalOperator> outerOpsMap;
    
    public LogicalExpPlanMigrationVistor(LogicalPlan expressionPlan, LogicalOperator oldAttachedOperator,
            LogicalRelationalOperator attachedOperator, LogicalPlan outerPlan, 
            Map<LogicalOperator, LogicalRelationalOperator> outerOpsMap) {
        super(expressionPlan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(expressionPlan));
        exprPlan = new org.apache.pig.newplan.logical.expression.LogicalExpressionPlan();
        exprOpsMap = new HashMap<LogicalOperator, LogicalExpression>();
        attachedRelationalOp = attachedOperator;
        oldAttachedRelationalOp = oldAttachedOperator;
        this.outerPlan = outerPlan;
        this.outerOpsMap = outerOpsMap;
    }    

    private void translateConnection(LogicalOperator oldOp, org.apache.pig.newplan.Operator newOp) {       
       List<LogicalOperator> preds = mPlan.getPredecessors(oldOp);
       
       // the dependency relationship of new expression plan is opposite to the old logical plan
       // for example, a+b, in old plan, "add" is a leave, and "a" and "b" are roots
       // in new plan, "add" is root, and "a" and "b" are leaves.
       if(preds != null) {
           for(LogicalOperator pred: preds) {
               org.apache.pig.newplan.Operator newPred = exprOpsMap.get(pred);
               newOp.getPlan().connect(newOp, newPred);
           }
       }
       
       List<LogicalOperator> softPreds = mPlan.getSoftLinkPredecessors(oldOp);
       
       if(softPreds != null) {
           for(LogicalOperator softPred: softPreds) {
               org.apache.pig.newplan.Operator newSoftPred = exprOpsMap.get(softPred);
               newOp.getPlan().createSoftLink(newOp, newSoftPred);
           }
       }
   }
    
    public void visit(LOProject project) throws VisitorException {
        int col = project.getCol();
        
        LogicalExpression pe;
        if (project.getPlan().getPredecessors(project)!=null && project.getPlan().getPredecessors(project).get(0)
                instanceof LOProject) {
            List<Integer> columnNums = new ArrayList<Integer>();
            columnNums.add(col);
            pe = new DereferenceExpression(exprPlan, columnNums);
        }
        else {
            LogicalOperator lg = project.getExpression();
            int input;
            if (oldAttachedRelationalOp instanceof LOSplitOutput) {
                LOSplit split = (LOSplit)outerPlan.getPredecessors(oldAttachedRelationalOp).get(0);
                input = outerPlan.getPredecessors(split).indexOf(lg);
            }
            else {
                input = outerPlan.getPredecessors(oldAttachedRelationalOp).indexOf(lg);
            }
            pe = new ProjectExpression(exprPlan, input, project.isStar()?-1:col, attachedRelationalOp);
        }
        
        exprPlan.add(pe);
        exprOpsMap.put(project, pe);       
        translateConnection(project, pe);                       
    }
    
    public void visit(LOConst con) throws VisitorException{
        ConstantExpression ce = null;
        try {
            ce = new ConstantExpression(exprPlan, con.getValue(), Util.translateFieldSchema(con.getFieldSchema()));
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }
        
        exprPlan.add(ce);
        exprOpsMap.put(con, ce);       
        translateConnection(con, ce);
    }
    
    public void visit(LOGreaterThan op) throws VisitorException {
        ExpressionOperator left = op.getLhsOperand();
        ExpressionOperator right = op.getRhsOperand();
                
        GreaterThanExpression eq = new GreaterThanExpression
        (exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(op, eq);
    }

    public void visit(LOLesserThan op) throws VisitorException {
        ExpressionOperator left = op.getLhsOperand();
        ExpressionOperator right = op.getRhsOperand();
                
        LessThanExpression eq = new LessThanExpression
        (exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(op, eq);
    }

    public void visit(LOGreaterThanEqual op) throws VisitorException {
        ExpressionOperator left = op.getLhsOperand();
        ExpressionOperator right = op.getRhsOperand();
                
        GreaterThanEqualExpression eq = new GreaterThanEqualExpression
        (exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(op, eq);
    }

    public void visit(LOLesserThanEqual op) throws VisitorException {
        ExpressionOperator left = op.getLhsOperand();
        ExpressionOperator right = op.getRhsOperand();
                
        LessThanEqualExpression eq = new LessThanEqualExpression
        (exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(op, eq);
    }

    public void visit(LOEqual op) throws VisitorException {     
        ExpressionOperator left = op.getLhsOperand();
        ExpressionOperator right = op.getRhsOperand();
                
        EqualExpression eq = new EqualExpression(exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(op, eq);
    }

    public void visit(LOUserFunc op) throws VisitorException {
        UserFuncExpression exp = new UserFuncExpression(exprPlan, op.getFuncSpec());
        
        List<ExpressionOperator> args = op.getArguments();
        
        for( ExpressionOperator arg : args ) {
            LogicalExpression expArg = exprOpsMap.get(arg);
            exprPlan.connect(exp, expArg);
        }
        
        exprOpsMap.put(op, exp);
        // We need to track all the scalars
        if(op.getImplicitReferencedOperator() != null) {
            exp.setImplicitReferencedOperator(outerOpsMap.get(op.getImplicitReferencedOperator()));
        }

    }

    public void visit(LOBinCond op) throws VisitorException {
        ExpressionOperator condition = op.getCond();
        ExpressionOperator left = op.getLhsOp();
        ExpressionOperator right = op.getRhsOp();
        
        BinCondExpression exp = new BinCondExpression(exprPlan, 
                exprOpsMap.get(condition), exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(op, exp);
    }

    public void visit(LOCast cast) throws VisitorException {
        ExpressionOperator exp = cast.getExpression();
        
        CastExpression c = null;
        try {
            c = new CastExpression(exprPlan, exprOpsMap.get(exp), Util.translateFieldSchema(cast.getFieldSchema()));
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }
        c.setFuncSpec(cast.getLoadFuncSpec());
        exprOpsMap.put(cast, c);
    }
    
    public void visit(LORegexp binOp) throws VisitorException {
        ExpressionOperator left = binOp.getLhsOperand();
        ExpressionOperator right = binOp.getRhsOperand();
        
        RegexExpression ae = new RegexExpression(exprPlan
                , exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(binOp, ae);
    }

    public void visit(LONotEqual op) throws VisitorException {
        ExpressionOperator left = op.getLhsOperand();
        ExpressionOperator right = op.getRhsOperand();
                
        NotEqualExpression eq = new NotEqualExpression(exprPlan, 
                exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(op, eq);
    }

    public void visit(LOAdd binOp) throws VisitorException {        
        ExpressionOperator left = binOp.getLhsOperand();
        ExpressionOperator right = binOp.getRhsOperand();
        
        AddExpression ae = new AddExpression(exprPlan,
                exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(binOp, ae);
    }

    public void visit(LOSubtract binOp) throws VisitorException {
        ExpressionOperator left = binOp.getLhsOperand();
        ExpressionOperator right = binOp.getRhsOperand();
        
        SubtractExpression ae = new SubtractExpression(exprPlan, 
                exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(binOp, ae);
    }

    public void visit(LOMultiply binOp) throws VisitorException {
        ExpressionOperator left = binOp.getLhsOperand();
        ExpressionOperator right = binOp.getRhsOperand();
        
        MultiplyExpression ae = new MultiplyExpression(exprPlan,
                exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(binOp, ae);
    }

    public void visit(LODivide binOp) throws VisitorException {
        ExpressionOperator left = binOp.getLhsOperand();
        ExpressionOperator right = binOp.getRhsOperand();
        
        DivideExpression ae = new DivideExpression(exprPlan,
                exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(binOp, ae);
    }

    public void visit(LOMod binOp) throws VisitorException {
        ExpressionOperator left = binOp.getLhsOperand();
        ExpressionOperator right = binOp.getRhsOperand();
        
        ModExpression ae = new ModExpression(exprPlan, 
                exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(binOp, ae);
    }

    
    public void visit(LONegative uniOp) throws VisitorException {
        ExpressionOperator exp = uniOp.getOperand();
        NegativeExpression op = new NegativeExpression(exprPlan, exprOpsMap.get(exp));
        exprOpsMap.put(uniOp, op);
    }

    public void visit(LOMapLookup colOp) throws VisitorException {
        FieldSchema fieldSchema;
        try {
            fieldSchema = colOp.getFieldSchema();
        } catch (FrontendException e) {
            throw new VisitorException( e.getMessage() );
        }
        
        LogicalSchema.LogicalFieldSchema logfieldSchema = 
            new LogicalSchema.LogicalFieldSchema( fieldSchema.alias, 
                    Util.translateSchema(fieldSchema.schema), fieldSchema.type);
        
        LogicalExpression map = exprOpsMap.get( colOp.getMap() );
        
        MapLookupExpression op = new MapLookupExpression(exprPlan, 
                colOp.getLookUpKey(),  logfieldSchema);
        
        exprPlan.connect(op, map);
        
        exprOpsMap.put(colOp, op);
    }

    public void visit(LOAnd binOp) throws VisitorException {
        ExpressionOperator left = binOp.getLhsOperand();
        ExpressionOperator right = binOp.getRhsOperand();
                
        AndExpression ae = new AndExpression(exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(binOp, ae);            
    }

    public void visit(LOOr binOp) throws VisitorException {
        ExpressionOperator left = binOp.getLhsOperand();
        ExpressionOperator right = binOp.getRhsOperand();
                
        OrExpression ae = new OrExpression(exprPlan, exprOpsMap.get(left), exprOpsMap.get(right));
        exprOpsMap.put(binOp, ae);
    }

    public void visit(LONot uniOp) throws VisitorException {
        ExpressionOperator exp = uniOp.getOperand();
        NotExpression not = new NotExpression(exprPlan, exprOpsMap.get(exp));
        exprOpsMap.put(uniOp, not);
    }

    public void visit(LOIsNull uniOp) throws VisitorException {
        ExpressionOperator exp = uniOp.getOperand();
        IsNullExpression isNull = new IsNullExpression(exprPlan, exprOpsMap.get(exp));
        exprOpsMap.put(uniOp, isNull);
    }
}