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
package org.apache.pig.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
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
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This class transforms LogicalPlan to its textual representation
 *
 */
public class OptimizeLimitPlanPrinter extends LogicalRelationalNodesVisitor {
    private StringBuilder sb = null ;
    
    public OptimizeLimitPlanPrinter(LogicalPlan plan) throws FrontendException {
        super( plan, new DependencyOrderWalker( plan ) );
        sb = new StringBuilder() ;
    }

    @Override
    public void visit(LOLoad load) throws FrontendException {
        appendEdges( load );
    }

    @Override
    public void visit(LOJoin join) throws FrontendException {
        appendEdges( join );
    }
    
    @Override
    public void visit(LOGenerate gen) throws FrontendException {
        appendEdges( gen );
    }
    
    @Override
    public void visit(LOInnerLoad load) throws FrontendException {
        appendEdges( load );
    }

    @Override
    public void visit(LOUnion loUnion) throws FrontendException {
        appendEdges( loUnion );
    }
    
    @Override
    public void visit(LOStream loStream) throws FrontendException {
        appendEdges( loStream );
    }
    
    @Override
    public void visit(LOFilter filter) throws FrontendException {
    	LogicalExpressionPlan expPlan = filter.getFilterPlan();
    	MyLogicalExpressionVisitor visitor = new MyLogicalExpressionVisitor( expPlan );
    	sb.append("[" + visitor.printToString() + "]" );
    	
    	appendEdges( filter );
    }
    
    @Override
    public void visit(LOStore store) throws FrontendException {
        appendEdges( store );
    }
    
    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        appendOp( foreach ) ;
        
    	boolean hasFlatten = false;
        LogicalPlan inner1 = foreach.getInnerPlan();
        Iterator<Operator> it = inner1.getOperators();
        while( it.hasNext() ) {
            Operator op = it.next();
            if( op instanceof LOGenerate ) {
                LOGenerate gen = (LOGenerate)op;
                boolean[] flattenFlags = gen.getFlattenFlags();
                if( flattenFlags != null ) {
                    for( boolean flatten : flattenFlags ) {
                        if( flatten ) {
                            hasFlatten = true;
                            break;
                        }
                    }
                }
            }
        }

    	if (hasFlatten)
    		sb.append(" [hasflat=\"true\"]");
    	else
    		sb.append(" [hasflat=\"false\"]");
        sb.append(";\n");
        appendEdges(foreach);
    }
    
    @Override
    public void visit(LOCogroup loCogroup) throws FrontendException {
    	appendEdges(loCogroup);
    }
    
    @Override
    public void visit(LOSplit loSplit) throws FrontendException {
    	appendEdges(loSplit);
    }
    
    @Override
    public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
    }
    
    @Override
    public void visit(LOSort loSort) throws FrontendException {
        appendOp(loSort) ;
        sb.append(" [limit=\"" + loSort.getLimit() + "\"]");
        sb.append(";\n");
        appendEdges(loSort);
    }
    
    @Override
    public void visit(LODistinct loDistinct) throws FrontendException {
        appendEdges(loDistinct);
    }
    
    @Override
    public void visit(LOLimit loLimit) throws FrontendException {
        appendOp(loLimit) ;
        sb.append(" [limit=\""+loLimit.getLimit()+"\"]");
        sb.append(";\n");
        appendEdges(loLimit);
    }
    
    @Override
    public void visit(LOCross loCross) throws FrontendException {
        appendEdges(loCross);
    }
    
    private void appendOp(Operator op)  {
        sb.append("    "+op.getClass().getSimpleName()) ;

    }
    
    private void appendEdges(Operator op) throws FrontendException {
        List<Operator> list = currentWalker.getPlan().getSuccessors(op) ;

		if (list!=null) {
			for(Operator tmp: list) {         
			    sb.append("    "+op.getClass().getSimpleName()+" -> ");
			    sb.append(tmp.getClass().getSimpleName()+";\n");
		    }
		}
    }
    
    /***
     * This method has to be called after the visit is totally finished only
     * @throws IOException 
     */
    public String printToString() throws IOException {
        try {
            visit() ;
        } catch(VisitorException vse) {
            throw new AssertionError("Error while transforming type graph to text") ;
        }
        return sb.toString() ;
    }
}

class MyLogicalExpressionVisitor extends LogicalExpressionVisitor {
    private StringBuilder sb = null ;
    
    public MyLogicalExpressionVisitor(LogicalExpressionPlan plan) throws FrontendException {
        super( plan, new DependencyOrderWalker( plan ) );
        sb = new StringBuilder() ;
    }

    @Override
    public void visit(AndExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(OrExpression op) throws FrontendException { 
    	appendEdges( op );
    }

    @Override
    public void visit(EqualExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(ProjectExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(ConstantExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(CastExpression op) throws FrontendException {
    	appendEdges( op );
    }

    @Override
    public void visit(GreaterThanExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(GreaterThanEqualExpression op) throws FrontendException {
    	appendEdges( op );
    }

    @Override
    public void visit(LessThanExpression op) throws FrontendException { 
    	appendEdges( op );
    }
    
    @Override
    public void visit(LessThanEqualExpression op) throws FrontendException {
    	appendEdges( op );
    }

    @Override
    public void visit(NotEqualExpression op) throws FrontendException { 
    	appendEdges( op );
    }

    @Override
    public void visit(NotExpression op ) throws FrontendException {
    	appendEdges( op );
    }

    @Override
    public void visit(IsNullExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(NegativeExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(AddExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(SubtractExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(MultiplyExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(ModExpression op) throws FrontendException {
    	appendEdges( op );
    }
    
    @Override
    public void visit(DivideExpression op) throws FrontendException {
    	appendEdges( op );
    }

    @Override
    public void visit(MapLookupExpression op) throws FrontendException {
    	appendEdges( op );
    }

    @Override
    public void visit(BinCondExpression op) throws FrontendException {        
    	appendEdges(op);
    }

    @Override
    public void visit(UserFuncExpression op) throws FrontendException {
    	appendEdges( op );
    }

    @Override
    public void visit(DereferenceExpression bagDerefenceExpression) throws FrontendException {
    	appendEdges( bagDerefenceExpression );
    }

    public void visit(RegexExpression op) throws FrontendException {
    	appendEdges( op );
    }

    private void appendEdges(Operator op) throws FrontendException {
        List<Operator> list = currentWalker.getPlan().getSuccessors(op) ;

		if (list!=null) {
			for(Operator tmp: list) {         
			    sb.append("    "+op.getClass().getSimpleName()+" -> ");
			    sb.append(tmp.getClass().getSimpleName()+";\n");
		    }
		}
    }

    public String printToString() throws FrontendException {
        try {
            visit() ;
        } catch(VisitorException vse) {
            throw new AssertionError("Error while transforming type graph to text") ;
        }
        return sb.toString() ;
    }
    
}
