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

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.parser.ParseException ;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.* ;

/**
 * This class transforms LogicalPlan to its textual representation
 *
 */
public class OpLimitOptimizerPrinter extends LOVisitor {
    
    private StringBuilder sb = null ;
    
    public OpLimitOptimizerPrinter(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        sb = new StringBuilder() ;
    }

    protected void visit(LogicalOperator op) {
    	appendEdges(op);
    }
    
    protected void visit(ExpressionOperator op) {
    	appendEdges(op);
    }
    
    protected void visit(BinaryExpressionOperator op) {
    	appendEdges(op);
    }
    
    protected void visit(UnaryExpressionOperator op) {               
    	appendEdges(op);
    }
    
    protected void visit(LOCogroup op) throws VisitorException {
    	appendEdges(op);
    }
    
    protected void visit(LOForEach op) throws VisitorException {
        appendOp(op) ;
    	boolean hasFlatten = false;
    	List<Boolean> mFlatten = op.getFlatten();

    	for (Boolean b:mFlatten)
    		if (b.equals(true)) hasFlatten = true;

    	if (hasFlatten)
    		sb.append(" [hasflat=\"true\"]");
    	else
    		sb.append(" [hasflat=\"false\"]");
        sb.append(";\n");
        appendEdges(op);
    }
    
    protected void visit(LOSort op) {
        appendOp(op) ;
        sb.append(" [limit=\""+op.getLimit()+"\"]");
        sb.append(";\n");
        appendEdges(op);
    }
    
    protected void visit(LOFilter op) throws VisitorException {
    	appendEdges(op);
    }
    
    protected void visit(LOSplit op) {
    	appendEdges(op);
    }
    
    protected void visit(LOUserFunc op) {
        appendEdges(op);
    }
    
    protected void visit(LOBinCond op) {
        appendEdges(op);
    }

    protected void visit(LOCross op) {
        appendEdges(op);
    }
    
    protected void visit(LOUnion op) {
        appendEdges(op);
    }
    
    protected void visit(LOCast op) { 
        appendEdges(op);
    }
      
    protected void visit(LOLoad op) { 
        appendEdges(op);
    }
    
    protected void visit(LOStore op) {   
        appendEdges(op);
    }
    
    protected void visit(LOConst op) {  
        appendEdges(op);
    }

    protected void visit(LORegexp op) {
        appendEdges(op);
    }

    protected void visit(LOProject op) {
        appendEdges(op);
    }

    protected void visit(LODistinct op) {
        appendEdges(op);
    }
    
    protected void visit(LOLimit op) {
        appendOp(op) ;
        sb.append(" [limit=\""+op.getLimit()+"\"]");
        sb.append(";\n");
        appendEdges(op);
    }


    private void appendOp(LogicalOperator op)  {
        sb.append("    "+op.getClass().getSimpleName()+op.getOperatorKey().getId()) ;

    }
    
    private void appendEdges(LogicalOperator op)
    {
        List<LogicalOperator> list = mCurrentWalker.getPlan().getSuccessors(op) ;

		if (list!=null) {
			for(LogicalOperator tmp: list) {         
			    sb.append("    "+op.getClass().getSimpleName()+op.getOperatorKey().getId()+" -> ");
			    sb.append(tmp.getClass().getSimpleName()+tmp.getOperatorKey().getId()+";\n");
		    }
		}
    }
    
    /***
     * This method has to be called after the visit is totally finished only
     */
    public String printToString() {
        try {
            visit() ;
        }
        catch(VisitorException vse) {
            throw new AssertionError("Error while transforming type graph to text") ;
        }
        return sb.toString() ;
    }
}
