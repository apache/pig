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

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.plan.* ;

/**
 * This class transforms LogicalPlan to its textual representation
 *
 */
public class TypeGraphPrinter extends LOVisitor {
    
    private StringBuilder sb = null ;
    
    public TypeGraphPrinter(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        sb = new StringBuilder() ;
    }
    
    protected void visit(LogicalOperator op) {
        appendOp(op) ;
    }
    
    protected void visit(ExpressionOperator op) {
        appendOp(op) ;
    }
    
    protected void visit(BinaryExpressionOperator op) {
        appendOp(op) ;
    }
    
    protected void visit(UnaryExpressionOperator op) {               
        appendOp(op) ;
    }
    
    protected void visit(LOCogroup op) {
        appendOp(op) ;
    }
    
    protected void visit(LOGenerate op) {
        appendOp(op) ;
    }
    
    protected void visit(LOSort op) {
        appendOp(op) ;
    }
    
    protected void visit(LOFilter op) {
        appendOp(op) ;
    }
    
    protected void visit(LOSplit op) {
        appendOp(op) ;
    }
    
    protected void visit(LOForEach op) {
        appendOp(op) ;
    }
    
    protected void visit(LOUserFunc op) {
        appendOp(op) ;
    }
    
    protected void visit(LOBinCond op) {
        appendOp(op) ;
    }
    
    protected void visit(LOCast op) { 
        appendOp(op) ;
    }
      
    protected void visit(LOLoad op) { 
        appendOp(op) ;
    }
    
    protected void visit(LOStore op) {   
        appendOp(op) ;
    }
    
    protected void visit(LOConst op) {  
        appendOp(op) ;
    }

    private void appendOp(LogicalOperator op) {
        sb.append("(") ;
        sb.append(op.getOperatorKey().getId()) ;
        sb.append(":") ;
        sb.append(op.getClass().getSimpleName()) ;
        sb.append("=") ;
        sb.append(DataType.findTypeName(op.getType())) ;
        sb.append("{") ;
        List<LogicalOperator> list = mPlan.getSuccessors(op) ;
        if (list!=null) {
            boolean isFirst = true ;
            for(LogicalOperator tmp: list) {         
                if (!isFirst) {
                    sb.append(",") ;
                } 
                else {
                    isFirst = false ;
                }
                sb.append(tmp.getOperatorKey().getId()) ;                
            }
        }
        sb.append("}") ;
        sb.append(")") ;
        sb.append("\n") ;
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
