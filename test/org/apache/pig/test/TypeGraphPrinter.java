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
