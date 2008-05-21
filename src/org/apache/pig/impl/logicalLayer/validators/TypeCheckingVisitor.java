package org.apache.pig.impl.logicalLayer.validators;

import java.util.Iterator;
import java.util.List ;
import java.util.ArrayList;

import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LOEqual;
import org.apache.pig.impl.logicalLayer.LOGreaterThan;
import org.apache.pig.impl.logicalLayer.LOGreaterThanEqual;
import org.apache.pig.impl.logicalLayer.LOLesserThan;
import org.apache.pig.impl.logicalLayer.LOLesserThanEqual;
import org.apache.pig.impl.logicalLayer.LOMod;
import org.apache.pig.impl.logicalLayer.LONegative;
import org.apache.pig.impl.logicalLayer.LONotEqual;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;

import org.apache.pig.impl.logicalLayer.* ;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType ;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.apache.pig.impl.plan.*;
import org.apache.pig.data.DataType ;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Visitor for type checking. For simplicity of the first implementation,
 * we throw exception immediately once something doesn't look alright.
 * This is not quite smart e.g. if the plan has another unrelated branch.
 *
 */
public class TypeCheckingVisitor extends LOVisitor {

    private static final Log log = LogFactory.getLog(TypeCheckingVisitor.class);
    
    private CompilationMessageCollector msgCollector = null ;
    
    public TypeCheckingVisitor(LogicalPlan plan,
                        CompilationMessageCollector messageCollector) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        msgCollector = messageCollector ;
    }

    // Just in case caller is lazy
    @Override
    protected void visit(ExpressionOperator eOp)
                                throws VisitorException {
        if (eOp instanceof BinaryExpressionOperator) {
            visit((BinaryExpressionOperator) eOp) ;
        } 
        else if (eOp instanceof UnaryExpressionOperator) {
            visit((UnaryExpressionOperator) eOp) ;
        }
        else if (eOp instanceof LOConst) {
            visit((LOConst) eOp) ;
        }
        else if (eOp instanceof LOBinCond) {
            visit((LOBinCond) eOp) ;
        }
        else if (eOp instanceof LOCast) {
            visit((LOCast) eOp) ;
        }
        else if (eOp instanceof LORegexp) {
            visit((LORegexp) eOp) ;
        }
        else if (eOp instanceof LOUserFunc) {
            visit((LOUserFunc) eOp) ;
        }
        else if (eOp instanceof LOProject) {
            visit((LOProject) eOp) ;
        }
        else if (eOp instanceof LONegative) {
            visit((LONegative) eOp) ;
        }
        else if (eOp instanceof LONot) {
            visit((LONot) eOp) ;
        }
        // TODO: Check that all operators are included here
    }


    // Just in case caller is lazy
    @Override
    protected void visit(LogicalOperator lOp)
                                throws VisitorException {
        if (lOp instanceof LOLoad) {
            visit((LOLoad) lOp) ;
        }
        else if (lOp instanceof LODistinct) {
            visit((LODistinct) lOp) ;
        }
        else if (lOp instanceof LOFilter) {
            visit((LOFilter) lOp) ;
        }
        else if (lOp instanceof LOUnion) {
            visit((LOUnion) lOp) ;
        }
        else if (lOp instanceof LOSplit) {
            visit((LOSplit) lOp) ;
        }
        else if (lOp instanceof LOSplitOutput) {
            visit((LOSplitOutput) lOp) ;
        }
        else if (lOp instanceof LOCogroup) {
            visit((LOCogroup) lOp) ;
        }
        else if (lOp instanceof LOSort) {
            visit((LOSort) lOp) ;
        }
        // TODO: Check that all operators are included here
    }



    protected void visit(LOProject pj) throws VisitorException {
        try {
            pj.getFieldSchema() ;
        }
        catch (FrontendException fe) {
            VisitorException vse = new VisitorException("Problem in LOProject") ;
            vse.initCause(fe) ;
            throw vse ;
        }
    }

    /**
     * LOConst. Type information should be associated with LOConst
     * in the parsing stage so we don't need any logic here
     */
    @Override
    protected void visit(LOConst cs)
                        throws VisitorException {

    }

    /**
     * LORegexp expects CharArray as input
     * Itself always returns Boolean
     * @param rg
     */
    @Override
    protected void visit(LORegexp rg)
            throws VisitorException {

        // We allow BYTEARRAY to be converted to CHARARRAY
        if (rg.getOperand().getType() == DataType.BYTEARRAY)
        {
            insertCastForRegexp(rg) ;
        }

        // Other than that if it's not CharArray just say goodbye
        if (rg.getOperand().getType() != DataType.CHARARRAY)
        {
            String msg = "Operand of Regex can be CharArray only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }
    }

    private void insertCastForRegexp(LORegexp rg) {
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;
        collectCastWarning(rg, DataType.BYTEARRAY, DataType.CHARARRAY) ;
        OperatorKey newKey = genNewOperatorKey(rg) ;
        LOCast cast = new LOCast(currentPlan, newKey, rg.getOperand(), DataType.CHARARRAY) ;
        currentPlan.add(cast) ;
        currentPlan.disconnect(rg.getOperand(), rg) ;
        try {
            currentPlan.connect(rg.getOperand(), cast) ;
            currentPlan.connect(cast, rg) ;
        } 
        catch (PlanException ioe) {
            AssertionError err =  new AssertionError("Explicit casting insertion") ;
            err.initCause(ioe) ;
            throw err ;
        }
        rg.setOperand(cast) ;
    }

    @Override
    public void visit(LOAnd binOp) throws VisitorException {
    	ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;

    	byte lhsType = lhs.getType() ;
    	byte rhsType = rhs.getType() ;

    	if (  (lhsType != DataType.BOOLEAN)  ||
    			(rhsType != DataType.BOOLEAN)  ) {
    		String msg = "Operands of AND/OR can be boolean only" ;
    		msgCollector.collect(msg, MessageType.Error);
    		throw new VisitorException(msg) ;
    	}

    	binOp.setType(DataType.BOOLEAN) ;

    }
    
    @Override
    public void visit(LOOr binOp) throws VisitorException {
    	ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;

    	byte lhsType = lhs.getType() ;
    	byte rhsType = rhs.getType() ;

    	if (  (lhsType != DataType.BOOLEAN)  ||
    			(rhsType != DataType.BOOLEAN)  ) {
    		String msg = "Operands of AND/OR can be boolean only" ;
    		msgCollector.collect(msg, MessageType.Error);
    		throw new VisitorException(msg) ;
    	}

    	binOp.setType(DataType.BOOLEAN) ;
    }
    
    @Override
    public void visit(LOMultiply binOp) throws VisitorException {
    	ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
    	
    	if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
              
               // return the bigger type
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;

               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               }
               else if (rhsType != biggerType) {
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               }
               binOp.setType(biggerType) ;
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (DataType.isNumberType(rhsType)) ) {
               insertLeftCastForBinaryOp(binOp, rhsType) ;
               // Set output type
               binOp.setType(rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                   (DataType.isNumberType(lhsType)) ) {
               insertRightCastForBinaryOp(binOp, lhsType) ;
               // Set output type
               binOp.setType(lhsType) ;
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (rhsType == DataType.BYTEARRAY) ) {
               // Cast both operands to double
               insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
               insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
               // Set output type
               binOp.setType(DataType.DOUBLE) ;
           }
           else {
               String msg = "Cannot evaluate output type of Mul/Div Operator" ;
               msgCollector.collect(msg, MessageType.Error);
               throw new VisitorException(msg) ;
           }
    }
    
    @Override
    public void visit(LODivide binOp) throws VisitorException {
    	ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
    	
    	if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
              
               // return the bigger type
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;

               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               }
               else if (rhsType != biggerType) {
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               }
               binOp.setType(biggerType) ;
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (DataType.isNumberType(rhsType)) ) {
               insertLeftCastForBinaryOp(binOp, rhsType) ;
               // Set output type
               binOp.setType(rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                   (DataType.isNumberType(lhsType)) ) {
               insertRightCastForBinaryOp(binOp, lhsType) ;
               // Set output type
               binOp.setType(lhsType) ;
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (rhsType == DataType.BYTEARRAY) ) {
               // Cast both operands to double
               insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
               insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
               // Set output type
               binOp.setType(DataType.DOUBLE) ;
           }
           else {
               String msg = "Cannot evaluate output type of Mul/Div Operator" ;
               msgCollector.collect(msg, MessageType.Error);
               throw new VisitorException(msg) ;
           }
    }
    
    @Override
    public void visit(LOAdd binOp) throws VisitorException {
    	ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
    	if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
               
               // return the bigger type
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
               
               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {            
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               } 
               else if (rhsType != biggerType) { 
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               }              
               binOp.setType(biggerType) ;
           } 
           else if ( (lhsType == DataType.BYTEARRAY) &&
                   (DataType.isNumberType(rhsType)) ) {
               insertLeftCastForBinaryOp(binOp, rhsType) ;
               // Set output type
               binOp.setType(rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                 (DataType.isNumberType(lhsType)) ) {
               insertRightCastForBinaryOp(binOp, lhsType) ;
               // Set output type
               binOp.setType(lhsType) ;
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                   (rhsType == DataType.BYTEARRAY) ) {
               // Cast both operands to double
               insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
               insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
               // Set output type
               binOp.setType(DataType.DOUBLE) ;
           }
           else {
               String msg = "Cannot evaluate output type of Add/Subtract Operator" ;
               msgCollector.collect(msg, MessageType.Error);
               throw new VisitorException(msg) ;
           }
    }
    
    @Override
    public void visit(LOSubtract binOp) throws VisitorException {
    	ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
    	if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
               
               // return the bigger type
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
               
               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {            
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               } 
               else if (rhsType != biggerType) { 
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               }              
               binOp.setType(biggerType) ;
           } 
           else if ( (lhsType == DataType.BYTEARRAY) &&
                   (DataType.isNumberType(rhsType)) ) {
               insertLeftCastForBinaryOp(binOp, rhsType) ;
               // Set output type
               binOp.setType(rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                 (DataType.isNumberType(lhsType)) ) {
               insertRightCastForBinaryOp(binOp, lhsType) ;
               // Set output type
               binOp.setType(lhsType) ;
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                   (rhsType == DataType.BYTEARRAY) ) {
               // Cast both operands to double
               insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
               insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
               // Set output type
               binOp.setType(DataType.DOUBLE) ;
           }
           else {
               String msg = "Cannot evaluate output type of Add/Subtract Operator" ;
               msgCollector.collect(msg, MessageType.Error);
               throw new VisitorException(msg) ;
           }
    }
    
    
     
    @Override
	public void visit(LOGreaterThan binOp) throws VisitorException {
    	ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
    	if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
               // If not the same type, we cast them to the same
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
               
               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {            
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               } 
               else if (rhsType != biggerType) { 
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               }       
           } 
           else if ( (lhsType == DataType.CHARARRAY) &&
                     (rhsType == DataType.CHARARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (rhsType == DataType.BYTEARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     ( (rhsType == DataType.CHARARRAY) || (DataType.isNumberType(rhsType)) )
                   ) {
               // Cast byte array to the type on rhs
               insertLeftCastForBinaryOp(binOp, rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                     ( (lhsType == DataType.CHARARRAY) || (DataType.isNumberType(lhsType)) )
                   ) {
               // Cast byte array to the type on lhs
               insertRightCastForBinaryOp(binOp, lhsType) ;
           }
           else {
               throw new VisitorException("Cannot evaluate output type of "
                            + binOp.getClass().getSimpleName()
                            + " LHS:" + DataType.findTypeName(lhsType)
                            + " RHS:" + DataType.findTypeName(rhsType)) ;
           }
           
           binOp.setType(DataType.BOOLEAN) ;
	}

	@Override
	public void visit(LOGreaterThanEqual binOp) throws VisitorException {
		ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
    	if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
               // If not the same type, we cast them to the same
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
               
               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {            
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               } 
               else if (rhsType != biggerType) { 
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               }       
           } 
           else if ( (lhsType == DataType.CHARARRAY) &&
                     (rhsType == DataType.CHARARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (rhsType == DataType.BYTEARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     ( (rhsType == DataType.CHARARRAY) || (DataType.isNumberType(rhsType)) )
                   ) {
               // Cast byte array to the type on rhs
               insertLeftCastForBinaryOp(binOp, rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                     ( (lhsType == DataType.CHARARRAY) || (DataType.isNumberType(lhsType)) )
                   ) {
               // Cast byte array to the type on lhs
               insertRightCastForBinaryOp(binOp, lhsType) ;
           }
           else {
               throw new VisitorException("Cannot evaluate output type of "
                            + binOp.getClass().getSimpleName()
                            + " LHS:" + DataType.findTypeName(lhsType)
                            + " RHS:" + DataType.findTypeName(rhsType)) ;
           }
           
           binOp.setType(DataType.BOOLEAN) ;
	}

	@Override
	public void visit(LOLesserThan binOp) throws VisitorException {
		ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
    	if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
               // If not the same type, we cast them to the same
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
               
               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {            
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               } 
               else if (rhsType != biggerType) { 
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               }       
           } 
           else if ( (lhsType == DataType.CHARARRAY) &&
                     (rhsType == DataType.CHARARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (rhsType == DataType.BYTEARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     ( (rhsType == DataType.CHARARRAY) || (DataType.isNumberType(rhsType)) )
                   ) {
               // Cast byte array to the type on rhs
               insertLeftCastForBinaryOp(binOp, rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                     ( (lhsType == DataType.CHARARRAY) || (DataType.isNumberType(lhsType)) )
                   ) {
               // Cast byte array to the type on lhs
               insertRightCastForBinaryOp(binOp, lhsType) ;
           }
           else {
               throw new VisitorException("Cannot evaluate output type of "
                            + binOp.getClass().getSimpleName()
                            + " LHS:" + DataType.findTypeName(lhsType)
                            + " RHS:" + DataType.findTypeName(rhsType)) ;
           }
           
           binOp.setType(DataType.BOOLEAN) ;
	}

	@Override
	public void visit(LOLesserThanEqual binOp) throws VisitorException {
		ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
    	if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
               // If not the same type, we cast them to the same
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
               
               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {            
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               } 
               else if (rhsType != biggerType) { 
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               }       
           } 
           else if ( (lhsType == DataType.CHARARRAY) &&
                     (rhsType == DataType.CHARARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (rhsType == DataType.BYTEARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     ( (rhsType == DataType.CHARARRAY) || (DataType.isNumberType(rhsType)) )
                   ) {
               // Cast byte array to the type on rhs
               insertLeftCastForBinaryOp(binOp, rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                     ( (lhsType == DataType.CHARARRAY) || (DataType.isNumberType(lhsType)) )
                   ) {
               // Cast byte array to the type on lhs
               insertRightCastForBinaryOp(binOp, lhsType) ;
           }
           else {
               throw new VisitorException("Cannot evaluate output type of "
                            + binOp.getClass().getSimpleName()
                            + " LHS:" + DataType.findTypeName(lhsType)
                            + " RHS:" + DataType.findTypeName(rhsType)) ;
           }
           
           binOp.setType(DataType.BOOLEAN) ;
	}

	
	
	@Override
	public void visit(LOEqual binOp) throws VisitorException {
		ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
		if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
               
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
               
               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {            
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               } 
               else if (rhsType != biggerType) { 
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               } 
               
           } 
           else if ( (lhsType == DataType.CHARARRAY) &&
                     (rhsType == DataType.CHARARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (rhsType == DataType.BYTEARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     ( (rhsType == DataType.CHARARRAY) || (DataType.isNumberType(rhsType)) )
                   ) {
               // Cast byte array to the type on rhs
               insertLeftCastForBinaryOp(binOp, rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                     ( (lhsType == DataType.CHARARRAY) || (DataType.isNumberType(lhsType)) )
                   ) {
               // Cast byte array to the type on lhs
               insertRightCastForBinaryOp(binOp, lhsType) ;
           }
           else if ( (lhsType == DataType.TUPLE) &&
                     (rhsType == DataType.TUPLE) ) {
               // good
           }
           else if ( (lhsType == DataType.MAP) &&
                     (rhsType == DataType.MAP) ) {
               // good
           }
           else {
               String msg = "Cannot evaluate output type of Equal/NotEqual Operator" ;
               msgCollector.collect(msg, MessageType.Error);
               throw new VisitorException(msg) ;
           }
           
           binOp.setType(DataType.BOOLEAN) ;
	}

	@Override
	public void visit(LONotEqual binOp) throws VisitorException {
		ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
		if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
               
               byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
               
               // Cast smaller type to the bigger type
               if (lhsType != biggerType) {            
                   insertLeftCastForBinaryOp(binOp, biggerType) ;
               } 
               else if (rhsType != biggerType) { 
                   insertRightCastForBinaryOp(binOp, biggerType) ;
               } 
               
           } 
           else if ( (lhsType == DataType.CHARARRAY) &&
                     (rhsType == DataType.CHARARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     (rhsType == DataType.BYTEARRAY) ) {
               // good
           }
           else if ( (lhsType == DataType.BYTEARRAY) &&
                     ( (rhsType == DataType.CHARARRAY) || (DataType.isNumberType(rhsType)) )
                   ) {
               // Cast byte array to the type on rhs
               insertLeftCastForBinaryOp(binOp, rhsType) ;
           }
           else if ( (rhsType == DataType.BYTEARRAY) &&
                     ( (lhsType == DataType.CHARARRAY) || (DataType.isNumberType(lhsType)) )
                   ) {
               // Cast byte array to the type on lhs
               insertRightCastForBinaryOp(binOp, lhsType) ;
           }
           else if ( (lhsType == DataType.TUPLE) &&
                     (rhsType == DataType.TUPLE) ) {
               // good
           }
           else if ( (lhsType == DataType.MAP) &&
                     (rhsType == DataType.MAP) ) {
               // good
           }
           else {
               String msg = "Cannot evaluate output type of Equal/NotEqual Operator" ;
               msgCollector.collect(msg, MessageType.Error);
               throw new VisitorException(msg) ;
           }
           
           binOp.setType(DataType.BOOLEAN) ;
	}
	
	

	@Override
	public void visit(LOMod binOp) throws VisitorException {
		ExpressionOperator lhs = binOp.getLhsOperand() ;
    	ExpressionOperator rhs = binOp.getRhsOperand() ;
    	
    	byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
		if ( (lhsType == DataType.INTEGER) &&
                (rhsType == DataType.INTEGER) ) {
               binOp.setType(DataType.INTEGER) ;
           }           
           else if ( (lhsType == DataType.LONG) &&
                     ( (rhsType == DataType.INTEGER) || (rhsType == DataType.LONG) )) {
               if (rhsType == DataType.INTEGER) {
                   insertRightCastForBinaryOp(binOp, DataType.LONG) ;
               }
               binOp.setType(DataType.LONG) ;
           }            
           else if ( (lhsType == DataType.BYTEARRAY) &&
                   ( (rhsType == DataType.INTEGER) || (rhsType == DataType.LONG) )) {
               insertLeftCastForBinaryOp(binOp, rhsType) ;
               binOp.setType(rhsType) ;
           }          
           else {
               String msg = "Cannot evaluate output type of Mod Operator" ;
               msgCollector.collect(msg, MessageType.Error);
               throw new VisitorException(msg) ;
           }
	}

	private void insertLeftCastForBinaryOp(BinaryExpressionOperator binOp,
                                           byte toType ) {
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;
        collectCastWarning(binOp,
                           binOp.getLhsOperand().getType(),
                           toType) ;
        OperatorKey newKey = genNewOperatorKey(binOp) ;
        LOCast cast = new LOCast(currentPlan, newKey, binOp.getLhsOperand(), toType) ;
        currentPlan.add(cast) ;
        currentPlan.disconnect(binOp.getLhsOperand(), binOp) ;
        try {
            currentPlan.connect(binOp.getLhsOperand(), cast) ;
            currentPlan.connect(cast, binOp) ;
        } 
        catch (PlanException ioe) {
            AssertionError err =  new AssertionError("Explicit casting insertion") ;
            err.initCause(ioe) ;
            throw err ;
        } 
        binOp.setLhsOperand(cast) ;
    }
	
    private void insertRightCastForBinaryOp(BinaryExpressionOperator binOp,
                                            byte toType ) {
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;
        collectCastWarning(binOp,
                           binOp.getRhsOperand().getType(),
                           toType) ;
        OperatorKey newKey = genNewOperatorKey(binOp) ;
        LOCast cast = new LOCast(currentPlan, newKey, binOp.getRhsOperand(), toType) ;
        currentPlan.add(cast) ;
        currentPlan.disconnect(binOp.getRhsOperand(), binOp) ;
        try {
            currentPlan.connect(binOp.getRhsOperand(), cast) ;
            currentPlan.connect(cast, binOp) ;
        } 
        catch (PlanException ioe) {
            AssertionError err =  new AssertionError("Explicit casting insertion") ;
            err.initCause(ioe) ;
            throw err ;
        }               
        binOp.setRhsOperand(cast) ;
    }
    
    /** 
     * Currently, there are two unaryOps: Neg and Not. 
     */   
    @Override
    protected void visit(UnaryExpressionOperator uniOp) throws VisitorException {
        
        byte type = uniOp.getOperand().getType() ;
        
        if (uniOp instanceof LONegative) {
            if (DataType.isNumberType(type)) {
                uniOp.setType(type) ;
            }
            else if (type == DataType.BYTEARRAY) {
                insertCastForUniOp(uniOp, DataType.DOUBLE) ;
                uniOp.setType(DataType.DOUBLE) ;              
            }
            else {
                String msg = "NEG can be used with numbers or Bytearray only" ;
                msgCollector.collect(msg, MessageType.Error);
                throw new VisitorException(msg) ;
            }
        } 
        else if (uniOp instanceof LONot) {            
            if (type == DataType.BOOLEAN) {
                uniOp.setType(DataType.BOOLEAN) ;                
            }
            else {
                String msg = "NOT can be used with boolean only" ;
                msgCollector.collect(msg, MessageType.Error);
                throw new VisitorException(msg) ;
            }
        }
        else {
            // undefined for this unknown unary operator
            throw new AssertionError(" Undefined type checking logic for " + uniOp.getClass()) ;
        }
            
    }
    
    @Override
	public void visit(LONegative uniOp) throws VisitorException {
    	byte type = uniOp.getOperand().getType() ;


    	if (DataType.isNumberType(type)) {
    		uniOp.setType(type) ;
    	}
    	else if (type == DataType.BYTEARRAY) {
    		insertCastForUniOp(uniOp, DataType.DOUBLE) ;
    		uniOp.setType(DataType.DOUBLE) ;              
    	}
    	else {
    		String msg = "NEG can be used with numbers or Bytearray only" ;
    		msgCollector.collect(msg, MessageType.Error);
    		throw new VisitorException(msg) ;
    	}
         
	}
    
    @Override
    public void visit(LONot uniOp) throws VisitorException {
    	byte type = uniOp.getOperand().getType() ;
    	
    	if (type == DataType.BOOLEAN) {
            uniOp.setType(DataType.BOOLEAN) ;                
        }
        else {
            String msg = "NOT can be used with boolean only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }
    }

	private void insertCastForUniOp(UnaryExpressionOperator uniOp, byte toType) {
        collectCastWarning(uniOp,
                           uniOp.getOperand().getType(),
                           toType) ;
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;
        List<LogicalOperator> list = currentPlan.getPredecessors(uniOp) ;
        if (list==null) {
            throw new AssertionError("No input for " + uniOp.getClass()) ;
        }
        // All uniOps at the moment only work with Expression input
        ExpressionOperator input = (ExpressionOperator) list.get(0) ;                
        OperatorKey newKey = genNewOperatorKey(uniOp) ;
        LOCast cast = new LOCast(currentPlan, newKey, input, toType) ;
        
        currentPlan.disconnect(input, uniOp) ;
        try {
            currentPlan.connect(input, cast) ;
            currentPlan.connect(cast, uniOp) ;
        } 
        catch (PlanException ioe) {
            AssertionError err =  new AssertionError("Explicit casting insertion") ;
            err.initCause(ioe) ;
            throw err ;
        }

    }
    
    // Currently there is no input type information support in UserFunc
    // So we can just check if all inputs are not of any stupid type
    @Override
    protected void visit(LOUserFunc func) throws VisitorException {

        List<ExpressionOperator> list = func.getArguments() ;

        // If the dependency graph is right, all the inputs
        // must already know the types

        for(ExpressionOperator op: list) {
            if (!DataType.isUsableType(op.getType())) {
                String msg = "Problem with input of User-defined function" ;
                msgCollector.collect(msg, MessageType.Error);
                throw new VisitorException(msg) ;
            }
        }

        /*
        while (iterator.hasNext()) {
            iterator.next().visit(this);          
        }
        */
    }

    /**
     * For Bincond, lhsOp and rhsOp must have the same output type
     * or both sides have to be number
     */
    @Override
    protected void visit(LOBinCond binCond) throws VisitorException {
             
        // high-level type checking
        if (binCond.getCond().getType() != DataType.BOOLEAN) {
            String msg = "Condition in BinCond must be boolean" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }       
        
        byte lhsType = binCond.getLhsOp().getType() ;
        byte rhsType = binCond.getRhsOp().getType() ;
        
        // If both sides are number, we can convert the smaller type to the bigger type
        if (DataType.isNumberType(lhsType) && DataType.isNumberType(rhsType)) {
            byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
            if (biggerType > lhsType) {
                insertLeftCastForBinCond(binCond, biggerType) ;
            }
            else if (biggerType > rhsType) {
                insertRightCastForBinCond(binCond, biggerType) ;
            }
            binCond.setType(biggerType) ;
        }        
        else if (lhsType != rhsType) {
            String msg = "Two inputs of BinCond do not have compatible types" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }
        
        // Matching schemas if we're working with tuples
        else if (lhsType == DataType.TUPLE) {            
            try {
                if (!binCond.getLhsOp().getSchema().equals(binCond.getRhsOp().getSchema())) {
                    String msg = "Two inputs of BinCond must have compatible schemas" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new VisitorException(msg) ;
                }
                // TODO: We may have to merge the schema here
                //       if the previous check is not exact match
                //       Is Schema.reconcile good enough?
                try {
                    binCond.setSchema(binCond.getLhsOp().getSchema()) ;
                }
                catch (ParseException pe) {
                    String msg = "Problem during setting BinCond output schema" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    VisitorException vse = new VisitorException(msg) ;
                    vse.initCause(pe) ;
                    throw vse ;
                }
            } 
            catch (FrontendException ioe) {
                String msg = "Problem during evaluating BinCond output type" ;
                msgCollector.collect(msg, MessageType.Error) ;
                VisitorException vse = new VisitorException(msg) ;
                vse.initCause(ioe) ;
                throw vse ;
            }
            binCond.setType(DataType.TUPLE) ;
        }
        else if (lhsType == DataType.BYTEARRAY) {   
            binCond.setType(DataType.BYTEARRAY) ;
        }
        else if (lhsType == DataType.CHARARRAY) {   
            binCond.setType(DataType.CHARARRAY) ;
        }
        else {
            String msg = "Unsupported input type for BinCond" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }

    }

    private void insertLeftCastForBinCond(LOBinCond binCond, byte toType) {
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;

        collectCastWarning(binCond,
                           binCond.getLhsOp().getType(),
                           toType) ;

        OperatorKey newKey = genNewOperatorKey(binCond) ;
        LOCast cast = new LOCast(currentPlan, newKey, binCond.getLhsOp(), toType) ;
        currentPlan.add(cast) ;
        currentPlan.disconnect(binCond.getLhsOp(), binCond) ;
        try {
            currentPlan.connect(binCond.getLhsOp(), cast) ;
            currentPlan.connect(cast, binCond) ;
        } 
        catch (PlanException ioe) {
            AssertionError err =  new AssertionError("Explicit casting insertion") ;
            err.initCause(ioe) ;
            throw err ;
        } 
        binCond.setLhsOp(cast) ;

    }

    private void insertRightCastForBinCond(LOBinCond binCond, byte toType) {
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;

        collectCastWarning(binCond,
                           binCond.getRhsOp().getType(),
                           toType) ;

        OperatorKey newKey = genNewOperatorKey(binCond) ;
        LOCast cast = new LOCast(currentPlan, newKey, binCond.getRhsOp(), toType) ;
        currentPlan.add(cast) ;
        currentPlan.disconnect(binCond.getRhsOp(), binCond) ;
        try {
            currentPlan.connect(binCond.getRhsOp(), cast) ;
            currentPlan.connect(cast, binCond) ;
        } 
        catch (PlanException ioe) {
            AssertionError err =  new AssertionError("Explicit casting insertion") ;
            err.initCause(ioe) ;
            throw err ;
        }               
        binCond.setRhsOp(cast) ;

    }

    /**
     * 0) Casting to itself is always ok
     * 1) Casting from number to number is always ok 
     * 2) ByteArray to anything is ok
     * 3) (number or chararray) to (bytearray or chararray) is ok
     */
    @Override
    protected void visit(LOCast cast) throws VisitorException {      
        
        // TODO: Add support for tuple casting????
        if (cast.getType() == DataType.TUPLE) {
            throw new AssertionError("Tuple schema casting is not supported yet") ;
        }
        
        byte inputType = cast.getExpression().getType() ; 
        byte expectedType = cast.getType() ;     
        
        if (inputType == cast.getType()) {
            // good
        }
        else if (DataType.isNumberType(inputType) &&
            DataType.isNumberType(expectedType) ) {
            // good
        }
        else if (inputType == DataType.BYTEARRAY) {
            // good
        }
        else if (  ( DataType.isNumberType(inputType) || 
                     inputType == DataType.CHARARRAY 
                   )  &&
                   (  (expectedType == DataType.CHARARRAY) ||
                      (expectedType == DataType.BYTEARRAY)    
                   ) 
                ) {
            // good
        }
        else {
            String msg = "Cannot cast "
                           + DataType.findTypeName(inputType)
                           + " to "
                           + DataType.findTypeName(expectedType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ; 
        }
        
        // cast.getType() already returns the correct type so don't have to 
        // set here. This is a special case where output type is not
        // automatically determined.
    }
    
    
    /***********************************************************************/
    /*                  Relational Operators                               */                                                             
    /***********************************************************************/
    /*
        All the getType() of these operators always return BAG.
        We just have to :-
        1) Check types of inputs, inner plans
        2) Compute output schema with type information
           (At the moment, the parser does only return GetSchema with correct aliases)
        3) Insert casting if necessary

     */

    /*
        The output schema of LOUnion is the merge of all input schemas.
        Operands on left side always take precedance on aliases.

        We allow type promotion here
    */

    @Override
    protected void visit(LOUnion u) throws VisitorException {
        List<LogicalOperator> inputs =  u.getInputs() ;

        // There is no point to union only one operand
        // that should be a problem in the parser
        if (inputs.size() < 2) {
            AssertionError err =  new AssertionError("Union with Count(Operand) < 2") ;
        }

        try {
            Schema schema = inputs.get(0).getSchema() ;
            
            // Keep merging one by one
            for (int i=1; i< inputs.size() ;i++) {
                // Assume the first input's aliases take precedance
                schema = schema.merge(inputs.get(i).getSchema(), false) ;
                // if they cannot be merged, we just give up
                if (schema == null) {
                    String msg = "cannot merge schemas from inputs of UNION" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new VisitorException(msg) ;
                }
            }

            try {
                u.setSchema(schema);
            }
            catch (ParseException pe) {
                // This should never happen
                AssertionError err =  new AssertionError("problem with computing UNION schema") ;
                err.initCause(pe) ;
                throw err ;
            }

            // Insert casting to inputs if necessary

            for (int i=0; i< inputs.size() ;i++) {
                insertCastForEachInBetweenIfNecessary(inputs.get(i), u, schema);
            }


        }
        catch (FrontendException fee) {
            // I don't quite understand how this can happen
            // Anyway, just throw an exception to be on the safe side
            String msg = "Problem while reading schemas from inputs of UNION" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            //vse.initCause(fee) ;
            throw vse ;
        }

    }

    @Override
    protected void visit(LOSplitOutput op) throws VisitorException {
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;

        // LOSplitOutput can only have 1 input
        List<LogicalOperator> list = currentPlan.getPredecessors(op) ;
        if (list.size() != 1) {
            throw new AssertionError("LOSplitOutput can only have 1 input") ;
        }

        LogicalOperator input = list.get(0);
        LogicalPlan condPlan = op.getConditionPlan() ;

        // Check that the inner plan has only 1 output port
        if (!condPlan.isSingleLeafPlan()) {
            String msg = "Split's cond plan can only have one output (leaf)" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }
            
        checkInnerPlan(condPlan, input) ;
                 
        byte innerCondType = condPlan.getLeaves().get(0).getType() ;
        if (innerCondType != DataType.BOOLEAN) {
            String msg = "Split's condition must evaluate to boolean" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }

        op.setType(input.getType()) ; // This should be bag always

        try {
            op.setSchema(input.getSchema()) ;
        } 
        catch (ParseException pe) {
            String msg = "Problem while reading schemas from"
                         + " inputs of LOSplitOutput " ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(pe) ;
            throw vse ;
        }
        catch (FrontendException fe) {
            String msg = "Problem while reading"
                         + " schemas from inputs of LOSplitOutput" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw vse ;
        }
    }


    /***
     *  LODistinct, output schema should be the same as input
     * @param op
     * @throws VisitorException
     */
    
    @Override
    protected void visit(LODistinct op) throws VisitorException {
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;
        List<LogicalOperator> list = currentPlan.getPredecessors(op) ;
        // LOSplitOutput can only have 1 input

        try {
            op.setSchema(list.get(0).getSchema()) ;
        }
        catch (ParseException pe) {
            String msg = "Problem while reading"
                         + " schemas from inputs of LODistinct" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(pe) ;
            throw vse ;
        }
        catch (FrontendException fe) {
            String msg = "Problem while reading"
                         + " schemas from inputs of LODistinct" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw vse ;
        }
    }

    /***
     * Return concatenated of all fields from all input operators
     * @param cs
     * @throws VisitorException
     */
    protected void visit(LOCross cs) throws VisitorException {
        List<LogicalOperator> inputs = cs.getInputs() ;
        List<FieldSchema> fsList = new ArrayList<FieldSchema>() ;

        try {
            for(LogicalOperator op: inputs) {
                // All of inputs are relational operators
                // so we can access getSchema()
                Schema inputSchema = op.getSchema() ;
                for(int i=0; i < inputSchema.size(); i++) {
                    // For types other than tuple
                    if (inputSchema.getField(0).type != DataType.TUPLE) {
                        fsList.add(new FieldSchema(inputSchema.getField(0).alias,
                                                   inputSchema.getField(0).type)) ;
                    }
                    // For tuple type
                    else {
                        fsList.add(new FieldSchema(inputSchema.getField(0).alias,
                                                   inputSchema.getField(0).schema)) ;
                    }
                }
            }
        }
        catch (ParseException pe) {
            String msg = "Problem while reading"
                         + " schemas from inputs of CROSS" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(pe) ;
            throw vse ;
        }
        catch (FrontendException fe) {
            String msg = "Problem while reading"
                        + " schemas from inputs of CROSS" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw vse ;
        }

        try {
            cs.setSchema(new Schema(fsList));
        }
        catch (ParseException pe) {
            String msg = "Problem while reading"
                        + " schemas from inputs of CROSS" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(pe) ;
            throw vse ;
        }
    }
    
    /***
     * The schema of sort output will be the same as sort input.
     *
     */

    protected void visit(LOSort s) throws VisitorException {

        LogicalOperator input = s.getInput() ;
        
        // Type checking internal plans.
        for(int i=0;i < s.getSortColPlans().size(); i++) {
            
            LogicalPlan sortColPlan = s.getSortColPlans().get(i) ;

            // Check that the inner plan has only 1 output port
            if (!sortColPlan.isSingleLeafPlan()) {
                String msg = "LOSort's sort plan can only have one output (leaf)" ;
                msgCollector.collect(msg, MessageType.Error) ;
                throw new VisitorException(msg) ;
            }

            checkInnerPlan(sortColPlan, input) ;

            // TODO: May have to check SortFunc compatibility here in the future
                       
        }
        
        s.setType(input.getType()) ;  // This should be bag always.

        try {
            s.setSchema(input.getSchema()) ;
        }
        catch (FrontendException ioe) {
            String msg = "Problem while reconciling output schema of LOSort" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(ioe) ;
            throw vse ;
        }
        // TODO: Is this ParseException applicable ?
        catch (ParseException pe) {
            String msg = "Problem while reconciling output schema of LOSort" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(pe) ;
            throw vse ;
        }
    }


    /***
     * The schema of filter output will be the same as filter input
     */

    @Override
    protected void visit(LOFilter filter) throws VisitorException {

        LogicalOperator input = filter.getInput() ;
        LogicalPlan comparisonPlan = filter.getComparisonPlan() ;
        
        // Check that the inner plan has only 1 output port
        if (!comparisonPlan.isSingleLeafPlan()) {
            String msg = "Filter's cond plan can only have one output (leaf)" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }

        checkInnerPlan(comparisonPlan, input) ;
              
        byte innerCondType = comparisonPlan.getLeaves().get(0).getType() ;
        if (innerCondType != DataType.BOOLEAN) {
            String msg = "Filter's condition must evaluate to boolean" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }       
        
        filter.setType(input.getType()) ; // This should be bag always.

        try { 
            filter.setSchema(input.getSchema()) ;
        } 
        catch (FrontendException ioe) {
            String msg = "Problem while reconciling output schema of LOFilter" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(ioe) ;
            throw vse ;
        }
        // TODO: Is this ParseException applicable ?
        catch (ParseException pe) {
            String msg = "Problem while reconciling output schema of LOFilter" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(pe) ;
            throw vse ;
        }

    }

    /***
     * The schema of split output will be the same as split input
     */

    protected void visit(LOSplit split) throws VisitorException {

        // TODO: Why doesn't LOSplit have getInput() ???
        List<LogicalOperator> inputList = mPlan.getPredecessors(split) ;
        
        if (inputList.size() != 1) {
            throw new AssertionError("LOSplit cannot have more than one input") ;
        }
        
        LogicalOperator input = inputList.get(0) ;
        
        split.setType(input.getType()) ; // This should be bag always
        
        try {
            split.setSchema(input.getSchema()) ;
        }
        catch (FrontendException ioe) {
            String msg = "Problem while reconciling output schema of LOSplit" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(ioe) ;
            throw vse ;
        }
        // TODO: Is this ParseException applicable ?
        catch (ParseException pe) {
            String msg = "Problem while reconciling output schema of LOSplit" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(pe) ;
            throw vse ;
        }
    }

    /**
     * The output schema will be generated.
     */
    /*
    protected void visit(LOForEach forEach) throws VisitorException {
        // TODO: Again, should ForEach have getInput() ???
        List<LogicalOperator> inputList = mPlan.getPredecessors(forEach) ;
        
        if (inputList.size() != 1) {
            throw new AssertionError("LOForEach cannot have more than one input") ;
        }
        
        LogicalOperator input = inputList.get(0) ;
        
        // This variable for generating output schema at the end
        List<FieldSchema> fsList = new ArrayList<FieldSchema>() ;
        
        // Checking internal plans.
        for(int i=0;i < forEach.getEvaluationPlans().size(); i++) {
            
            LogicalPlan evalPlan = forEach.getEvaluationPlans().get(i) ;            
            checkInnerPlan(evalPlan, input.getSchema()) ;
            
            if (!evalPlan.isSingleLeafPlan()) {
                throw new VisitorException("Inner plans can only have one output (leaf)") ;
            }
            
            LogicalOperator leafOp = evalPlan.getLeaves().get(0) ;
            // TODO: May have to fill in field aliases here
            FieldSchema fs = new FieldSchema(null, leafOp.getType()) ; 
            if (leafOp.getType() == DataType.TUPLE) {
                fs.schema = leafOp.getSchema() ;
            }
            fsList.add(fs) ;
            
        }
        
        forEach.setType(input.getType()) ; // This should be bag always    
        forEach.setSchema(new Schema(fsList)) ;
               
    }
    */
    /**
     * COGroup
     * Still have questions about LOCOGroup internal structure
     * so this may not look quite right
     */
    protected void visit(LOCogroup cg) throws VisitorException {

        // TODO: Do all the GroupBy cols from all inputs have
        // TODO: to have the same type??? I assume "no"

        MultiMap<LogicalOperator, LogicalPlan> groupByPlans
                                                    = cg.getGroupByPlans() ;
        List<LogicalOperator> inputs = cg.getInputs() ;

        // Type checking internal plans.
        for(int i=0;i < inputs.size(); i++) {

            LogicalOperator input = inputs.get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(groupByPlans.get(input)) ;

            for(int j=0; j < innerPlans.size(); j++) {

                LogicalPlan innerPlan = innerPlans.get(j) ;
                
                // Check that the inner plan has only 1 output port
                if (!innerPlan.isSingleLeafPlan()) {
                    String msg = "COGroup's inner plans can only"
                                 + "have one output (leaf)" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new VisitorException(msg) ;
                }

                checkInnerPlan(innerPlans.get(j), input) ;
            }

        }
       
        // Generate output schema based on the schema generated from
        // COGroup itself

        try {

            Schema schema = cg.getSchema() ;

            for(int i=0; i< inputs.size(); i++) {
                FieldSchema fs = schema.getField(i+1) ;
                fs.type = DataType.BAG ;
                fs.schema = inputs.get(i).getSchema() ;
            }

            cg.setType(DataType.BAG) ;
            cg.setSchema(schema) ;

        }
        catch (FrontendException fe) {
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw vse ;
        }
        catch (ParseException pe) {
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(pe) ;
            throw vse ;
        }
        
    }

    /***
     * Output schema of LOGenerate is a tuple schma
     * which is the output of all inner plans
     *
     * Flatten also has to be taken care on in here
     *
     */

    protected void visit(LOGenerate g) throws VisitorException {
        List<LogicalPlan> plans = g.getGeneratePlans() ;
        List<Boolean> flattens = g.getFlatten() ;

        /*
        for(int i=0;i < plans.size(); i++) {

            LogicalPlan plan = plans.get(i) ;

            // Check that the inner plan has only 1 output port
            if (!plan.isSingleLeafPlan()) {
                String msg = "Generate's expression plan can "
                             + " only have one output (leaf)" ;
                msgCollector.collect(msg, MessageType.Error) ;
                throw new VisitorException(msg) ;
            }

            checkInnerPlan(plan, ) ;

            byte innerCondType = condPlan.getLeaves().get(0).getType() ;
            if (innerCondType != DataType.BOOLEAN) {
                String msg = "Split's condition must evaluate to boolean" ;
                msgCollector.collect(msg, MessageType.Error) ;
                throw new VisitorException(msg) ;
            }

        }
        */

    }
    
    /***
     * This does:-
     * 1) Propagate typing information from a schema to
     *    an inner plan of a relational operator
     * 2) Type checking of the inner plan
     * NOTE: This helper method only supports one source schema
     * 
     * @param innerPlan  the inner plan
     * @param srcOuterOp the source data operator
     * @throws VisitorException
     */

    private void checkInnerPlan(LogicalPlan innerPlan,
                                LogicalOperator srcOuterOp)
                                    throws VisitorException {
        // Preparation
        int errorCount = 0 ;     
        List<LogicalOperator> rootList = innerPlan.getRoots() ;
        if (rootList.size() < 1) {
            throw new AssertionError("Inner plan is poorly constructed") ;
        }

        Schema inputSchema = null ;
        try {
            inputSchema = srcOuterOp.getSchema() ;
        }
        catch(FrontendException fe) {
            String msg = "Cannot not get schema out of "
                         + srcOuterOp.getClass().getSimpleName() ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }

        // Actual checking
        for(LogicalOperator op: rootList) {
            // TODO: Support map dereference
            if (op instanceof LOProject) {
                // Get the required field from input operator's schema
                // Copy type and schema (if any) to the inner plan root
                LOProject project = (LOProject)op ;

                FieldSchema fs = null ;
                try {
                    fs = inputSchema.getField(project.getCol()) ;
                }
                catch(ParseException pe) {
                    String msg = "Cannot not get schema out of "
                                 + srcOuterOp.getClass().getSimpleName() ;
                    msgCollector.collect(msg, MessageType.Error);
                    VisitorException vse = new VisitorException(msg) ;
                    vse.initCause(pe) ;
                    throw vse ;
                }

                if (fs != null) {
                    op.setType(fs.type) ;
                    if (fs.type == DataType.TUPLE) {
                        try {
                            op.setSchema(fs.schema) ;
                        } 
                        catch (ParseException pe) {
                            String msg = "A schema from a field in input tuple cannot "
                                         + " be reconciled with inner plan schema " ;
                            msgCollector.collect(msg, MessageType.Error);
                            VisitorException vse = new VisitorException(msg) ;
                            vse.initCause(pe) ;
                            throw vse ;
                        }
                    }
                    // TODO: Support for bag schema?????
                }
                else {
                    errorCount++ ;
                }
            }
            else if (op instanceof LOConst) {
                // don't have to do anything
            }
            else {
                throw new AssertionError("Unsupported root operator in inner plan") ;
            }
        }
        
        // Throw an exception if we found errors
        if (errorCount > 0) {
            // TODO: Should indicate the field names or indexes here
            String msg =  "Some required fields in inner plan cannot be found in input" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            throw vse ;
        }
        
        // Check typing of the inner plan by visiting it
        PlanWalker<LogicalOperator, LogicalPlan> walker
            = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(innerPlan) ;
        pushWalker(walker) ;       
        this.visit() ; 
        popWalker() ;
        
    }
       

    /***
     * For casting insertion for relational operators
     * only if it's necessary
     * Currently this only does "shallow" casting
     * @param fromOp
     * @param toOp
     * @param targetSchema array of target types
     */
    private void insertCastForEachInBetweenIfNecessary(LogicalOperator fromOp,
                                                       LogicalOperator toOp,
                                                       Schema targetSchema)
                                                    throws VisitorException {
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;

        // Make sure that two operators are in the same plan
        if (fromOp.getPlan() != toOp.getPlan()) {
            throw new AssertionError("Two operators have toOp be in the same plan") ;
        }
        // Mare sure that they are in the plan we're looking at
        if (fromOp.getPlan() != toOp.getPlan()) {
            throw new AssertionError("Cannot manipulate any other plan"
                                    +" than the current one") ;
        }

        // Make sure that they are adjacent and the direction
        // is from "fromOp" to "toOp"
        List<LogicalOperator> preList = currentPlan.getPredecessors(toOp) ;
        boolean found = false ;
        for(LogicalOperator tmpOp: preList) {
            // compare by reference
            if (tmpOp == fromOp) {
                found = true ;
                break ;
            }
        }

        if (!found) {
            throw new AssertionError("Two operators are not adjacent") ;
        }

        // retrieve input schema to be casted
        // this will be used later
        Schema fromSchema = null ;
        try {
            fromSchema = fromOp.getSchema() ;
        }
        catch(FrontendException fe) {
            AssertionError err =  new AssertionError("Cannot get schema from"
                                                     + " input operator") ;
            err.initCause(fe) ;
            throw err ;
        }

        // make sure the supplied targetSchema has the same number of members
        // as number of output fields from "fromOp"
        if (fromSchema.size() != targetSchema.size()) {
            throw new AssertionError("Invalid input parameters in cast insert") ;
        }

        // Compose the new inner plan to be used in ForEach
        LogicalPlan foreachPlan = new LogicalPlan() ;

        // Plans inside Generate. Fields that do not need casting will only
        // have Project. Fields that need casting will have Project + Cast
        ArrayList<LogicalPlan> generatePlans = new ArrayList<LogicalPlan>() ;

        int castNeededCounter = 0 ;
        for(int i=0;i < fromSchema.size(); i++) {

            LogicalPlan genPlan = new LogicalPlan() ;
            // TODO: should the LogicalOp here be null in this case?
            LOProject project = new LOProject(genPlan,
                                                 genNewOperatorKey(fromOp),
                                                 null,
                                                 i) ;
            genPlan.add(project);

            // add casting if necessary by comparing target types
            // to the input schema
            FieldSchema fs = null ;
            try {
                fs = fromSchema.getField(i) ;
            }
            catch(ParseException pe) {
                String msg = "Problem while reading"
                                + " field schema from input while"
                                + " insert casting " ;
                msgCollector.collect(msg, MessageType.Error) ;
                VisitorException vse = new VisitorException(msg) ;
                vse.initCause(pe) ;
                throw vse ;
            }

            // This only does "shallow checking"

            byte inputFieldType ;

            try {
                inputFieldType = targetSchema.getField(i).type ;
            }
            catch (ParseException e) {
                throw new AssertionError("Cannot get field type") ;
            }

            if (inputFieldType != fs.type) {
                castNeededCounter++ ;
                LOCast cast = new LOCast(genPlan,
                                         genNewOperatorKey(fromOp),
                                         project,
                                         inputFieldType) ;
                genPlan.add(cast) ;
                try {
                    genPlan.connect(project, cast);
                }
                catch (PlanException pe) {
                    // This should never happen
                    throw new AssertionError("unpected plan exception while insert casting") ;
                }
            }

            generatePlans.add(genPlan) ;

        }

        // if we really need casting
        if (castNeededCounter > 0)  {
            // Flatten List
            // This is just cast insertion so we don't have any flatten
            ArrayList<Boolean> flattenList = new ArrayList<Boolean>() ;
            for(int i=0;i < targetSchema.size(); i++) {
                flattenList.add(new Boolean(false)) ;
            }

            LOGenerate generate = new LOGenerate(currentPlan, genNewOperatorKey(fromOp),
                                                 generatePlans, flattenList) ;

            foreachPlan.add(generate) ;

            // Create ForEach to be inserted
            LOForEach foreach = new LOForEach(currentPlan, genNewOperatorKey(fromOp), foreachPlan) ;

            // Manipulate the plan structure
            currentPlan.add(foreach);
            currentPlan.disconnect(fromOp, toOp) ;

            try {
                currentPlan.connect(fromOp, foreach);
                currentPlan.connect(foreach, toOp);
            }
            catch (PlanException pe) {
                AssertionError err = new AssertionError("Problem wiring the plan while insert casting") ;
                err.initCause(pe) ;
                throw err ;
            }
        }
        else {
            log.debug("Tried to insert relational casting when not necessary");
        }
    }

    /***
     * Helper for collecting warning when casting is inserted
     * to the plan (implicit casting)
     *
     * @param op
     * @param originalType
     * @param toType
     */
    private void collectCastWarning(LogicalOperator op,
                                   byte originalType,
                                   byte toType) {
        String originalTypeName = DataType.findTypeName(originalType) ;
        String toTypeName = DataType.findTypeName(toType) ;
        String opName = op.getClass().getSimpleName() ;
        msgCollector.collect(originalTypeName + " is implicitly casted to "
                             + toTypeName +" under " + opName + " Operator",
                             MessageType.Warning) ;
    }

    /***
     * We need the neighbor to make sure that the new key is in the same scope
     */
    private OperatorKey genNewOperatorKey(LogicalOperator neighbor) {
        String scope = neighbor.getOperatorKey().getScope() ;
        long newId = NodeIdGenerator.getGenerator().getNextNodeId(scope) ;
        return new OperatorKey(scope, newId) ;
    }
    
    protected void visit(LOLoad load)
                        throws VisitorException {
        // do nothing
    }
    
    protected void visit(LOStore store) {
        // do nothing        
    }
}
