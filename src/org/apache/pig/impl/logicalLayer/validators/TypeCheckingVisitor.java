package org.apache.pig.impl.logicalLayer.validators;

import java.util.Iterator;
import java.util.List ;
import java.util.ArrayList; 
import java.io.IOException; 

import org.apache.hadoop.mapred.lib.FieldSelectionMapReduce;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;

import org.apache.pig.impl.logicalLayer.* ;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
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
        // TODO: A lot more "else" here for all LOs

    }

    /**
     * LOConst. Type information should be associated with LOConst
     * in the parsing stage so we don't need any logic here
     */
    @Override
    protected void visit(LOConst cs) {
   
    }
    
    /**
     * For all binary operators
     */
    @Override
    protected void visit(BinaryExpressionOperator binOp)
            throws VisitorException {       
        
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;
        
        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        
        
        /**
         * For AND/OR, all operands must be boolean
         * Output is always boolean
         */
        if ( (binOp instanceof LOAnd) ||
             (binOp instanceof LOOr)  ) {
            
            if (  (lhsType != DataType.BOOLEAN)  ||
                  (rhsType != DataType.BOOLEAN)  ) {
                throw new VisitorException("Operands of AND/OR can be boolean only") ;
            }
            
            binOp.setType(DataType.BOOLEAN) ;
        } 
        
        /**
         * Mul/Div
         * 1) If both operands are numbers, that's ok.
         * 2) If one operand is number, the other can be bytearray
         * 3) ByteArray and ByteArray is OK. Output will be Double
         */
        else if ( (binOp instanceof LOMultiply) ||
                  (binOp instanceof LODivide)  ) {
                     
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
               throw new VisitorException("Cannot evaluate output type of Mul/Div Operator") ;
           }
       }        
        
        /**
         * Add/Subtract
         * 1) If both operands are numbers, that's ok.
         * 2) If one operand is number, the other can be bytearray
         * 3) ByteArray and ByteArray is OK. Output will be Double
         */
        else if ( (binOp instanceof LOAdd) ||
                  (binOp instanceof LOSubtract) ) {       
            
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
                throw new VisitorException("Cannot evaluate output type of Add/Subtract Operator") ;
            }
        }
        
        /**
         * GreaterThan/LesserThan/GreaterThanEqual/LesserThanEqual
         * 1) Both operands are numbers is ok
         * 2) CharArray and CharArray is ok
         * 3) ByteArray and ByteArray is ok
         * 4) ByteArray and (Number or CharArray) is ok
         * Output is always boolean
         */
        else if ( (binOp instanceof LOGreaterThan) ||
                  (binOp instanceof LOGreaterThanEqual) ||
                  (binOp instanceof LOLesserThan) ||
                  (binOp instanceof LOLesserThanEqual) ) {            
            
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
                throw new VisitorException("Cannot evaluate output type of Inequality Operator") ;
            }
            
            binOp.setType(DataType.BOOLEAN) ;
        }
        
        /**
         * For Equal/NotEqual
         * 1) Both operands are numbers is ok
         * 2) Both are CharArray is ok
         * 3) Both are ByteArray is ok
         * 4) ByteArray and (Number or CharArray) is ok
         * 5) Both are tuples is ok
         * 6) Both are maps is ok
         * Output is boolean
         */      
        else if ( (binOp instanceof LOEqual) ||
                  (binOp instanceof LONotEqual) ) {
            
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
                  throw new VisitorException("Cannot evaluate output type of Equal/NotEqual Operator") ;
            }
            
            binOp.setType(DataType.BOOLEAN) ;
        }
        
        /**
         * Modulo. 
         * This operator is non-commutative so the implementation 
         * doesn't have to be symmetric.
         */
        else if (binOp instanceof LOMod) {
           
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
                throw new VisitorException("Cannot evaluate output type of Mod Operator") ;
            }
        }
        
    }
     
    private void insertLeftCastForBinaryOp(BinaryExpressionOperator binOp,
                                           byte toType ) {
        OperatorKey newKey = genNewOperatorKey(binOp) ;
        LOCast cast = new LOCast(mPlan, newKey, 1, binOp.getLhsOperand(), toType) ;
        mPlan.add(cast) ;
        mPlan.disconnect(binOp.getLhsOperand(), binOp) ;
        try {
            mPlan.connect(binOp.getLhsOperand(), cast) ;
            mPlan.connect(cast, binOp) ;            
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
        OperatorKey newKey = genNewOperatorKey(binOp) ;
        LOCast cast = new LOCast(mPlan, newKey, 1, binOp.getRhsOperand(), toType) ;
        mPlan.add(cast) ;
        mPlan.disconnect(binOp.getRhsOperand(), binOp) ;
        try {
            mPlan.connect(binOp.getRhsOperand(), cast) ;
            mPlan.connect(cast, binOp) ;            
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
                throw new VisitorException("NEG can be used with numbers or Bytearray only") ;
            }
        } 
        else if (uniOp instanceof LONot) {            
            if (type == DataType.BOOLEAN) {
                uniOp.setType(DataType.BOOLEAN) ;                
            }
            else {
                throw new VisitorException("NOT can be used with boolean only") ;
            }
        }
        else {
            // undefined for this unknown unary operator
            throw new AssertionError(" Undefined type checking logic for " + uniOp.getClass()) ;
        }
            
    }
    
    private void insertCastForUniOp(UnaryExpressionOperator uniOp, byte toType) {
        List<LogicalOperator> list = mPlan.getPredecessors(uniOp) ;
        if (list==null) {
            throw new AssertionError("No input for " + uniOp.getClass()) ;
        }
        // All uniOps at the moment only work with Expression input
        ExpressionOperator input = (ExpressionOperator) list.get(0) ;                
        OperatorKey newKey = genNewOperatorKey(uniOp) ;
        LOCast cast = new LOCast(mPlan, newKey, 1, input, toType) ;
        
        mPlan.disconnect(input, uniOp) ;       
        try {
            mPlan.connect(input, cast) ;
            mPlan.connect(cast, uniOp) ;
        } 
        catch (PlanException ioe) {
            AssertionError err =  new AssertionError("Explicit casting insertion") ;
            err.initCause(ioe) ;
            throw err ;
        }      
    }
    
    // TODO: NOT DONE YET because LOUserFunc itself is not done!!!
    @Override
    protected void visit(LOUserFunc func) throws VisitorException {
        // Visit each of the arguments
        Iterator<ExpressionOperator> i = func.getArguments().iterator();
        while (i.hasNext()) {
            i.next().visit(this);
        }
    }

    /**
     * For Bincond, lhsOp and rhsOp must have the same output type
     * or both sides have to be number
     */
    @Override
    protected void visit(LOBinCond binCond) throws VisitorException {
             
        // high-level type checking
        if (binCond.getCond().getType() != DataType.BOOLEAN) {
            throw new VisitorException("Condition in BinCond must be boolean") ;
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
            throw new VisitorException("Two inputs of BinCond do not have compatible types") ;
        }
        
        // Matching schemas if we're working with tuples
        else if (lhsType == DataType.TUPLE) {            
            try {
                if (!binCond.getLhsOp().getSchema().equals(binCond.getRhsOp().getSchema())) {
                    throw new VisitorException("Two inputs of BinCond must have compatible schemas") ;
                }
                // TODO: We may have to merge the schema here
                //       if the previous check is not exact match
                //       Is Schema.reconcile good enough?
                try {
                    binCond.setSchema(binCond.getLhsOp().getSchema()) ;
                }
                catch (ParseException pe) {
                    VisitorException vse = new VisitorException("Problem during setting BinCond output schema") ;
                    vse.initCause(pe) ;
                    throw vse ;
                }
            } 
            catch (FrontendException ioe) {
                VisitorException vse = new VisitorException("Problem during evaluating BinCond output type") ;
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
            throw new VisitorException("Unsupported input type for BinCond") ;
        }

    }

    private void insertLeftCastForBinCond(LOBinCond binCond, byte toType) {
        OperatorKey newKey = genNewOperatorKey(binCond) ;
        LOCast cast = new LOCast(mPlan, newKey, 1, binCond.getLhsOp(), toType) ;
        mPlan.add(cast) ;
        mPlan.disconnect(binCond.getLhsOp(), binCond) ;
        try {
            mPlan.connect(binCond.getLhsOp(), cast) ;
            mPlan.connect(cast, binCond) ;            
        } 
        catch (PlanException ioe) {
            AssertionError err =  new AssertionError("Explicit casting insertion") ;
            err.initCause(ioe) ;
            throw err ;
        } 
        binCond.setLhsOp(cast) ;
    }

    private void insertRightCastForBinCond(LOBinCond binCond, byte toType) {
        OperatorKey newKey = genNewOperatorKey(binCond) ;
        LOCast cast = new LOCast(mPlan, newKey, 1, binCond.getRhsOp(), toType) ;
        mPlan.add(cast) ;
        mPlan.disconnect(binCond.getRhsOp(), binCond) ;
        try {
            mPlan.connect(binCond.getRhsOp(), cast) ;
            mPlan.connect(cast, binCond) ;            
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
            throw new VisitorException("Cannot cast " 
                                       + DataType.findTypeName(inputType)
                                       + " to "
                                       + DataType.findTypeName(expectedType)
                                       ) ;    
        }
        
        // cast.getType() already returns the correct type so don't have to 
        // set here. This is a special case where output type is not
        // automatically determined.
    }
    
    
    /***********************************************************************/
    /*                  Relational Operators                               */                                                             
    /***********************************************************************/

    /***
     * The schema of sort output will be the same as sort input.
     *
     */
    
    /*
    protected void visit(LOSort s) throws VisitorException {
       
        // TODO: Why doesn't LOSort have getInput() ???
        List<LogicalOperator> inputList = mPlan.getPredecessors(s) ;
        
        if (inputList.size() != 1) {
            throw new AssertionError("LOSort cannot have more than one input") ;
        }       
        
        LogicalOperator input = inputList.get(0) ;
        
        // Type checking internal plans.
        for(int i=0;i < s.getSortColPlans().size(); i++) {
            
            LogicalPlan sortColPlan = s.getSortColPlans().get(i) ;                     
            checkInnerPlan(sortColPlan, input.getSchema()) ;
            
            // Check that the inner plan has only 1 output
            if (!sortColPlan.isSingleLeafPlan()) {
                throw new VisitorException("Sort's inner plans can only have one output (leaf)") ;
            }
                       
        }
        
        s.setType(input.getType()) ;  // This should be bag always.    
        
        // I assume we can enforce schema in Bags, so this has to be done
        try { 
            s.setSchema(input.getSchema()) ;
        } 
        catch (IOException ioe) {
            VisitorException vse = new VisitorException("Problem while checking sort schema") ;
            vse.initCause(ioe) ;
            throw vse ;
        }
        // TODO: Is this ParseException applicable ?
        catch (ParseException pe) {
            VisitorException vse = new VisitorException("Problem while checking sort schema") ;
            vse.initCause(pe) ;
            throw vse ;
        }
    }
    */
    /***
     * The schema of filter output will be the same as filter input
     */
    /*
    protected void visit(LOFilter filter) throws VisitorException {
              
        LogicalOperator input = filter.getInput() ;
        LogicalPlan condPlan = filter.getConditionPlan() ;
        checkInnerPlan(condPlan, input.getSchema()) ;
        
        // Check that the inner plan has only 1 output
        if (!condPlan.isSingleLeafPlan()) {
            throw new VisitorException("Filter's cond plan can only have one output (leaf)") ;
        }
              
        // TODO: We don't have a way to getOutputType of inner plan yet!
        byte innerCondType = condPlan.getLeaves().get(0).getType() ;
        if (innerCondType != DataType.BOOLEAN) {
            throw new VisitorException("Filter's condition must be boolean") ;
        }       
        
        filter.setType(input.getType()) ; // This should be bag always.
        
        // I assume we can enforce schema in Bags, so this has to be done       
        try { 
            filter.setSchema(input.getSchema()) ;
        } 
        catch (IOException ioe) {
            VisitorException vse = new VisitorException("Problem while reconciling output schema") ;
            vse.initCause(ioe) ;
            throw vse ;
        }
        // TODO: Is this ParseException applicable ?
        catch (ParseException pe) {
            VisitorException vse = new VisitorException("Problem while reconciling output schema") ;
            vse.initCause(pe) ;
            throw vse ;
        };
        
    }
    */
    /***
     * The schema of split output will be the same as split input
     */
    /*
    protected void visit(LOSplit split) throws VisitorException {
     
        // TODO: Why doesn't LOSplit have getInput() ???
        List<LogicalOperator> inputList = mPlan.getPredecessors(split) ;
        
        if (inputList.size() != 1) {
            throw new AssertionError("LOSplit cannot have more than one input") ;
        }
        
        LogicalOperator input = inputList.get(0) ;
        
        // Checking internal plans.
        for(int i=0;i < split.getConditionPlans().size(); i++) {
            
            LogicalPlan condPlan = split.getConditionPlans().get(i) ;            
            checkInnerPlan(condPlan, input.getSchema()) ;
                  
            // Check that the inner plan has only 1 output
            if (!condPlan.isSingleLeafPlan()) {
                throw new VisitorException("Split's inner plans can only have one output (leaf)") ;
            }
            
            byte innerCondType = condPlan.getLeaves().get(0).getType() ;
            if (innerCondType != DataType.BOOLEAN) {
                throw new VisitorException("Split's conditions must be boolean") ;
            }
                       
        }         
        
        split.setType(input.getType()) ; // This should be bag always
        
        // I assume we can enforce schema in Bags, so this has to be done 
        try { 
            split.setSchema(input.getSchema()) ;
        } 
        catch (IOException ioe) {
            VisitorException vse = new VisitorException("Problem while checking sort schema") ;
            vse.initCause(ioe) ;
            throw vse ;
        }
        // TODO: Is this ParseException applicable ?
        catch (ParseException pe) {
            VisitorException vse = new VisitorException("Problem while checking sort schema") ;
            vse.initCause(pe) ;
            throw vse ;
        };
    }
    */
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
    /*
    protected void visit(LOCogroup cg) throws VisitorException {

        // Type checking internal plans.
        // TODO: Do all the GroupBy cols from all inputs have
        //       to have the same type??? I assume "no"
        for(int i=0;i < cg.getGroupByPlans().size(); i++) {
            
            LogicalPlan groupColPlan = cg.getGroupByPlans().get(i) ;
            // TODO: Question!!!, why getInputs() here not returning 
            // List<LogicalOperator> in the latest implementation ???
            LogicalOperator input = cg.getInputs().get(i) ;
            checkInnerPlan(groupColPlan, input.getSchema()) ;
            
            // Check that the inner plan has only 1 output
            // If we group by more than one fields, this should be tuple
            if (!groupColPlan.isSingleLeafPlan()) {
                throw new VisitorException("Cogroup's inner plans can only have one output (leave)") ;
            }
             
            
        }
       
        // Generate output schema
        List<FieldSchema> fsList = new ArrayList<FieldSchema>() ;
        fsList.add(new FieldSchema("group", DataType.UNKNOWN)) ;
        for(int i=0;i < cg.getInputs().size(); i++) {
            FieldSchema fs = new FieldSchema(cg.getInputs().getAlias(), DataType.BAG) ;
            fsList.add(fs) ;
            // TODO: How do we transfer Bag's Schema in this case?
        }
        
        cg.setType(DataType.BAG) ;
        cg.setSchema(new Schema(fsList)) ;
    }
    */

    // TODO: NOT DONE YET
    protected void visit(LOGenerate g) throws VisitorException {
        // Visit each of generates projection elements.
        Iterator<ExpressionOperator> i = g.getProjections().iterator();
        while (i.hasNext()) {
            i.next().visit(this);
        }
    }
    
    /***
     * This does:-
     * 1) Propagate typing information from a schema to
     *    an inner plan of a relational operator
     * 2) Type checking of the inner plan
     * NOTE: This helper method only supports one source schema
     * 
     * @param innerPlan
     * @param inputSchema
     * @throws VisitorException
     */
    /*
    private void checkInnerPlan(LogicalPlan innerPlan,
                                Schema inputSchema) 
                                    throws VisitorException {
        // Preparation
        int errorCount = 0 ;     
        List<LogicalOperator> rootList = innerPlan.getRoots() ;
        if (rootList.size() < 1) {
            throw new AssertionError("Inner plan is poorly constructed") ;
        }
        
        // Actual checking
        for(LogicalOperator op: rootList) {
            // TODO: Support map dereference
            if (op instanceof LOProject) {
                // Get the required field from input operator's schema
                // Copy type and schema (if any) to the inner plan root
                // TODO: Is this the correct way of getting a column name from LOProject ???
                FieldSchema fs = inputSchema.getField(((LOProject)op).getColumnName()) ;
                if (fs != null) {
                    op.setType(fs.type) ;
                    if (fs.type == DataType.TUPLE) {
                        try {
                            op.setSchema(fs.schema) ;
                        } 
                        catch (ParseException pe) {
                            VisitorException vse = new VisitorException(
                                "A schema from a field in input tuple cannot "
                                + " be reconciled with inner plan schema ") ;
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
            else {
                throw new AssertionError("Unsupported root operator in inner plan") ;
            }
        }
        
        // Throw an exception if we found errors
        if (errorCount > 0) {
            // TODO: Should indicate the field names or indexes here
            VisitorException vse = new VisitorException(
                    "Some required fields in inner plan cannot be found in input") ;
            throw vse ;
        }
        
        // Check typing of the inner plan by visiting it
        PlanWalker<LogicalOperator, LogicalPlan> walker
            = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(innerPlan) ;
        pushWalker(walker) ;       
        this.visit() ; 
        popWalker() ;
        
    }
       */
    
    /***
     * We need the neighbor to make sure that the new key is in the same scope
     */
    private OperatorKey genNewOperatorKey(LogicalOperator neighbor) {
        String scope = neighbor.getOperatorKey().getScope() ;
        long newId = NodeIdGenerator.getGenerator().getNextNodeId(scope) ;
        return new OperatorKey(scope, newId) ;
    }
    
    protected void visit(LOLoad load) {
        // do nothing
    }
    
    protected void visit(LOStore store) {
        // do nothing        
    }
}
