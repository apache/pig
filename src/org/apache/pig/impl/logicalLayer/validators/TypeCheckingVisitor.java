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

package org.apache.pig.impl.logicalLayer.validators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.HashSet;
import java.util.TreeMap;


import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LOUserFunc;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;

import org.apache.pig.impl.logicalLayer.* ;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType ;
import org.apache.pig.impl.plan.*;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.Handle;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Visitor for type checking. For simplicity of the first implementation,
 * we throw exception immediately once something doesn't look alright.
 * This is not quite smart e.g. if the plan has another unrelated branch.
 *
 */
public class TypeCheckingVisitor extends LOVisitor {

    private static final int INF = -1;

    private static final Log log = LogFactory.getLog(TypeCheckingVisitor.class);

    private CompilationMessageCollector msgCollector = null ;

    private boolean strictMode = false ;
    
    public static MultiMap<Byte, Byte> castLookup = new MultiMap<Byte, Byte>();
    static{
        //Ordering here decides the score for the best fit function.
        //Do not change the order. Conversions to a smaller type is preferred
        //over conversion to a bigger type where ordering of types is:
        //INTEGER, LONG, FLOAT, DOUBLE, CHARARRAY, TUPLE, BAG, MAP
        //from small to big
//        castLookup.put(DataType.BOOLEAN, DataType.INTEGER);
//        castLookup.put(DataType.BOOLEAN, DataType.LONG);
//        castLookup.put(DataType.BOOLEAN, DataType.FLOAT);
//        castLookup.put(DataType.BOOLEAN, DataType.DOUBLE);
//        castLookup.put(DataType.BOOLEAN, DataType.CHARARRAY);
        castLookup.put(DataType.INTEGER, DataType.LONG);
        castLookup.put(DataType.INTEGER, DataType.FLOAT);
        castLookup.put(DataType.INTEGER, DataType.DOUBLE);
//        castLookup.put(DataType.INTEGER, DataType.CHARARRAY);
        castLookup.put(DataType.LONG, DataType.FLOAT);
        castLookup.put(DataType.LONG, DataType.DOUBLE);
//        castLookup.put(DataType.LONG, DataType.CHARARRAY);
        castLookup.put(DataType.FLOAT, DataType.DOUBLE);
//        castLookup.put(DataType.FLOAT, DataType.CHARARRAY);
//        castLookup.put(DataType.DOUBLE, DataType.CHARARRAY);
//        castLookup.put(DataType.BYTEARRAY, DataType.BOOLEAN);
        castLookup.put(DataType.BYTEARRAY, DataType.INTEGER);
        castLookup.put(DataType.BYTEARRAY, DataType.LONG);
        castLookup.put(DataType.BYTEARRAY, DataType.FLOAT);
        castLookup.put(DataType.BYTEARRAY, DataType.DOUBLE);
        castLookup.put(DataType.BYTEARRAY, DataType.CHARARRAY);
        castLookup.put(DataType.BYTEARRAY, DataType.TUPLE);
        castLookup.put(DataType.BYTEARRAY, DataType.BAG);
        castLookup.put(DataType.BYTEARRAY, DataType.MAP);
    }

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
        else if (eOp instanceof LOMapLookup) {
            visit((LOMapLookup) eOp) ;
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
        else if (lOp instanceof LOForEach) {
            visit((LOForEach) lOp) ;
        }
        else if (lOp instanceof LOGenerate) {
            visit((LOGenerate) lOp) ;
        }
        else if (lOp instanceof LOCross) {
            visit((LOCross) lOp) ;
        }
        // TODO: Check that all operators are included here
    }



    protected void visit(LOProject pj) throws VisitorException {
        resolveLOProjectType(pj) ;
    }

    private void resolveLOProjectType(LOProject pj) throws VisitorException {

        try {
            pj.getFieldSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1035;
            String msg = "Error getting LOProject's input schema" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
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

    @Override
    public void visit(LOMapLookup map)
                        throws VisitorException {
        if(!DataType.isAtomic(DataType.findType(map.getLookUpKey()))) {
            int errCode = 1036;
            String msg = "Map key should be a basic type" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

        map.setType(map.getValueType());

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
            int errCode = 1037;
            String msg = "Operand of Regex can be CharArray only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }
    }

    private void insertCastForRegexp(LORegexp rg) throws VisitorException {
        insertCast(rg, DataType.CHARARRAY, rg.getOperand());
    }

    public void visit(LOAnd binOp) throws VisitorException {
        // if lhs or rhs is null constant then cast it to boolean
        insertCastsForNullToBoolean(binOp);
        ExpressionOperator lhs = binOp.getLhsOperand();
        ExpressionOperator rhs = binOp.getRhsOperand();

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);

        if (  (lhsType != DataType.BOOLEAN)  ||
              (rhsType != DataType.BOOLEAN)  ) {
            int errCode = 1038;
            String msg = "Operands of AND/OR can be boolean only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

    }

    /**
     * @param binOp
     * @throws VisitorException
     */
    private void insertCastsForNullToBoolean(BinaryExpressionOperator binOp)
            throws VisitorException {
        if (binOp.getLhsOperand() instanceof LOConst
                && ((LOConst) binOp.getLhsOperand()).getValue() == null)
            insertLeftCastForBinaryOp(binOp, DataType.BOOLEAN);
        if (binOp.getRhsOperand() instanceof LOConst
                && ((LOConst) binOp.getRhsOperand()).getValue() == null)
            insertRightCastForBinaryOp(binOp, DataType.BOOLEAN);
    }

    @Override
    public void visit(LOOr binOp) throws VisitorException {
        // if lhs or rhs is null constant then cast it to boolean
        insertCastsForNullToBoolean(binOp);
        ExpressionOperator lhs = binOp.getLhsOperand();
        ExpressionOperator rhs = binOp.getRhsOperand();

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);

        if (  (lhsType != DataType.BOOLEAN)  ||
              (rhsType != DataType.BOOLEAN)  ) {
            int errCode = 1038;
            String msg = "Operands of AND/OR can be boolean only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

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
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(rhsType)) ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(lhsType)) ) {
            insertRightCastForBinaryOp(binOp, lhsType) ;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (rhsType == DataType.BYTEARRAY) ) {
            // Cast both operands to double
            insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
            insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
        }
        else {
            int errCode = 1039;
            String msg = "Incompatible types in Multiplication Operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

        try {
            binOp.regenerateFieldSchema();
        } catch (FrontendException fe) {
            int errCode = 1040;
            String msg = "Could not set Multiply field schema";
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
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
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(rhsType)) ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(lhsType)) ) {
            insertRightCastForBinaryOp(binOp, lhsType) ;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (rhsType == DataType.BYTEARRAY) ) {
            // Cast both operands to double
            insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
            insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
        }
        else {
            int errCode = 1039;
            String msg = "Incompatible types in Division Operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

        try {
            binOp.regenerateFieldSchema();
        } catch (FrontendException fe) {
            int errCode = 1040;
            String msg = "Could not set Divide field schema";
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
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
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(rhsType)) ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(lhsType)) ) {
            insertRightCastForBinaryOp(binOp, lhsType) ;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (rhsType == DataType.BYTEARRAY) ) {
            // Cast both operands to double
            insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
            insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
        }
        else {
            int errCode = 1039;
            String msg = "Incompatible types in Add Operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }
        try {
            binOp.regenerateFieldSchema();
        } catch (FrontendException fe) {
            int errCode = 1040;
            String msg = "Could not set Add field schema";
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
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
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                (DataType.isNumberType(rhsType)) ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(lhsType)) ) {
            insertRightCastForBinaryOp(binOp, lhsType) ;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (rhsType == DataType.BYTEARRAY) ) {
            // Cast both operands to double
            insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
            insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
        }
        else {
            int errCode = 1039;
            String msg = "Incompatible types in Subtract Operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }
        try {
            binOp.regenerateFieldSchema();
        } catch (FrontendException fe) {
            int errCode = 1040;
            String msg = "Could not set Subtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
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
            int errCode = 1039;
            String msg = "Incompatible types in GreaterThan operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

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
            int errCode = 1039;
            String msg = "Incompatible types in GreaterThanEqualTo operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

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
            int errCode = 1039;
            String msg = "Incompatible types in LesserThan operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

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
            int errCode = 1039;
            String msg = "Incompatible types in LesserThanEqualTo operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

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
        // A constant null is always bytearray - so cast it
        // to rhs type
        else if (binOp.getLhsOperand() instanceof LOConst
                && ((LOConst) binOp.getLhsOperand()).getValue() == null) {
            insertLeftCastForBinaryOp(binOp, rhsType);
        } else if (binOp.getRhsOperand() instanceof LOConst
                && ((LOConst) binOp.getRhsOperand()).getValue() == null) {
            insertRightCastForBinaryOp(binOp, lhsType);
        } else {
            int errCode = 1039;
            String msg = "Incompatible types in EqualTo Operator"
                    + " left hand side:" + DataType.findTypeName(lhsType) + " right hand side:"
                    + DataType.findTypeName(rhsType);
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

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
        // A constant null is always bytearray - so cast it
        // to rhs type
        else if (binOp.getLhsOperand() instanceof LOConst
                && ((LOConst) binOp.getLhsOperand()).getValue() == null) {
            insertLeftCastForBinaryOp(binOp, rhsType);
        } else if (binOp.getRhsOperand() instanceof LOConst
                && ((LOConst) binOp.getRhsOperand()).getValue() == null) {
            insertRightCastForBinaryOp(binOp, lhsType);
        } else {
            int errCode = 1039;
            String msg = "Incompatible types in NotEqual Operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

    }

    @Override
    public void visit(LOMod binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;

        if ( (lhsType == DataType.INTEGER) &&
             (rhsType == DataType.INTEGER)
           ) {
           //do nothing
        }
        else if ( (lhsType == DataType.LONG) &&
                  ( (rhsType == DataType.INTEGER) || (rhsType == DataType.LONG) )
                ) {
            if (rhsType == DataType.INTEGER) {
                insertRightCastForBinaryOp(binOp, DataType.LONG) ;
            }
        }
        else if ( (rhsType == DataType.LONG) &&
                  ( (lhsType == DataType.INTEGER) || (lhsType == DataType.LONG) )
                ) {
            if (lhsType == DataType.INTEGER) {
                insertLeftCastForBinaryOp(binOp, DataType.LONG) ;
            }
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  ( (rhsType == DataType.INTEGER) || (rhsType == DataType.LONG) )
                ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
        }
        else {
            int errCode = 1039;
            String msg = "Incompatible types in Mod Operator"
                            + " left hand side:" + DataType.findTypeName(lhsType)
                            + " right hand side:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }
        try {
            binOp.regenerateFieldSchema();
        } catch (FrontendException fe) {
            int errCode = 1040;
            String msg = "Could not set Mod field schema";
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }


    @Override
    public void visit(LONegative uniOp) throws VisitorException {
        byte type = uniOp.getOperand().getType() ;


        if (DataType.isNumberType(type)) {
            //do nothing
        }
        else if (type == DataType.BYTEARRAY) {
            insertCastForUniOp(uniOp, DataType.DOUBLE) ;
        }
        else {
            int errCode = 1041;
            String msg = "NEG can be used with numbers or Bytearray only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

        try {
            uniOp.regenerateFieldSchema();
        } catch (FrontendException fe) {
            int errCode = 1040;
            String msg = "Could not set Negative field schema";
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }
    
    @Override
    public void visit(LONot uniOp) throws VisitorException {
        if (uniOp.getOperand() instanceof LOConst
                && ((LOConst) uniOp.getOperand()).getValue() == null) {
            insertCastForUniOp(uniOp, DataType.BOOLEAN);
        }
        byte type = uniOp.getOperand().getType();
        if (type != DataType.BOOLEAN) {
            int errCode = 1042;
            String msg = "NOT can be used with boolean only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

    }

    @Override
    public void visit(LOIsNull uniOp) throws VisitorException {
    }

    private void insertLeftCastForBinaryOp(BinaryExpressionOperator binOp,
                                           byte toType ) throws VisitorException {
        insertCast(binOp, toType, binOp.getLhsOperand());
    }

    private void insertRightCastForBinaryOp(BinaryExpressionOperator binOp,
                                            byte toType ) throws VisitorException {
        insertCast(binOp, toType, binOp.getRhsOperand());
    }


    private void insertCast(ExpressionOperator node,
                            byte toType, ExpressionOperator predecessor) 
    throws VisitorException {
        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;
        collectCastWarning(node, predecessor.getType(), toType);

        OperatorKey newKey = genNewOperatorKey(node);
        LOCast cast = new LOCast(currentPlan, newKey, toType) ;
        currentPlan.add(cast) ;
        try {
            currentPlan.insertBetween(predecessor, cast, node);
        }
        catch (PlanException pe) {
            int errCode = 2059;
            String msg = "Problem with inserting cast operator for " + node + " in plan.";
            throw new TypeCheckerException(msg, errCode, PigException.BUG, pe);
        }
        this.visit(cast);
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
                int errCode = 1041;
                String msg = "NEG can be used with numbers or Bytearray only" ;
                msgCollector.collect(msg, MessageType.Error);
                throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
            }
        }
        else if (uniOp instanceof LONot) {
            if (type == DataType.BOOLEAN) {
                uniOp.setType(DataType.BOOLEAN) ;
            }
            else {
                int errCode = 1042;
                String msg = "NOT can be used with boolean only" ;
                msgCollector.collect(msg, MessageType.Error);
                throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
            }
        }
        else {
            // undefined for this unknown unary operator
            int errCode = 1079;
            String msg = "Undefined type checking logic for unary operator: " + uniOp.getClass().getSimpleName();
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

    }

    private void insertCastForUniOp(UnaryExpressionOperator uniOp, byte toType) throws VisitorException {
        insertCast(uniOp, toType, uniOp.getOperand());
    }
    
    // Currently there is no input type information support in UserFunc
    // So we can just check if all inputs are not of any stupid type
    @Override
    protected void visit(LOUserFunc func) throws VisitorException {

        List<ExpressionOperator> list = func.getArguments() ;

        // If the dependency graph is right, all the inputs
        // must already know the types
        Schema s = new Schema();
        for(ExpressionOperator op: list) {
            if (!DataType.isUsableType(op.getType())) {
                int errCode = 1014;
                String msg = "Problem with input " + op + " of User-defined function: " + func;
                msgCollector.collect(msg, MessageType.Error);
                throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
            }
            try {
                s.add(op.getFieldSchema());    
            } catch (FrontendException e) {
                int errCode = 1043;
                String msg = "Unable to retrieve field schema.";
                throw new TypeCheckerException(msg, errCode, PigException.INPUT, e);
            }
            
        }

        EvalFunc<?> ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(func.getFuncSpec());

        // If the function is algebraic and the project is just sentinel
        // (special case when we apply aggregate on flattened members)
        // then it will never match algebraic functions' schemas
        // without this

        // Assuming all aggregates has only one argument at this stage
        if(func.getArguments()!=null && func.getArguments().size()>0){
            ExpressionOperator tmpExp = func.getArguments().get(0) ;
            if ( (ef instanceof Algebraic)
                 && (tmpExp instanceof LOProject)
                 && (((LOProject)tmpExp).getSentinel())) {
    
                FieldSchema tmpField ;
    
                try {
                    // embed the schema above inside a bag
                    tmpField = new FieldSchema(null, s, DataType.BAG) ;
                }
                catch (FrontendException e) {
                    int errCode = 1023;
                    String msg = "Unable to create new field schema.";
                    throw new TypeCheckerException(msg, errCode, PigException.INPUT, e) ;
                }
    
                s = new Schema(tmpField) ;
            }
        }
        
        // ask the EvalFunc what types of inputs it can handle
        List<FuncSpec> funcSpecs = null;
        try {
            funcSpecs = ef.getArgToFuncMapping();    
        } catch (Exception e) {
            int errCode = 1044;
            String msg = "Unable to get list of overloaded methods.";
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, e);
        }
        
        /**
         * Here is an explanation of the way the matching UDF funcspec will be chosen
         * based on actual types in the input schema.
         * First an "exact" match is tried for each of the fields in the input schema
         * with the corresponding fields in the candidate funcspecs' schemas. 
         * 
         * If exact match fails, then first a check if made if the input schema has any
         * bytearrays in it. 
         * 
         * If there are NO bytearrays in the input schema, then a best fit match is attempted
         * for the different fields. Essential a permissible cast from one type to another
         * is given a "score" based on its position in the "castLookup" table. A final
         * score for a candidate funcspec is deduced as  
         *               SUM(score_of_particular_cast*noOfCastsSoFar). 
         * If no permissible casts are possible, the score for the candidate is -1. Among 
         * the non -1 score candidates, the candidate with the lowest score is chosen. 
         * 
         * If there are bytearrays in the input schema, a modified exact match is tried. In this
         * matching, bytearrays in the input schema are not considered. As a result of
         * ignoring the bytearrays, we could get multiple candidate funcspecs which match
         * "exactly" for the other columns - if this is the case, we notify the user of
         * the ambiguity and error out. Else if all other (non byte array) fields 
         * matched exactly, then we can cast bytearray(s) to the corresponding type(s)
         * in the matched udf schema. If this modified exact match fails, the above best fit 
         * algorithm is attempted by initially coming up with scores and candidate funcSpecs 
         * (with bytearray(s) being ignored in the scoring process). Then a check is 
         * made to ensure that the positions which have bytearrays in the input schema
         * have the same type (for a given position) in the corresponding positions in
         * all the candidate funcSpecs. If this is not the case, it indicates a conflict
         * and the user is notified of the error (because we have more than
         * one choice for the destination type of the cast for the bytearray). If this is the case,
         * the candidate with the lowest score is chosen. 
         */
        
        
        
        FuncSpec matchingSpec = null;
        boolean notExactMatch = false;
        if(funcSpecs!=null && funcSpecs.size()!=0){
            //Some function mappings found. Trying to see
            //if one of them fits the input schema
            if((matchingSpec = exactMatch(funcSpecs, s, func))==null){
                //Oops, no exact match found. Trying to see if we
                //have mappings that we can fit using casts.
                notExactMatch = true;
                if(byteArrayFound(s)){
                    // try "exact" matching all other fields except the byte array 
                    // fields and if they all exact match and we have only one candidate
                    // for the byte array cast then that's the matching one!
                    if((matchingSpec = exactMatchWithByteArrays(funcSpecs, s, func))==null){
                        // "exact" match with byte arrays did not work - try best fit match
                        if((matchingSpec = bestFitMatchWithByteArrays(funcSpecs, s, func)) == null) {
                            int errCode = 1045;
                            String msg = "Could not infer the matching function for "
                                + func.getFuncSpec()
                                + " as multiple or none of them fit. Please use an explicit cast.";
                            msgCollector.collect(msg, MessageType.Error);
                            throw new TypeCheckerException(msg, errCode, PigException.INPUT);
                        }
                    }
                } else if ((matchingSpec = bestFitMatch(funcSpecs, s)) == null) {
                    // Either no byte arrays found or there are byte arrays
                    // but only one mapping exists.
                    // However, we could not find a match as there were either
                    // none fitting the input schema or it was ambiguous.
                    // Throw exception that we can't infer a fit.
                    int errCode = 1045;
                    String msg = "Could not infer the matching function for "
                            + func.getFuncSpec()
                            + " as multiple or none of them fit. Please use an explicit cast.";
                    msgCollector.collect(msg, MessageType.Error);
                    throw new TypeCheckerException(msg, errCode, PigException.INPUT);
                }
            }
        }
        if(matchingSpec!=null){
            //Voila! We have a fitting match. Lets insert casts and make
            //it work.
            // notify the user about the match we picked if it was not
            // an exact match
            if(notExactMatch) {
                String msg = "Function " + func.getFuncSpec().getClassName() + "()" +
                             " will be called with following argument types: " +
                             matchingSpec.getInputArgsSchema() + ". If you want to use " +
                             "different input argument types, please use explicit casts.";
                msgCollector.collect(msg, MessageType.Warning, PigWarning.USING_OVERLOADED_FUNCTION);
            }
            func.setFuncSpec(matchingSpec);
            insertCastsForUDF(func, s, matchingSpec.getInputArgsSchema());
            
        }
            
        //Regenerate schema as there might be new additions
        try {
            func.regenerateFieldSchema();
        } catch (FrontendException fee) {
            int errCode = 1040;
            String msg = "Could not set UserFunc field schema";
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee) ;
        }
    }
    
    /**
     * Finds if there is an exact match between the schema supported by
     * one of the funcSpecs and the input schema s. Here first exact match
     * for all non byte array fields is first attempted and if there is
     * exactly one candidate, it is chosen (since the bytearray(s) can
     * just be cast to corresponding type(s) in the candidate)
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param func - LOUserfunc for which matching is requested
     * @return the matching spec if found else null
     * @throws VisitorException 
     */
    private FuncSpec exactMatchWithByteArrays(List<FuncSpec> funcSpecs,
            Schema s, LOUserFunc func) throws VisitorException {
        // exact match all fields except byte array fields
        // ignore byte array fields for matching
        return exactMatchHelper(funcSpecs, s, func, true);
    }

    /**
     * Finds if there is an exact match between the schema supported by
     * one of the funcSpecs and the input schema s. Here an exact match
     * for all fields is attempted.
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param func - LOUserfunc for which matching is requested
     * @return the matching spec if found else null
     * @throws VisitorException 
     */
    private FuncSpec exactMatch(List<FuncSpec> funcSpecs, Schema s,
            LOUserFunc func) throws VisitorException {
        // exact match all fields, don't ignore byte array fields
        return exactMatchHelper(funcSpecs, s, func, false);
    }

    /**
     * Tries to find the schema supported by one of funcSpecs which can
     * be obtained by inserting a set of casts to the input schema
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @return the funcSpec that supports the schema that is best suited
     *          to s. The best suited schema is one that has the
     *          lowest score as returned by fitPossible().
     */
    private FuncSpec bestFitMatch(List<FuncSpec> funcSpecs, Schema s) {
        FuncSpec matchingSpec = null;
        long score = INF;
        long prevBestScore = Long.MAX_VALUE;
        long bestScore = Long.MAX_VALUE;
        for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator.hasNext();) {
            FuncSpec fs = iterator.next();
            score = fitPossible(s,fs.getInputArgsSchema());
            if(score!=INF && score<=bestScore){
                matchingSpec = fs;
                prevBestScore = bestScore;
                bestScore = score;
            }
        }
        if(matchingSpec!=null && bestScore!=prevBestScore)
            return matchingSpec;

        return null;
    }

    private class ScoreFuncSpecListComparator implements Comparator<Pair<Long, FuncSpec>> {

        /* (non-Javadoc)
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        public int compare(Pair<Long, FuncSpec> o1, Pair<Long, FuncSpec> o2) {
            if(o1.first < o2.first)
                return -1;
            else if (o1.first > o2.first)
                return 1;
            else
                return 0;
        }
        
    }
    
    /**
     * Tries to find the schema supported by one of funcSpecs which can be
     * obtained by inserting a set of casts to the input schema
     * 
     * @param funcSpecs -
     *            mappings provided by udf
     * @param s -
     *            input schema
     * @return the funcSpec that supports the schema that is best suited to s.
     *         The best suited schema is one that has the lowest score as
     *         returned by fitPossible().
     * @throws VisitorException
     */
    private FuncSpec bestFitMatchWithByteArrays(List<FuncSpec> funcSpecs,
            Schema s, LOUserFunc func) throws VisitorException {
		List<Pair<Long, FuncSpec>> scoreFuncSpecList = new ArrayList<Pair<Long,FuncSpec>>();
        for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator
                .hasNext();) {
            FuncSpec fs = iterator.next();
            long score = fitPossible(s, fs.getInputArgsSchema());
            if (score != INF) {
                scoreFuncSpecList.add(new Pair<Long, FuncSpec>(score, fs));
            }
        }

        // if no candidates found, return null
        if(scoreFuncSpecList.size() == 0)
            return null;
        
        if(scoreFuncSpecList.size() > 1) {
            // sort the candidates based on score
            Collections.sort(scoreFuncSpecList, new ScoreFuncSpecListComparator());
            
            // if there are two (or more) candidates with the same *lowest* score
            // we cannot choose one of them - notify the user
            if (scoreFuncSpecList.get(0).first == scoreFuncSpecList.get(1).first) {
                int errCode = 1046;
                String msg = "Multiple matching functions for "
                        + func.getFuncSpec() + " with input schemas: " + "("
                        + scoreFuncSpecList.get(0).second.getInputArgsSchema() + ", " 
                        + scoreFuncSpecList.get(1).second.getInputArgsSchema() + "). Please use an explicit cast.";
                msgCollector.collect(msg, MessageType.Error);
                throw new TypeCheckerException(msg, errCode, PigException.INPUT);
            }
        
            // now consider the bytearray fields
            List<Integer> byteArrayPositions = getByteArrayPositions(s);
            // make sure there is only one type to "cast to" for the byte array
            // positions among the candidate funcSpecs
            Map<Integer, Pair<FuncSpec, Byte>> castToMap = new HashMap<Integer, Pair<FuncSpec, Byte>>();
            for (Iterator<Pair<Long, FuncSpec>> it = scoreFuncSpecList.iterator(); it.hasNext();) {
                FuncSpec funcSpec = it.next().second;
                Schema sch = funcSpec.getInputArgsSchema();
                for (Iterator<Integer> iter = byteArrayPositions.iterator(); iter
                        .hasNext();) {
                    Integer i = iter.next();
                    try {
                        if (!castToMap.containsKey(i)) {
                            // first candidate
                            castToMap.put(i, new Pair<FuncSpec, Byte>(funcSpec, sch
                                    .getField(i).type));
                        } else {
                            // make sure the existing type from an earlier candidate
                            // matches
                            Pair<FuncSpec, Byte> existingPair = castToMap.get(i);
                            if (sch.getField(i).type != existingPair.second) {
                                int errCode = 1046;
                                String msg = "Multiple matching functions for "
                                        + func.getFuncSpec() + " with input schema: " 
                                        + "(" + existingPair.first.getInputArgsSchema() 
                                        + ", " + funcSpec.getInputArgsSchema() 
                                        + "). Please use an explicit cast.";
                                msgCollector.collect(msg, MessageType.Error);
                                throw new TypeCheckerException(msg, errCode, PigException.INPUT);
                            }
                        }
                    } catch (FrontendException fee) {
                        int errCode = 1043;
                        String msg = "Unalbe to retrieve field schema.";
                        throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee);
                    }
                }
            }
        }
        
        // if we reached here, it means we have >= 1 candidates and these candidates
        // have the same type for position which have bytearray in the input
        // Also the candidates are stored sorted by score in a list - we can now
        // just return the first candidate (the one with the lowest score)
        return scoreFuncSpecList.get(0).second;
    }
    
    /**
     * Checks to see if any field of the input schema is a byte array
     * @param s - input schema
     * @return true if found else false
     * @throws VisitorException
     */
    private boolean byteArrayFound(Schema s) throws VisitorException {
        for(int i=0;i<s.size();i++){
            try {
                FieldSchema fs=s.getField(i);
                if(fs.type==DataType.BYTEARRAY){
                    return true;
                }
            } catch (FrontendException fee) {
                int errCode = 1043;
                String msg = "Unable to retrieve field schema.";
                throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee);
            }
        }
        return false;
    }

    /**
     * Gets the positions in the schema which are byte arrays
     * 
     * @param s -
     *            input schema
     * @throws VisitorException
     */
    private List<Integer> getByteArrayPositions(Schema s)
            throws VisitorException {
        List<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < s.size(); i++) {
            try {
                FieldSchema fs = s.getField(i);
                if (fs.type == DataType.BYTEARRAY) {
                    result.add(i);
                }
            } catch (FrontendException fee) {
                int errCode = 1043;
                String msg = "Unable to retrieve field schema.";
                throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee);            }
        }
        return result;
    }

    /**
     * Finds if there is an exact match between the schema supported by
     * one of the funcSpecs and the input schema s
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param ignoreByteArrays - flag for whether the exact match is to computed
     * after ignoring bytearray (if true) or without ignoring bytearray (if false)
     * @return the matching spec if found else null
     * @throws VisitorException 
     */
    private FuncSpec exactMatchHelper(List<FuncSpec> funcSpecs, Schema s, LOUserFunc func, boolean ignoreByteArrays) throws VisitorException {
        List<FuncSpec> matchingSpecs = new ArrayList<FuncSpec>();
        for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator.hasNext();) {
            FuncSpec fs = iterator.next();
            if (schemaEqualsForMatching(s, fs.getInputArgsSchema(), ignoreByteArrays)) {
                matchingSpecs.add(fs);
            }
        }
        if(matchingSpecs.size() == 0)
            return null;
        
        if(matchingSpecs.size() > 1) {
            int errCode = 1046;
            String msg = "Multiple matching functions for "
                                        + func.getFuncSpec() + " with input schema: " 
                                        + "(" + matchingSpecs.get(0).getInputArgsSchema() 
                                        + ", " + matchingSpecs.get(1).getInputArgsSchema() 
                                        + "). Please use an explicit cast.";
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT);
        }
        
        // exactly one matching spec - return it
        return matchingSpecs.get(0);
    }

    /***************************************************************************
     * Compare two schemas for equality for argument matching purposes. This is
     * a more relaxed form of Schema.equals wherein first the Datatypes of the
     * field schema are checked for equality. Then if a field schema in the udf
     * schema is for a complex type AND if the inner schema is NOT null, check
     * for schema equality of the inner schemas of the UDF field schema and
     * input field schema
     * 
     * @param inputSchema
     * @param udfSchema
     * @param ignoreByteArrays
     * @return true if FieldSchemas are equal for argument matching, false
     *         otherwise
     */
    public static boolean schemaEqualsForMatching(Schema inputSchema,
            Schema udfSchema, boolean ignoreByteArrays) {
        // If both of them are null, they are equal
        if ((inputSchema == null) && (udfSchema == null)) {
            return true;
        }

        // otherwise
        if (inputSchema == null) {
            return false;
        }

        if (udfSchema == null) {
            return false;
        }

        if (inputSchema.size() != udfSchema.size())
            return false;

        Iterator<FieldSchema> i = inputSchema.getFields().iterator();
        Iterator<FieldSchema> j = udfSchema.getFields().iterator();

        while (i.hasNext()) {

            FieldSchema inputFieldSchema = i.next();
            FieldSchema udfFieldSchema = j.next();

            if(ignoreByteArrays && inputFieldSchema.type == DataType.BYTEARRAY) {
                continue;
            }
            
            if (inputFieldSchema.type != udfFieldSchema.type) {
                return false;
            }

            // if a field schema in the udf schema is for a complex
            // type AND if the inner schema is NOT null, check for schema
            // equality of the inner schemas of the UDF field schema and
            // input field schema. If the field schema in the udf schema is
            // for a complex type AND if the inner schema IS null it means
            // the udf is applicable for all input which has the same type
            // for that field (irrespective of inner schema)
            if (DataType.isSchemaType(udfFieldSchema.type)
                    && udfFieldSchema.schema != null) {
                // Compare recursively using field schema
                if (!FieldSchema.equals(inputFieldSchema, udfFieldSchema,
                        false, true)) {
                    return false;
                }
            }

        }
        return true;
    }

    /**
     * Computes a modified version of manhattan distance between 
     * the two schemas: s1 & s2. Here the value on the same axis
     * are preferred over values that change axis as this means
     * that the number of casts required will be lesser on the same
     * axis. 
     * 
     * However, this function ceases to be a metric as the triangle
     * inequality does not hold.
     * 
     * Each schema is an s1.size() dimensional vector.
     * The ordering for each axis is as defined by castLookup. 
     * Unallowed casts are returned a dist of INFINITY.
     * @param s1
     * @param s2
     * @return
     */
    private long fitPossible(Schema s1, Schema s2) {
        if(s1==null || s2==null) return INF;
        List<FieldSchema> sFields = s1.getFields();
        List<FieldSchema> fsFields = s2.getFields();
        if(sFields.size()!=fsFields.size())
            return INF;
        long score = 0;
        int castCnt=0;
        for(int i=0;i<sFields.size();i++){
            FieldSchema sFS = sFields.get(i);

            // if we have a byte array do not include it
            // in the computation of the score - bytearray
            // fields will be looked at separately outside
            // of this function
            if (sFS.type == DataType.BYTEARRAY)
                continue;

            FieldSchema fsFS = fsFields.get(i);
            
            if(DataType.isSchemaType(sFS.type)){
                if(!FieldSchema.equals(sFS, fsFS, false, true))
                    return INF;
            }
            if(FieldSchema.equals(sFS, fsFS, true, true)) continue;
            if(!castLookup.containsKey(sFS.type))
                return INF;
            if(!(castLookup.get(sFS.type).contains(fsFS.type)))
                return INF;
            score += ((List)castLookup.get(sFS.type)).indexOf(fsFS.type) + 1;
            ++castCnt;
        }
        return score * castCnt;
    }
    
    private void insertCastsForUDF(LOUserFunc udf, Schema fromSch, Schema toSch) throws VisitorException {
        List<FieldSchema> fsLst = fromSch.getFields();
        List<FieldSchema> tsLst = toSch.getFields();
        List<ExpressionOperator> args = udf.getArguments();
        List<ExpressionOperator> newArgs = new ArrayList<ExpressionOperator>(args.size());
        int i=-1;
        for (FieldSchema fFSch : fsLst) {
            ++i;
            FieldSchema tFSch = tsLst.get(i); 
            if(fFSch.type==tFSch.type) {
                continue;
            }
            insertCast(udf, tFSch.type, args.get(i));
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
            int errCode = 1047;
            String msg = "Condition in BinCond must be boolean" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
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
        else if ((lhsType == DataType.BYTEARRAY)
                && ((rhsType == DataType.CHARARRAY) || (DataType
                        .isNumberType(rhsType)))) {
            // Cast byte array to the type on rhs
            insertLeftCastForBinCond(binCond, rhsType);
            binCond.setType(DataType.mergeType(lhsType, rhsType));
        } else if ((rhsType == DataType.BYTEARRAY)
                && ((lhsType == DataType.CHARARRAY) || (DataType
                        .isNumberType(lhsType)))) {
            // Cast byte array to the type on lhs
            insertRightCastForBinCond(binCond, lhsType);
            binCond.setType(DataType.mergeType(lhsType, rhsType));
        }
        // A constant null is always bytearray - so cast it
        // to rhs type
        else if (binCond.getLhsOp() instanceof LOConst
                && ((LOConst) binCond.getLhsOp()).getValue() == null) {
            insertLeftCastForBinCond(binCond, rhsType);
        } else if (binCond.getRhsOp() instanceof LOConst
                && ((LOConst) binCond.getRhsOp()).getValue() == null) {
            insertRightCastForBinCond(binCond, lhsType);
        } else if (lhsType == rhsType) {
            // Matching schemas if we're working with tuples
            if (DataType.isSchemaType(lhsType)) {            
                try {
                    if (!Schema.FieldSchema.equals(binCond.getLhsOp().getFieldSchema(), binCond.getRhsOp().getFieldSchema(), false, true)) {
                        int errCode = 1048;
                        String msg = "Two inputs of BinCond must have compatible schemas." 
                            + " left hand side: " + binCond.getLhsOp().getFieldSchema() 
                            + " right hand side: " + binCond.getRhsOp().getFieldSchema();
                        msgCollector.collect(msg, MessageType.Error) ;
                        throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
                    }
                    // TODO: We may have to merge the schema here
                    //       if the previous check is not exact match
                    //       Is Schema.reconcile good enough?
                } 
                catch (FrontendException fe) {
                    int errCode = 1049;
                    String msg = "Problem during evaluaton of BinCond output type" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
                }
                binCond.setType(DataType.TUPLE) ;
            }
        
            binCond.setType(lhsType);
        }
        else {
            int errCode = 1050;
            String msg = "Unsupported input type for BinCond: left hand side: " + DataType.findTypeName(lhsType) + "; right hand side: " + DataType.findTypeName(rhsType);
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

        try {
            binCond.regenerateFieldSchema();
        } catch (FrontendException fee) {
            int errCode = 1040;
            String msg = "Could not set BinCond field schema";
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee) ;
        }

    }

    private void insertLeftCastForBinCond(LOBinCond binCond, byte toType) throws VisitorException {
        insertCast(binCond, toType, binCond.getLhsOp());
    }

    private void insertRightCastForBinCond(LOBinCond binCond, byte toType) throws VisitorException {
        insertCast(binCond, toType, binCond.getRhsOp());
    }

    /**
     * For Basic Types:
     * 0) Casting to itself is always ok
     * 1) Casting from number to number is always ok 
     * 2) ByteArray to anything is ok
     * 3) (number or chararray) to (bytearray or chararray) is ok
     * For Composite Types:
     * Recursively traverse the schemas till you get a basic type
     */
    @Override
    protected void visit(LOCast cast) throws VisitorException {      
        
        byte inputType = cast.getExpression().getType(); 
        byte expectedType = cast.getType();


        if(expectedType == DataType.BYTEARRAY) {
            int errCode = 1051;
            String msg = "Cannot cast to bytearray";
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ; 
        }
        
        Schema.FieldSchema castFs;
        Schema.FieldSchema inputFs;
        try {
            castFs = cast.getFieldSchema();
            inputFs = cast.getExpression().getFieldSchema();
        } catch(FrontendException fee) {
            int errCode = 1076;
            String msg = "Problem while reading field schema of cast operator.";
            throw new TypeCheckerException(msg, errCode, PigException.BUG, fee);
        }
        boolean castable = Schema.FieldSchema.castable(castFs, inputFs);
        if(!castable) {
            int errCode = 1052;
            String msg = "Cannot cast "
                           + DataType.findTypeName(inputType)
                           + ((DataType.isSchemaType(inputType))? " with schema " + inputFs : "")
                           + " to "
                           + DataType.findTypeName(expectedType)
                           + ((DataType.isSchemaType(expectedType))? " with schema " + castFs : "");
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ; 
        }
        
        // cast.getType() already returns the correct type so don't have to 
        // set here. This is a special case where output type is not
        // automatically determined.
        
        if(inputType == DataType.BYTEARRAY) {
            try {
                FuncSpec loadFuncSpec = getLoadFuncSpec(cast.getExpression());
                cast.setLoadFuncSpec(loadFuncSpec);
            } catch (FrontendException fee) {
                int errCode = 1053;
                String msg = "Cannot resolve load function to use for casting from " + 
                            DataType.findTypeName(inputType) + " to " +
                            DataType.findTypeName(expectedType) + ". ";
                msgCollector.collect(msg, MessageType.Error);
                throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee); 
            }
        }
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
        u.unsetSchema();
        // Have to make a copy, because as we insert operators, this list will
        // change under us.
        List<LogicalOperator> inputs = 
            new ArrayList<LogicalOperator>(u.getInputs());

        // There is no point to union only one operand
        // it should be a problem in the parser
        if (inputs.size() < 2) {
            AssertionError err =  new AssertionError("Union with Count(Operand) < 2") ;
        }

        Schema schema = null ;
        try {

            if (strictMode) {
                // Keep merging one by one just to check if there is
                // any problem with types in strict mode
                Schema tmpSchema = inputs.get(0).getSchema() ;
                for (int i=1; i< inputs.size() ;i++) {
                    // Assume the first input's aliases take precedance
                    tmpSchema = tmpSchema.merge(inputs.get(i).getSchema(), false) ;

                    // if they cannot be merged, we just give up
                    if (tmpSchema == null) {
                        int errCode = 1054;
                        String msg = "Cannot merge schemas from inputs of UNION" ;
                        msgCollector.collect(msg, MessageType.Error) ;
                        throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
                    }
                }
            }

            // Compute the schema
            schema = u.getSchema() ;

        }
        catch (FrontendException fee) {
            int errCode = 1055;
            String msg = "Problem while reading schemas from inputs of Union" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee) ;
        }

        // Do cast insertion only if we are typed
        if (schema != null) {
            // Insert casting to inputs if necessary
            for (int i=0; i< inputs.size() ;i++) {
                LOForEach insertedOp
                        = insertCastForEachInBetweenIfNecessary(inputs.get(i), u, schema) ;

                // We may have to compute the schema of the input again
                // because we have just inserted
                if (insertedOp != null) {
                    if(insertedOp.getAlias()==null){
                        insertedOp.setAlias(inputs.get(i).getAlias());
                    }
                    try {
                        this.visit(insertedOp);
                    }
                    catch (FrontendException fee) {
                        int errCode = 1056;
                        String msg = "Problem while casting inputs of Union" ;
                        msgCollector.collect(msg, MessageType.Error) ;
                        throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee) ;
                    }
                }
            }
        }
    }

    @Override
    protected void visit(LOSplitOutput op) throws VisitorException {
        op.unsetSchema();
        LogicalPlan currentPlan =  mCurrentWalker.getPlan() ;

        // LOSplitOutput can only have 1 input
        List<LogicalOperator> list = currentPlan.getPredecessors(op) ;
        if (list.size() != 1) {
            int errCode = 2008;
            String msg = "LOSplitOutput cannot have more than one input. Found: " + list.size() + " input(s).";
            throw new TypeCheckerException(msg, errCode, PigException.BUG) ;
        }

        LogicalOperator input = list.get(0);
        LogicalPlan condPlan = op.getConditionPlan() ;

        // Check that the inner plan has only 1 output port
        if (!condPlan.isSingleLeafPlan()) {
            int errCode = 1057;
            String msg = "Split's inner plan can only have one output (leaf)" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }
            
        checkInnerPlan(condPlan) ;
                 
        byte innerCondType = condPlan.getLeaves().get(0).getType() ;
        if (innerCondType != DataType.BOOLEAN) {
            int errCode = 1058;
            String msg = "Split's condition must evaluate to boolean. Found: " + DataType.findTypeName(innerCondType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

        try {
            // Compute the schema
            op.getSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading"
                         + " schemas from inputs of SplitOutput" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }


    /***
     *  LODistinct, output schema should be the same as input
     * @param op
     * @throws VisitorException
     */
    
    @Override
    protected void visit(LODistinct op) throws VisitorException {
        op.unsetSchema();
        LogicalPlan currentPlan = mCurrentWalker.getPlan() ;
        List<LogicalOperator> list = currentPlan.getPredecessors(op) ;

        try {
            // Compute the schema
            op.getSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading"
                         + " schemas from inputs of Distinct" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }

    @Override
    protected void visit(LOLimit op) throws VisitorException {
        try {
            // Compute the schema
            op.regenerateSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading"
                         + " schemas from inputs of Limit" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }

    /***
     * Return concatenated of all fields from all input operators
     * If one of the inputs have no schema then we cannot construct
     * the output schema.
     * @param cs
     * @throws VisitorException
     */
    protected void visit(LOCross cs) throws VisitorException {
        cs.unsetSchema();
        List<LogicalOperator> inputs = cs.getInputs() ;
        List<FieldSchema> fsList = new ArrayList<FieldSchema>() ;

        try {
            // Compute the schema
            cs.getSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading"
                        + " schemas from inputs of Cross" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }
    
    /***
     * The schema of sort output will be the same as sort input.
     *
     */

    protected void visit(LOSort s) throws VisitorException {
        s.unsetSchema();
        LogicalOperator input = s.getInput() ;
        
        // Type checking internal plans.
        for(int i=0;i < s.getSortColPlans().size(); i++) {
            
            LogicalPlan sortColPlan = s.getSortColPlans().get(i) ;

            // Check that the inner plan has only 1 output port
            if (!sortColPlan.isSingleLeafPlan()) {
                int errCode = 1057;
                String msg = "Sort's inner plan can only have one output (leaf)" ;
                msgCollector.collect(msg, MessageType.Error) ;
                throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
            }

            checkInnerPlan(sortColPlan) ;
            // TODO: May have to check SortFunc compatibility here in the future
                       
        }
        
        s.setType(input.getType()) ;  // This should be bag always.

        try {
            // Compute the schema
            s.getSchema() ;
        }
        catch (FrontendException fee) {
            int errCode = 1059;
            String msg = "Problem while reconciling output schema of Sort" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee) ;
        }
    }


    /***
     * The schema of filter output will be the same as filter input
     */

    @Override
    protected void visit(LOFilter filter) throws VisitorException {
        filter.unsetSchema();
        LogicalOperator input = filter.getInput() ;
        LogicalPlan comparisonPlan = filter.getComparisonPlan() ;
        
        // Check that the inner plan has only 1 output port
        if (!comparisonPlan.isSingleLeafPlan()) {
            int errCode = 1057;
            String msg = "Filter's cond plan can only have one output (leaf)" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }

        checkInnerPlan(comparisonPlan) ;
              
        byte innerCondType = comparisonPlan.getLeaves().get(0).getType() ;
        if (innerCondType != DataType.BOOLEAN) {
            int errCode = 1058;
            String msg = "Filter's condition must evaluate to boolean. Found: " + DataType.findTypeName(innerCondType);
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
        }       


        try {
            // Compute the schema
            filter.getSchema() ;
        } 
        catch (FrontendException fe) {
            int errCode = 1059;
            String msg = "Problem while reconciling output schema of Filter" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }

    /***
     * The schema of split output will be the same as split input
     */

    protected void visit(LOSplit split) throws VisitorException {
        // TODO: Why doesn't LOSplit have getInput() ???
        List<LogicalOperator> inputList = mPlan.getPredecessors(split) ;
        
        if (inputList.size() != 1) {            
            int errCode = 2008;
            String msg = "LOSplit cannot have more than one input. Found: " + inputList.size() + " input(s).";
            throw new TypeCheckerException(msg, errCode, PigException.BUG) ;
        }
        
        LogicalOperator input = inputList.get(0) ;
        
        try {
            // Compute the schema
            split.regenerateSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1059;
            String msg = "Problem while reconciling output schema of Split" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }
    
    /**
     * Mimics the type checking of LOCogroup
     */
    protected void visit(LOFRJoin frj) throws VisitorException {
        try {
            frj.regenerateSchema();
        } catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve Fragment Replicate Join output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
        
        MultiMap<LogicalOperator, LogicalPlan> joinColPlans
                                                    = frj.getJoinColPlans() ;
        List<LogicalOperator> inputs = frj.getInputs() ;
        
        // Type checking internal plans.
        for(int i=0;i < inputs.size(); i++) {
            LogicalOperator input = inputs.get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(joinColPlans.get(input)) ;

            for(int j=0; j < innerPlans.size(); j++) {

                LogicalPlan innerPlan = innerPlans.get(j) ;
                
                // Check that the inner plan has only 1 output port
                if (!innerPlan.isSingleLeafPlan()) {
                    int errCode = 1057;
                    String msg = "Fragment Replicate Join's inner plans can only"
                                 + "have one output (leaf)" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
                }

                checkInnerPlan(innerPlans.get(j)) ;
            }
        }
        
        try {

            if (!frj.isTupleJoinCol()) {
                // merge all the inner plan outputs so we know what type
                // our group column should be

                // TODO: Don't recompute schema here
                //byte groupType = schema.getField(0).type ;
                byte groupType = frj.getAtomicJoinColType() ;

                // go through all inputs again to add cast if necessary
                for(int i=0;i < inputs.size(); i++) {
                    LogicalOperator input = inputs.get(i) ;
                    List<LogicalPlan> innerPlans
                                = new ArrayList<LogicalPlan>(joinColPlans.get(input)) ;
                    // Checking innerPlan size already done above
                    byte innerType = innerPlans.get(0).getSingleLeafPlanOutputType() ;
                    if (innerType != groupType) {
                        insertAtomicCastForFRJInnerPlan(innerPlans.get(0),
                                                            frj,
                                                            groupType) ;
                    }
                }
            }
            else {

                // TODO: Don't recompute schema here
                //Schema groupBySchema = schema.getField(0).schema ;
                Schema groupBySchema = frj.getTupleJoinColSchema() ;

                // go through all inputs again to add cast if necessary
                for(int i=0;i < inputs.size(); i++) {
                    LogicalOperator input = inputs.get(i) ;
                    List<LogicalPlan> innerPlans
                                = new ArrayList<LogicalPlan>(joinColPlans.get(input)) ;
                    for(int j=0;j < innerPlans.size(); j++) {
                        LogicalPlan innerPlan = innerPlans.get(j) ;
                        byte innerType = innerPlan.getSingleLeafPlanOutputType() ;
                        byte expectedType = DataType.BYTEARRAY ;

                        if (!DataType.isAtomic(innerType) && (DataType.TUPLE != innerType)) {
                            int errCode = 1057;
                            String msg = "Fragment Replicate Join's inner plans can only"
                                         + "have one output (leaf)" ;
                            msgCollector.collect(msg, MessageType.Error) ;
                            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
                        }

                        try {
                            expectedType = groupBySchema.getField(j).type ;
                        }
                        catch(FrontendException fee) {
                            int errCode = 1060;
                            String msg = "Cannot resolve Fragment Replicate Join output schema" ;
                            msgCollector.collect(msg, MessageType.Error) ;
                            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee) ;
                        }

                        if (innerType != expectedType) {
                            insertAtomicCastForFRJInnerPlan(innerPlan,
                                                                frj,
                                                                expectedType) ;
                        }
                    }
                }
            }
        }
        catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve Fragment Replicate Join output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }

        try {
            Schema outputSchema = frj.regenerateSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve Fragment Replicate Join output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }

    /**
     * COGroup
     * All group by cols from all inputs have to be of the
     * same type
     */
    protected void visit(LOCogroup cg) throws VisitorException {
        try {
            cg.regenerateSchema();
        } catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
        
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
                    int errCode = 1057;
                    String msg = "COGroup's inner plans can only"
                                 + "have one output (leaf)" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
                }

                checkInnerPlan(innerPlans.get(j)) ;
            }

        }

        try {

            if (!cg.isTupleGroupCol()) {
                // merge all the inner plan outputs so we know what type
                // our group column should be

                // TODO: Don't recompute schema here
                //byte groupType = schema.getField(0).type ;
                byte groupType = cg.getAtomicGroupByType() ;

                // go through all inputs again to add cast if necessary
                for(int i=0;i < inputs.size(); i++) {
                    LogicalOperator input = inputs.get(i) ;
                    List<LogicalPlan> innerPlans
                                = new ArrayList<LogicalPlan>(groupByPlans.get(input)) ;
                    // Checking innerPlan size already done above
                    byte innerType = innerPlans.get(0).getSingleLeafPlanOutputType() ;
                    if (innerType != groupType) {
                        insertAtomicCastForCOGroupInnerPlan(innerPlans.get(0),
                                                            cg,
                                                            groupType) ;
                    }
                }
            }
            else {

                // TODO: Don't recompute schema here
                //Schema groupBySchema = schema.getField(0).schema ;
                Schema groupBySchema = cg.getTupleGroupBySchema() ;

                // go through all inputs again to add cast if necessary
                for(int i=0;i < inputs.size(); i++) {
                    LogicalOperator input = inputs.get(i) ;
                    List<LogicalPlan> innerPlans
                                = new ArrayList<LogicalPlan>(groupByPlans.get(input)) ;
                    for(int j=0;j < innerPlans.size(); j++) {
                        LogicalPlan innerPlan = innerPlans.get(j) ;
                        byte innerType = innerPlan.getSingleLeafPlanOutputType() ;
                        byte expectedType = DataType.BYTEARRAY ;

                        if (!DataType.isAtomic(innerType) && (DataType.TUPLE != innerType)) {
                            int errCode = 1061;
                            String msg = "Sorry, group by complex types"
                                       + " will be supported soon" ;
                            msgCollector.collect(msg, MessageType.Error) ;
                            throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
                        }

                        try {
                            expectedType = groupBySchema.getField(j).type ;
                        }
                        catch(FrontendException fee) {
                            int errCode = 1060;
                            String msg = "Cannot resolve COGroup output schema" ;
                            msgCollector.collect(msg, MessageType.Error) ;
                            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee) ;
                        }

                        if (innerType != expectedType) {
                            insertAtomicCastForCOGroupInnerPlan(innerPlan,
                                                                cg,
                                                                expectedType) ;
                        }
                    }
                }
            }
        }
        catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }

        // TODO: Don't recompute schema here. Remove all from here!
        // Generate output schema based on the schema generated from
        // COGroup itself

        try {
            Schema outputSchema = cg.regenerateSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }
    
    private void insertAtomicCastForFRJInnerPlan(LogicalPlan innerPlan,
            LOFRJoin frj, byte toType) throws VisitorException {
        if (!DataType.isUsableType(toType)) {
            int errCode = 1051;
            String msg = "Cannot cast to "
                + DataType.findTypeName(toType);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT);
        }

        List<LogicalOperator> leaves = innerPlan.getLeaves();
        if (leaves.size() > 1) {
            int errCode = 2060;
            String msg = "Expected one leaf. Found " + leaves.size() + " leaves.";
            throw new TypeCheckerException(msg, errCode, PigException.BUG);
        }
        ExpressionOperator currentOutput = (ExpressionOperator) leaves.get(0);
        collectCastWarning(frj, currentOutput.getType(), toType);
        OperatorKey newKey = genNewOperatorKey(currentOutput);
        LOCast cast = new LOCast(innerPlan, newKey, toType);
        innerPlan.add(cast);
        try {
            innerPlan.connect(currentOutput, cast);
        } catch (PlanException pe) {
            int errCode = 2059;
            String msg = "Problem with inserting cast operator for fragment replicate join in plan.";
            throw new TypeCheckerException(msg, errCode, PigException.BUG, pe);
        }
        this.visit(cast);
    }

    // This helps insert casting to atomic types in COGroup's inner plans
    // as a new leave of the plan
    private void insertAtomicCastForCOGroupInnerPlan(LogicalPlan innerPlan,
                                                     LOCogroup cg,
                                                     byte toType) throws VisitorException {
        if(!DataType.isUsableType(toType)) {
            int errCode = 1051;
            String msg = "Cannot cast to "
                + DataType.findTypeName(toType);
            throw new TypeCheckerException(msg, errCode, PigException.INPUT);
        }
        
        List<LogicalOperator> leaves = innerPlan.getLeaves() ;
        if (leaves.size() > 1) {
            int errCode = 2060;
            String msg = "Expected one leaf. Found " + leaves.size() + " leaves.";
            throw new TypeCheckerException(msg, errCode, PigException.BUG);
        }
        ExpressionOperator currentOutput = (ExpressionOperator) leaves.get(0) ;
        collectCastWarning(cg, currentOutput.getType(), toType) ;
        OperatorKey newKey = genNewOperatorKey(currentOutput) ;
        LOCast cast = new LOCast(innerPlan, newKey, toType) ;
        innerPlan.add(cast) ;
        try {
            innerPlan.connect(currentOutput, cast) ;
        }
        catch (PlanException pe) {
            int errCode = 2059;
            String msg = "Problem with inserting cast operator for cogroup in plan.";
            throw new TypeCheckerException(msg, errCode, PigException.BUG, pe);
        }
        this.visit(cast);
    }

    /**
     * This can be used to get the merged type of output group col
     * only when the group col is of atomic type
     * TODO: This doesn't work with group by complex type
     * @return The type of the group by
     */
    public byte getAtomicGroupByType(LOCogroup cg) throws VisitorException {
        if (cg.isTupleGroupCol()) {
            int errCode = 2061;
            String msg = "Expected single group by element but found multiple elements.";
            throw new TypeCheckerException(msg, errCode, PigException.BUG);
        }
        byte groupType = DataType.BYTEARRAY ;
        // merge all the inner plan outputs so we know what type
        // our group column should be
        for(int i=0;i < cg.getInputs().size(); i++) {
            LogicalOperator input = cg.getInputs().get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(cg.getGroupByPlans().get(input)) ;
            if (innerPlans.size() != 1) {
                int errCode = 2062;
                String msg = "Each COGroup input has to have "
                    + "the same number of inner plans.";
                throw new TypeCheckerException(msg, errCode, PigException.BUG) ;
            }
            byte innerType = innerPlans.get(0).getSingleLeafPlanOutputType() ;
            groupType = DataType.mergeType(groupType, innerType) ;
            if (groupType == DataType.ERROR) {
                // We just warn about mismatch type in non-strict mode
                if (!strictMode) {
                    String msg = "COGroup by incompatible types results in ByteArray" ;
                    msgCollector.collect(msg, MessageType.Warning, PigWarning.GROUP_BY_INCOMPATIBLE_TYPES) ;
                    groupType = DataType.BYTEARRAY ;
                }
                // We just die if in strict mode
                else {
                    int errCode = 1062;
                    String msg = "COGroup by incompatible types" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
                }
            }


        }

        return groupType ;

    }

    /*
        This implementation is based on the assumption that all the
        inputs have the same group col tuple arity.
        TODO: This doesn't work with group by complex type
     */
    public Schema getTupleGroupBySchema(LOCogroup cg) throws VisitorException {
        if (!cg.isTupleGroupCol()) {
            int errCode = 2063;
            String msg = "Expected multiple group by element but found single element.";
            throw new TypeCheckerException(msg, errCode, PigException.BUG);
        }

        // this fsList represents all the columns in group tuple
        List<Schema.FieldSchema> fsList = new ArrayList<Schema.FieldSchema>() ;

        int outputSchemaSize = cg.getGroupByPlans().get(cg.getInputs().get(0)).size() ;

        // by default, they are all bytearray
        // for type checking, we don't care about aliases
        for(int i=0; i<outputSchemaSize; i++) {
            fsList.add(new Schema.FieldSchema(null, DataType.BYTEARRAY)) ;
        }

        // merge all the inner plan outputs so we know what type
        // our group column should be
        for(int i=0;i < cg.getInputs().size(); i++) {
            LogicalOperator input = cg.getInputs().get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(cg.getGroupByPlans().get(input)) ;

            for(int j=0;j < innerPlans.size(); j++) {
                byte innerType = innerPlans.get(j).getSingleLeafPlanOutputType() ;
                fsList.get(j).type = DataType.mergeType(fsList.get(j).type,
                                                        innerType) ;
                if (fsList.get(j).type == DataType.ERROR) {
                    // We just warn about mismatch type in non-strict mode
                    if (!strictMode) {
                        String msg = "COGroup by incompatible types results in ByteArray" ;
                        msgCollector.collect(msg, MessageType.Warning, PigWarning.GROUP_BY_INCOMPATIBLE_TYPES) ;
                        fsList.get(j).type = DataType.BYTEARRAY ;
                    }
                    // We just die if in strict mode
                    else {
                        int errCode = 1062;
                        String msg = "COGroup by incompatible types" ;
                        msgCollector.collect(msg, MessageType.Error) ;
                        throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
                    }
                }
            }
        }

        return new Schema(fsList) ;
    }

    /***
     * Output schema of LOForEach is a tuple schma
     * which is the output of all inner plans
     *
     * Flatten also has to be taken care on in here
     *
     */

    protected void visit(LOForEach f) throws VisitorException {
        List<LogicalPlan> plans = f.getForEachPlans() ;
        List<Boolean> flattens = f.getFlatten() ;

        f.unsetSchema();
        try {

            // Have to resolve all inner plans before calling getSchema
            int outputSchemaIdx = 0 ;
            for(int i=0;i < plans.size(); i++) {

                LogicalPlan plan = plans.get(i) ;

                // Check that the inner plan has only 1 output port
                if (!plan.isSingleLeafPlan()) {
                    int errCode = 1057;
                    String msg = "Generate's expression plan can "
                                 + " only have one output (leaf)" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new TypeCheckerException(msg, errCode, PigException.INPUT) ;
                }

                List<LogicalOperator> rootList = plan.getRoots() ;
                for(int j=0; j<rootList.size(); j++) {
                    LogicalOperator innerRoot = rootList.get(j) ;
                    // TODO: Support MAP dereference
                    if (innerRoot instanceof LOProject) {
                        resolveLOProjectType((LOProject) innerRoot) ;
                    }
                    else if (innerRoot instanceof LOConst) {
                        // it's ok because LOConst always has
                        // the right type information
                    }
                    else {
                        int errCode = 2064;
                        String msg = "Unsupported root type in "
                            +"LOForEach: " + innerRoot.getClass().getSimpleName();
                        throw new TypeCheckerException(msg, errCode, PigException.BUG) ;
                    }
                }

                checkInnerPlan(plan) ;

            }

            f.getSchema();

        } catch (VisitorException ve) {
            throw ve;
        } catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve ForEach output schema.";
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(msg, errCode, PigException.INPUT, fe) ;
        }
    }
    
    /***
     * This does:-
     * 1) Propagate typing information from a schema to
     *    an inner plan of a relational operator
     * 2) Type checking of the inner plan
     * NOTE: This helper method only supports one source schema
     * 
     * @param innerPlan  the inner plan
     * @throws VisitorException
     */

    private void checkInnerPlan(LogicalPlan innerPlan)
                                    throws VisitorException {
        // Preparation
        int errorCount = 0 ;     
        List<LogicalOperator> rootList = innerPlan.getRoots() ;
        if (rootList.size() < 1) {
            int errCode = 2065;
            String msg = "Did not find roots of the inner plan.";
            throw new TypeCheckerException(msg, errCode, PigException.BUG) ;
        }

        /*
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
        */

        // Actual checking
        for(LogicalOperator op: rootList) {
            // TODO: Support map dereference
            if (op instanceof LOProject) {
                resolveLOProjectType((LOProject) op);
            }
            // TODO: I think we better need a sentinel connecting to LOForEach
            else if (op instanceof LOForEach) {
                op.setType(DataType.TUPLE);
                PlanWalker<LogicalOperator, LogicalPlan> walker
                        = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(innerPlan) ;
                pushWalker(walker) ;
                this.visit((LOForEach) op) ;
                popWalker() ;
            }
            else if (op instanceof LOConst) {
                // don't have to do anything
            }
            else if (op instanceof LOUserFunc){
                visit((LOUserFunc)op);
            }
            else {
                int errCode = 2066;                
                String msg = "Unsupported root operator in inner plan:"
                             + op.getClass().getSimpleName() ;
                throw new TypeCheckerException(msg, errCode, PigException.BUG) ;
            }
        }
        
        // Throw an exception if we found errors
        // TODO: add it back
        /*
        if (errorCount > 0) {
            // TODO: Should indicate the field names or indexes here
            String msg =  "Some required fields in inner plan cannot be found in input" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            throw vse ;
        } */
        
        // Check typing of the inner plan by visiting it
        PlanWalker<LogicalOperator, LogicalPlan> walker
            = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(innerPlan) ;
        pushWalker(walker) ;       
        this.visit() ; 
        popWalker() ;
        
    }

    protected void visit(LOLoad load)
                        throws VisitorException {
        // do nothing
    }

    protected void visit(LOStore store) {
        // do nothing
    }


    /***
     * For casting insertion for relational operators
     * only if it's necessary
     * Currently this only does "shallow" casting
     * @param fromOp
     * @param toOp
     * @param targetSchema array of target types
     * @return the inserted operator. null is no insertion
     */
    private LOForEach insertCastForEachInBetweenIfNecessary(LogicalOperator fromOp,
                                                       LogicalOperator toOp,
                                                       Schema targetSchema)
                                                    throws VisitorException {
        LogicalPlan currentPlan =  mCurrentWalker.getPlan() ;

        /*
        // Make sure that two operators are in the same plan
        if (fromOp.getPlan() != toOp.getPlan()) {
            throw new AssertionError("Two operators have toOp be in the same plan") ;
        }
        // Mare sure that they are in the plan we're looking at
        if (fromOp.getPlan() != toOp.getPlan()) {
            throw new AssertionError("Cannot manipulate any other plan"
                                    +" than the current one") ;
        }
        */

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
            int errCode = 1077;
            String msg = "Two operators that require a cast in between are not adjacent.";
            throw new TypeCheckerException(msg, errCode, PigException.INPUT);
        }

        // retrieve input schema to be casted
        // this will be used later
        Schema fromSchema = null ;
        try {
            fromSchema = fromOp.getSchema() ;
        }
        catch(FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading schema from input of " + fromOp.getClass().getSimpleName();
            throw new TypeCheckerException(msg, errCode, PigException.BUG, fe);
        }

        // make sure the supplied targetSchema has the same number of members
        // as number of output fields from "fromOp"
        if (fromSchema.size() != targetSchema.size()) {
            int errCode = 1078;
            String msg = "Schema size mismatch for casting. Input schema size: " + fromSchema.size() + ". Target schema size: " + targetSchema.size();
            throw new TypeCheckerException(msg, errCode, PigException.INPUT);
        }

        // Compose the new inner plan to be used in ForEach
        LogicalPlan foreachPlan = new LogicalPlan() ;

        // Plans inside Generate. Fields that do not need casting will only
        // have Project. Fields that need casting will have Project + Cast
        ArrayList<LogicalPlan> generatePlans = new ArrayList<LogicalPlan>() ;

        int castNeededCounter = 0 ;
        for(int i=0;i < fromSchema.size(); i++) {

            LogicalPlan genPlan = new LogicalPlan() ;
            LOProject project = new LOProject(genPlan,
                                              genNewOperatorKey(fromOp),
                                              fromOp,
                                              i) ;
            project.setSentinel(true);
            genPlan.add(project);

            // add casting if necessary by comparing target types
            // to the input schema
            FieldSchema fs = null ;
            try {
                fs = fromSchema.getField(i) ;
            }
            catch(FrontendException fee) {
                int errCode = 1063;
                String msg = "Problem while reading"
                                + " field schema from input while"
                                + " inserting cast " ;
                msgCollector.collect(msg, MessageType.Error) ;
                throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee) ;
            }

            // This only does "shallow checking"

            byte inputFieldType ;

            try {
                inputFieldType = targetSchema.getField(i).type ;
            }
            catch (FrontendException fee) {
                int errCode = 1064;
                String msg = "Problem reading column " + i + " from schema: " + targetSchema;
                throw new TypeCheckerException(msg, errCode, PigException.INPUT, fee) ;                
            }

            if (inputFieldType != fs.type) {
                castNeededCounter++ ;
                LOCast cast = new LOCast(genPlan,
                                         genNewOperatorKey(fromOp),
                                         inputFieldType) ;
                genPlan.add(cast) ;
                try {
                    genPlan.connect(project, cast);
                }
                catch (PlanException pe) {
                    // This should never happen
                    int errCode = 2059;
                    String msg = "Problem with inserting cast operator for project in plan.";
                    throw new TypeCheckerException(msg, errCode, PigException.BUG, pe);
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

            // Create ForEach to be inserted
            LOForEach foreach = new LOForEach(currentPlan, genNewOperatorKey(fromOp), generatePlans, flattenList) ;

            // Manipulate the plan structure
            currentPlan.add(foreach);
            currentPlan.disconnect(fromOp, toOp) ;

            try {
                currentPlan.connect(fromOp, foreach);
                currentPlan.connect(foreach, toOp);
            }
            catch (PlanException pe) {
                int errCode = 2059;
                String msg = "Problem with inserting foeach operator for " + toOp.getClass().getSimpleName() + " in plan.";
                throw new TypeCheckerException(msg, errCode, PigException.BUG, pe);
            }

            return foreach;
            
        }
        else {
            log.debug("Tried to insert relational casting when not necessary");
            return null ;
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
        Enum kind = null;
        switch(toType) {
        case DataType.BAG:
        	kind = PigWarning.IMPLICIT_CAST_TO_BAG;
        	break;
        case DataType.CHARARRAY:
        	kind = PigWarning.IMPLICIT_CAST_TO_CHARARRAY;
        	break;
        case DataType.DOUBLE:
        	kind = PigWarning.IMPLICIT_CAST_TO_DOUBLE;
        	break;
        case DataType.FLOAT:
        	kind = PigWarning.IMPLICIT_CAST_TO_FLOAT;
        	break;
        case DataType.INTEGER:
        	kind = PigWarning.IMPLICIT_CAST_TO_INT;
        	break;
        case DataType.LONG:
        	kind = PigWarning.IMPLICIT_CAST_TO_LONG;
        	break;
        case DataType.MAP:
        	kind = PigWarning.IMPLICIT_CAST_TO_MAP;
        	break;
        case DataType.TUPLE:
        	kind = PigWarning.IMPLICIT_CAST_TO_TUPLE;
        	break;
        }
        msgCollector.collect(originalTypeName + " is implicitly cast to "
                             + toTypeName +" under " + opName + " Operator",
                             MessageType.Warning, kind) ;
    }

    /***
     * We need the neighbor to make sure that the new key is in the same scope
     */
    private OperatorKey genNewOperatorKey(LogicalOperator neighbor) {
        String scope = neighbor.getOperatorKey().getScope() ;
        long newId = NodeIdGenerator.getGenerator().getNextNodeId(scope) ;
        return new OperatorKey(scope, newId) ;
    }

    private FuncSpec getLoadFuncSpec(ExpressionOperator exOp) throws FrontendException {
        Schema.FieldSchema fs = ((ExpressionOperator)exOp).getFieldSchema();
        if(null == fs) {
            return null;
        }

        Map<String, LogicalOperator> canonicalMap = fs.getCanonicalMap();
        MultiMap<LogicalOperator, String> reverseCanonicalMap = fs.getReverseCanonicalMap();
        MultiMap<String, FuncSpec> loadFuncSpecMap = new MultiMap<String, FuncSpec>();
        
        if(canonicalMap.keySet().size() > 0) {
            for(String parentCanonicalName: canonicalMap.keySet()) {
                FuncSpec lfSpec = getLoadFuncSpec(exOp, parentCanonicalName);
                if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
            }
        } else {
            for(LogicalOperator op: reverseCanonicalMap.keySet()) {
                for(String parentCanonicalName: reverseCanonicalMap.get(op)) {
                    FuncSpec lfSpec = getLoadFuncSpec(op, parentCanonicalName);
                    if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
                }
            }
        }
        if(loadFuncSpecMap.keySet().size() == 0) {
            return null;
        }
        if(loadFuncSpecMap.keySet().size() == 1) {
            String lfString = loadFuncSpecMap.keySet().iterator().next();
            return loadFuncSpecMap.get(lfString).iterator().next();
        }

        {
            int errCode = 1065;
            String msg = "Found more than one load function to use: " + loadFuncSpecMap.keySet();
            throw new FrontendException(msg, errCode, PigException.INPUT);
        }
    }

    private FuncSpec getLoadFuncSpec(LogicalOperator op, String parentCanonicalName) throws FrontendException {
        MultiMap<String, FuncSpec> loadFuncSpecMap = new MultiMap<String, FuncSpec>();
        if(op instanceof ExpressionOperator) {
            if(op instanceof LOUserFunc) {
                return null;
            }
            
            Schema.FieldSchema fs = ((ExpressionOperator)op).getFieldSchema();
            Map<String, LogicalOperator> canonicalMap = fs.getCanonicalMap();
            MultiMap<LogicalOperator, String> reverseCanonicalMap = fs.getReverseCanonicalMap();
            
            if(canonicalMap.keySet().size() > 0) {
                for(String canonicalName: canonicalMap.keySet()) {
                    FuncSpec lfSpec = getLoadFuncSpec(fs, canonicalName);
                    if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
                }
            } else {
                for(LogicalOperator lop: reverseCanonicalMap.keySet()) {
                    for(String canonicalName: reverseCanonicalMap.get(lop)) {
                        FuncSpec lfSpec = getLoadFuncSpec(fs, canonicalName);
                        if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
                    }
                }
            }
        } else {
            if(op instanceof LOLoad) {
                return ((LOLoad)op).getInputFile().getFuncSpec();
            } else if (op instanceof LOStream) {
                StreamingCommand command = ((LOStream)op).getStreamingCommand();
                HandleSpec streamOutputSpec = command.getOutputSpec(); 
                FuncSpec streamLoaderSpec = new FuncSpec(streamOutputSpec.getSpec());
                return streamLoaderSpec;
            } else if ((op instanceof LOFilter)
                    || (op instanceof LODistinct)
                    || (op instanceof LOSort)
                    || (op instanceof LOSplit)
                    || (op instanceof LOSplitOutput)
                    || (op instanceof LOLimit)) {
                LogicalPlan lp = op.getPlan();
                return getLoadFuncSpec(lp.getPredecessors(op).get(0), parentCanonicalName);        
            }
            
            Schema s = op.getSchema();
            if(null != s) {
                for(Schema.FieldSchema fs: s.getFields()) {
                    if(null != parentCanonicalName && (parentCanonicalName.equals(fs.canonicalName))) {
                        if(fs.getCanonicalMap().keySet().size() > 0) {
                            for(String canonicalName: fs.getCanonicalMap().keySet()) {
                                FuncSpec lfSpec = getLoadFuncSpec(fs, canonicalName);
                                if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
                            }
                        } else {
                            FuncSpec lfSpec = getLoadFuncSpec(fs, null);
                            if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
                        }
                    } else if (null == parentCanonicalName) {
                        FuncSpec lfSpec = getLoadFuncSpec(fs, null);
                        if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
                    }
                }
            } else {
                LogicalPlan lp = op.getPlan();
                for(LogicalOperator pred: lp.getPredecessors(op)) {
                    FuncSpec lfSpec = getLoadFuncSpec(pred, parentCanonicalName);
                    if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
                }
            }
        }
        if(loadFuncSpecMap.keySet().size() == 0) {
            return null;
        }
        if(loadFuncSpecMap.keySet().size() == 1) {
            String lfString = loadFuncSpecMap.keySet().iterator().next();
            return loadFuncSpecMap.get(lfString).iterator().next();
        }
    
        {
            int errCode = 1065;
            String msg = "Found more than one load function to use: " + loadFuncSpecMap.keySet();
            throw new FrontendException(msg, errCode, PigException.INPUT);
        }
    }

    private FuncSpec getLoadFuncSpec(Schema.FieldSchema fs, String parentCanonicalName) throws FrontendException {
        if(null == fs) {
            return null;
        }
        Map<String, LogicalOperator> canonicalMap = fs.getCanonicalMap();
        MultiMap<LogicalOperator, String> reverseCanonicalMap = fs.getReverseCanonicalMap();
        MultiMap<String, FuncSpec> loadFuncSpecMap = new MultiMap<String, FuncSpec>();

        if(canonicalMap.keySet().size() > 0) {
            for(String canonicalName: canonicalMap.keySet()) {
                if((null == parentCanonicalName) || (parentCanonicalName.equals(canonicalName))) {
                    FuncSpec lfSpec = getLoadFuncSpec(canonicalMap.get(canonicalName), parentCanonicalName);
                    if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
                }
            }
        } else {
            for(LogicalOperator op: reverseCanonicalMap.keySet()) {
                for(String canonicalName: reverseCanonicalMap.get(op)) {
                    if((null == parentCanonicalName) || (parentCanonicalName.equals(canonicalName))) {
                        FuncSpec lfSpec = getLoadFuncSpec(op, parentCanonicalName);
                        if(null != lfSpec) loadFuncSpecMap.put(lfSpec.getClassName(), lfSpec);
                    }
                }
            }
        }
        if(loadFuncSpecMap.keySet().size() == 0) {
            return null;
        }
        if(loadFuncSpecMap.keySet().size() == 1) {
            String lfString = loadFuncSpecMap.keySet().iterator().next();
            return loadFuncSpecMap.get(lfString).iterator().next();
        }

        {
            int errCode = 1065;
            String msg = "Found more than one load function to use: " + loadFuncSpecMap.keySet();
            throw new FrontendException(msg, errCode, PigException.INPUT);
        }
    }


}
