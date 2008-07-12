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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;

import org.apache.pig.impl.logicalLayer.* ;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType ;
import org.apache.pig.impl.plan.NodeIdGenerator;
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

    private boolean strictMode = false ;

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

        LogicalPlan currentPlan =  (LogicalPlan) mCurrentWalker.getPlan() ;
        List<LogicalOperator> succList = currentPlan.getSuccessors(pj) ;
        List<LogicalOperator> predList = currentPlan.getPredecessors(pj) ;
        if((null != succList) && !(succList.get(0) instanceof ExpressionOperator)) {
            try {
                Schema.FieldSchema pjFs = pj.getFieldSchema();
                if(!DataType.isSchemaType(pj.getType())) {
                    Schema pjSchema = new Schema(pj.getFieldSchema());
                    pj.setFieldSchema(new Schema.FieldSchema(pj.getAlias(), pjSchema, DataType.TUPLE));
                } else {
                    pjFs.type = DataType.TUPLE;
                    pj.setFieldSchema(pjFs);
                }
                pj.setOverloaded(true);
                pj.setType(DataType.TUPLE);
            } catch (FrontendException fe) {
                String msg = "Error getting LOProject's input schema" ;
                msgCollector.collect(msg, MessageType.Error);
                VisitorException vse = new VisitorException(msg) ;
                vse.initCause(fe) ;
                throw new VisitorException(msg) ;
             }
        } else if(null != predList) {
            LogicalOperator projectInput = pj.getExpression();
            if(((projectInput instanceof LOProject) || !(predList.get(0) instanceof ExpressionOperator)) && (projectInput.getType() == DataType.BAG)) {
                try {
                    Schema.FieldSchema pjFs = pj.getFieldSchema();
                    if(!DataType.isSchemaType(pj.getType())) {
                        Schema pjSchema = new Schema(pj.getFieldSchema());
                        pj.setFieldSchema(new Schema.FieldSchema(pj.getAlias(), pjSchema, DataType.BAG));
                    } else {
                        pjFs.type = DataType.BAG;
                        pj.setFieldSchema(pjFs);
                    }
                    pj.setType(DataType.BAG);
                } catch (FrontendException fe) {
                    String msg = "Error getting LOProject's input schema" ;
                    msgCollector.collect(msg, MessageType.Error);
                    VisitorException vse = new VisitorException(msg) ;
                    vse.initCause(fe) ;
                    throw new VisitorException(msg) ;
                }
            }
        }
    }

    private void resolveLOProjectType(LOProject pj) throws VisitorException {

        try {
            pj.getFieldSchema() ;
        }
        catch (FrontendException fe) {
            String msg = "Error getting LOProject's input schema" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;

        }

        /*
        if (!pj.getSentinel()) {

            LogicalOperator op = pj.getExpression() ;

            if (!(op instanceof LOProject)) {
                throw new AssertionError("LOProject.getExpression() has to be "
                                         + "LOProject if it's not a sentinel") ;
            }

            // else
            LOProject innerProject = (LOProject) op ;
            resolveLOProjectType(innerProject) ;

            if ( (innerProject.getType() != DataType.BAG) &&
                 (innerProject.getType() != DataType.TUPLE) ) {
                throw new AssertionError("Nested LOProject is for extracting "
                                         + " from TUPLE/BAG only") ;
            }

            // set type of this project
            pj.setType(innerProject.getType());
            Schema inputSchema = null ;

            try {
                inputSchema = innerProject.getSchema() ;
            }
            catch (FrontendException fe) {
                String msg = "Cannot get source schema into LOProject" ;
                msgCollector.collect(msg, MessageType.Error);
                VisitorException vse = new VisitorException(msg) ;
                vse.initCause(fe) ;
                throw new VisitorException(msg) ;
            }

            // extracting schema from projection
            List<FieldSchema> fsList = new ArrayList<FieldSchema>() ;
            try {
                for(int index: pj.getProjection()) {
                    FieldSchema fs = null ;
                    // typed input
                    if (inputSchema != null) {
                        fs = inputSchema.getField(index) ;
                        FieldSchema newFs = new FieldSchema(fs.alias, fs.schema, fs.type) ;
                        fsList.add(newFs) ;
                    }
                    // non-typed input
                    else {
                        FieldSchema newFs = new FieldSchema(null, DataType.BYTEARRAY) ;
                        fsList.add(newFs) ;
                    }
                }
                pj.setFieldSchema(new FieldSchema(null, new Schema(fsList), innerProject.getType()));
            }
            catch (FrontendException fe) {
                String msg = "Cannot get source schema into LOProject" ;
                msgCollector.collect(msg, MessageType.Error);
                VisitorException vse = new VisitorException(msg) ;
                vse.initCause(fe) ;
                throw new VisitorException(msg) ;
            }
            catch (ParseException pe) {
                String msg = "Cannot get source schema into LOProject" ;
                msgCollector.collect(msg, MessageType.Error);
                VisitorException vse = new VisitorException(msg) ;
                vse.initCause(pe) ;
                throw new VisitorException(msg) ;
            }
        }
        // if it's a sentinel, we just get the projected input type to it
        else {
            if (pj.getProjection().size() != 1) {
                throw new AssertionError("Sentinel LOProject can have only "
                                         + "1 projection") ;
            }
            LogicalOperator input = pj.getExpression() ;
            int projectedField = pj.getProjection().get(0) ;
            try {
                Schema schema = input.getSchema() ;

                if (schema != null) {
                    FieldSchema fs = schema.getField(projectedField) ;
                    pj.setFieldSchema(fs);
                }
                else {
                    FieldSchema fs = new FieldSchema(null, DataType.BYTEARRAY) ;
                    pj.setFieldSchema(fs);
                }
            }
            catch (FrontendException fe) {
                String msg = "Cannot get source schema into LOProject" ;
                msgCollector.collect(msg, MessageType.Error);
                VisitorException vse = new VisitorException(msg) ;
                vse.initCause(fe) ;
                throw new VisitorException(msg) ;
            }
            catch (ParseException pe) {
                String msg = "Cannot get source schema into LOProject" ;
                msgCollector.collect(msg, MessageType.Error);
                VisitorException vse = new VisitorException(msg) ;
                vse.initCause(pe) ;
                throw new VisitorException(msg) ;
            }
        }
        */
        
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
            String msg = "Map key should be a basic type" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
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

    public void visit(LOAnd binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);

        if (  (lhsType != DataType.BOOLEAN)  ||
              (rhsType != DataType.BOOLEAN)  ) {
            String msg = "Operands of AND/OR can be boolean only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }

    }

    @Override
    public void visit(LOOr binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);

        if (  (lhsType != DataType.BOOLEAN)  ||
              (rhsType != DataType.BOOLEAN)  ) {
            String msg = "Operands of AND/OR can be boolean only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LOMultiply binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);

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
            fs.type = biggerType;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(rhsType)) ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
            // Set output type
            fs.type = rhsType;
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(lhsType)) ) {
            insertRightCastForBinaryOp(binOp, lhsType) ;
            // Set output type
            fs.type = lhsType;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (rhsType == DataType.BYTEARRAY) ) {
            // Cast both operands to double
            insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
            insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
            // Set output type
            fs.type = DataType.DOUBLE;
        }
        else {
            String msg = "Cannot evaluate output type of Mul/Div Operator" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LODivide binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);

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
            fs.type = biggerType;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(rhsType)) ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
            // Set output type
            fs.type = rhsType;
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(lhsType)) ) {
            insertRightCastForBinaryOp(binOp, lhsType) ;
            // Set output type
            fs.type = lhsType;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (rhsType == DataType.BYTEARRAY) ) {
            // Cast both operands to double
            insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
            insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
            // Set output type
            fs.type = DataType.DOUBLE;
        }
        else {
            String msg = "Cannot evaluate output type of Mul/Div Operator" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LOAdd binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);

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
            fs.type = biggerType;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(rhsType)) ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
            // Set output type
            fs.type = rhsType;
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(lhsType)) ) {
            insertRightCastForBinaryOp(binOp, lhsType) ;
            // Set output type
            fs.type = lhsType;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (rhsType == DataType.BYTEARRAY) ) {
            // Cast both operands to double
            insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
            insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
            // Set output type
            fs.type = DataType.DOUBLE;
        }
        else {
            String msg = "Cannot evaluate output type of Add/Subtract Operator" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }
        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LOSubtract binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);

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
            fs.type = biggerType;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                (DataType.isNumberType(rhsType)) ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
            // Set output type
            fs.type = rhsType;
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                  (DataType.isNumberType(lhsType)) ) {
            insertRightCastForBinaryOp(binOp, lhsType) ;
            // Set output type
            fs.type = lhsType;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  (rhsType == DataType.BYTEARRAY) ) {
            // Cast both operands to double
            insertLeftCastForBinaryOp(binOp, DataType.DOUBLE) ;
            insertRightCastForBinaryOp(binOp, DataType.DOUBLE) ;
            // Set output type
            fs.type = DataType.DOUBLE;
        }
        else {
            String msg = "Cannot evaluate output type of Add/Subtract Operator" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }
        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }



    @Override
    public void visit(LOGreaterThan binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);

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
            String msg = "Cannot evaluate output type of "
                            + binOp.getClass().getSimpleName()
                            + " LHS:" + DataType.findTypeName(lhsType)
                            + " RHS:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LOGreaterThanEqual binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);

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
            String msg = "Cannot evaluate output type of "
                            + binOp.getClass().getSimpleName()
                            + " LHS:" + DataType.findTypeName(lhsType)
                            + " RHS:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LOLesserThan binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);
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
            String msg = "Cannot evaluate output type of "
                            + binOp.getClass().getSimpleName()
                            + " LHS:" + DataType.findTypeName(lhsType)
                            + " RHS:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LOLesserThanEqual binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);

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
            String msg = "Cannot evaluate output type of "
                            + binOp.getClass().getSimpleName()
                            + " LHS:" + DataType.findTypeName(lhsType)
                            + " RHS:" + DataType.findTypeName(rhsType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }



    @Override
    public void visit(LOEqual binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);

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

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LONotEqual binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);


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

        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LOMod binOp) throws VisitorException {
        ExpressionOperator lhs = binOp.getLhsOperand() ;
        ExpressionOperator rhs = binOp.getRhsOperand() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);

        if ( (lhsType == DataType.INTEGER) &&
             (rhsType == DataType.INTEGER)
           ) {
            fs.type = DataType.INTEGER;
        }
        else if ( (lhsType == DataType.LONG) &&
                  ( (rhsType == DataType.INTEGER) || (rhsType == DataType.LONG) )
                ) {
            if (rhsType == DataType.INTEGER) {
                insertRightCastForBinaryOp(binOp, DataType.LONG) ;
            }
            fs.type = DataType.LONG;
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                  ( (rhsType == DataType.INTEGER) || (rhsType == DataType.LONG) )
                ) {
            insertLeftCastForBinaryOp(binOp, rhsType) ;
            fs.type = rhsType;
        }
        else {
            String msg = "Cannot evaluate output type of Mod Operator" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }
        try {
            binOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }


    @Override
    public void visit(LONegative uniOp) throws VisitorException {
        byte type = uniOp.getOperand().getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);


        if (DataType.isNumberType(type)) {
            fs.type = type;
        }
        else if (type == DataType.BYTEARRAY) {
            insertCastForUniOp(uniOp, DataType.DOUBLE) ;
            fs.type = DataType.DOUBLE;
        }
        else {
            String msg = "NEG can be used with numbers or Bytearray only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }

        try {
            uniOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }
    
    @Override
    public void visit(LONot uniOp) throws VisitorException {
        byte type = uniOp.getOperand().getType() ;
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);

        if (type != DataType.BOOLEAN) {
            String msg = "NOT can be used with boolean only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new VisitorException(msg) ;
        }

        try {
            uniOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw new VisitorException(msg) ;
        }
    }

    @Override
    public void visit(LOIsNull uniOp) throws VisitorException {
        Schema.FieldSchema fs = new Schema.FieldSchema(null, DataType.BOOLEAN);
        try {
            uniOp.setFieldSchema(fs);
        } catch (FrontendException fe) {
            String msg = "Could not set LOSubtract field schema";
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
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
        Schema s = new Schema();
        for(ExpressionOperator op: list) {
            if (!DataType.isUsableType(op.getType())) {
                String msg = "Problem with input of User-defined function" ;
                msgCollector.collect(msg, MessageType.Error);
                throw new VisitorException(msg) ;
            }
            try {
                s.add(op.getFieldSchema());    
            } catch (FrontendException e) {
                throw new VisitorException(e);
            }
            
        }
        
        // ask the EvalFunc what types of inputs it can handle
        EvalFunc<?> ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(func.getFuncSpec());
        List<FuncSpec> funcSpecs = null;
        try {
            funcSpecs = ef.getArgToFuncMapping();    
        } catch (Exception e) {
            throw new VisitorException(e);
        }
        
        if(funcSpecs != null) {
            // check the if a FuncSpec matching our schema exists
            FuncSpec matchingSpec = null;
            for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator.hasNext();) {
                FuncSpec fs = iterator.next();
                if(Schema.equals(s, fs.getInputArgsSchema(), false, true)) {
                    matchingSpec = fs;
                }
                
            }
            if(matchingSpec == null) {
                StringBuilder sb = new StringBuilder();
                sb.append(func.getFuncSpec());
                sb.append("does not work with inputs of type ");
                sb.append(s);
                throw new VisitorException(sb.toString());
            } else {
                func.setFuncSpec(matchingSpec);
                ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(matchingSpec);
                func.setType(DataType.findType(ef.getReturnType()));
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
        Schema.FieldSchema castFs;
        Schema.FieldSchema inputFs;
        try {
            castFs = cast.getFieldSchema();
            inputFs = cast.getExpression().getFieldSchema();
        } catch(FrontendException fee) {
            throw new VisitorException(fee.getMessage());
        }
        boolean castable = castable = Schema.FieldSchema.castable(castFs, inputFs);
        if(!castable) {
            String msg = "Cannot cast "
                           + DataType.findTypeName(inputType)
                           + ((DataType.isSchemaType(inputType))? " with schema " + inputFs : "")
                           + " to "
                           + DataType.findTypeName(expectedType)
                           + ((DataType.isSchemaType(expectedType))? " with schema " + castFs : "");
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
                        String msg = "cannot merge schemas from inputs of UNION" ;
                        msgCollector.collect(msg, MessageType.Error) ;
                        throw new VisitorException(msg) ;
                    }
                }
            }

            // Compute the schema
            schema = u.getSchema() ;

        }
        catch (FrontendException fee) {
            String msg = "Problem while reading schemas from inputs of UNION" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fee) ;
            throw vse ;
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
                    try {
                        this.visit(insertedOp);
                    }
                    catch (FrontendException fee) {
                        String msg = "Problem while casting inputs of UNION" ;
                        msgCollector.collect(msg, MessageType.Error) ;
                        VisitorException vse = new VisitorException(msg) ;
                        //vse.initCause(fee) ;
                        throw vse ;
                    }
                }
            }
        }
    }

    @Override
    protected void visit(LOSplitOutput op) throws VisitorException {
        LogicalPlan currentPlan =  mCurrentWalker.getPlan() ;

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
            
        checkInnerPlan(condPlan) ;
                 
        byte innerCondType = condPlan.getLeaves().get(0).getType() ;
        if (innerCondType != DataType.BOOLEAN) {
            String msg = "Split's condition must evaluate to boolean" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }

        try {
            // Compute the schema
            op.getSchema() ;
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
        LogicalPlan currentPlan = mCurrentWalker.getPlan() ;
        List<LogicalOperator> list = currentPlan.getPredecessors(op) ;

        // LOSplitOutput can only have 1 input
        try {
            // Compute the schema
            op.getSchema() ;
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
     * If one of the inputs have no schema then we cannot construct
     * the output schema.
     * @param cs
     * @throws VisitorException
     */
    protected void visit(LOCross cs) throws VisitorException {
        List<LogicalOperator> inputs = cs.getInputs() ;
        List<FieldSchema> fsList = new ArrayList<FieldSchema>() ;

        try {
            // Compute the schema
            cs.getSchema() ;

            boolean foundNullSchema = false ;
            for(LogicalOperator op: inputs) {
                // All of inputs are relational operators
                // so we can access getSchema()
                Schema inputSchema = op.getSchema() ;
                if (inputSchema == null) {
                    // force to null if one input has null schema
                    cs.forceSchema(null);
                    break ;
                }

            }

        }
        catch (FrontendException fe) {
            String msg = "Problem while reading"
                        + " schemas from inputs of CROSS" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
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

            checkInnerPlan(sortColPlan) ;
            // TODO: May have to check SortFunc compatibility here in the future
                       
        }
        
        s.setType(input.getType()) ;  // This should be bag always.

        try {
            // Compute the schema
            s.getSchema() ;
        }
        catch (FrontendException ioe) {
            String msg = "Problem while reconciling output schema of LOSort" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(ioe) ;
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

        checkInnerPlan(comparisonPlan) ;
              
        byte innerCondType = comparisonPlan.getLeaves().get(0).getType() ;
        if (innerCondType != DataType.BOOLEAN) {
            String msg = "Filter's condition must evaluate to boolean" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new VisitorException(msg) ;
        }       


        try {
            // Compute the schema
            filter.getSchema() ;
        } 
        catch (FrontendException ioe) {
            String msg = "Problem while reconciling output schema of LOFilter" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(ioe) ;
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
        
        try {
            // Compute the schema
            split.getSchema() ;
        }
        catch (FrontendException ioe) {
            String msg = "Problem while reconciling output schema of LOSplit" ;
            msgCollector.collect(msg, MessageType.Error);
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(ioe) ;
            throw vse ;
        }
    }

    /**
     * COGroup
     * All group by cols from all inputs have to be of the
     * same type
     */
    protected void visit(LOCogroup cg) throws VisitorException {
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

                checkInnerPlan(innerPlans.get(j)) ;
            }

        }

        Schema schema = null ;
        try {
            // Compute the schema
            schema = cg.getSchema() ;

            if (!cg.isTupleGroupCol()) {
                // merge all the inner plan outputs so we know what type
                // our group column should be

                // TODO: Don't recompute schema here
                //byte groupType = schema.getField(0).type ;
                byte groupType = getAtomicGroupByType(cg) ;

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
                Schema groupBySchema = getTupleGroupBySchema(cg) ;

                // go through all inputs again to add cast if necessary
                for(int i=0;i < inputs.size(); i++) {
                    LogicalOperator input = inputs.get(i) ;
                    List<LogicalPlan> innerPlans
                                = new ArrayList<LogicalPlan>(groupByPlans.get(input)) ;
                    for(int j=0;j < innerPlans.size(); j++) {
                        LogicalPlan innerPlan = innerPlans.get(j) ;
                        byte innerType = innerPlan.getSingleLeafPlanOutputType() ;
                        byte expectedType = DataType.BYTEARRAY ;

                        if (!DataType.isAtomic(innerType)) {
                            String msg = "Sorry, group by complex types"
                                       + " will be supported soon" ;
                            msgCollector.collect(msg, MessageType.Error) ;
                            VisitorException vse = new VisitorException(msg) ;
                            throw vse ;
                        }

                        try {
                            expectedType = groupBySchema.getField(j).type ;
                        }
                        catch(ParseException pe) {
                            String msg = "Cannot resolve COGroup output schema" ;
                            msgCollector.collect(msg, MessageType.Error) ;
                            VisitorException vse = new VisitorException(msg) ;
                            vse.initCause(pe) ;
                            throw vse ;
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
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(fe) ;
            throw vse ;
        }
        /*
        catch (ParseException pe) {
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            vse.initCause(pe) ;
            throw vse ;
        }
        */


        // TODO: Don't recompute schema here. Remove all from here!
        // Generate output schema based on the schema generated from
        // COGroup itself

        try {

            Schema outputSchema = cg.getSchema() ;

            // if the "group" col is atomic
            if (!cg.isTupleGroupCol()) {
                outputSchema.getField(0).type = getAtomicGroupByType(cg) ;
            }
            else {
                outputSchema.getField(0).type = DataType.TUPLE ;
                outputSchema.getField(0).schema = getTupleGroupBySchema(cg) ;
            }

            for(int i=0; i< inputs.size(); i++) {
                FieldSchema fs = outputSchema.getField(i+1) ;
                fs.type = DataType.BAG ;
                fs.schema = inputs.get(i).getSchema() ;
            }

            cg.setType(DataType.BAG) ;
            cg.setSchema(outputSchema) ;

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


    // This helps insert casting to atomic types in COGroup's inner plans
    // as a new leave of the plan
    private void insertAtomicCastForCOGroupInnerPlan(LogicalPlan innerPlan,
                                                     LOCogroup cg,
                                                     byte toType) {
        List<LogicalOperator> leaves = innerPlan.getLeaves() ;
        if (leaves.size() > 1) {
            throw new AssertionError("insertAtomicForCOGroupInnerPlan cannot be"
                                + " used when there is more than 1 output port") ;
        }
        ExpressionOperator currentOutput = (ExpressionOperator) leaves.get(0) ;
        collectCastWarning(cg, currentOutput.getType(), toType) ;
        OperatorKey newKey = genNewOperatorKey(currentOutput) ;
        LOCast cast = new LOCast(innerPlan, newKey, currentOutput, toType) ;
        innerPlan.add(cast) ;
        try {
            innerPlan.connect(currentOutput, cast) ;
        }
        catch (PlanException ioe) {
            AssertionError err =  new AssertionError("Explicit casting insertion") ;
            err.initCause(ioe) ;
            throw err ;
        }
    }

    /**
     * This can be used to get the merged type of output group col
     * only when the group col is of atomic type
     * TODO: This doesn't work with group by complex type
     * @return The type of the group by
     */
    public byte getAtomicGroupByType(LOCogroup cg) throws VisitorException {
        if (cg.isTupleGroupCol()) {
            throw new AssertionError("getAtomicGroupByType is used only when"
                                     + " dealing with atomic group col") ;
        }
        byte groupType = DataType.BYTEARRAY ;
        // merge all the inner plan outputs so we know what type
        // our group column should be
        for(int i=0;i < cg.getInputs().size(); i++) {
            LogicalOperator input = cg.getInputs().get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(cg.getGroupByPlans().get(input)) ;
            if (innerPlans.size() != 1) {
                throw new AssertionError("Each COGroup input has to have "
                                         + "the same number of inner plans") ;
            }
            byte innerType = innerPlans.get(0).getSingleLeafPlanOutputType() ;
            groupType = DataType.mergeType(groupType, innerType) ;
            if (groupType == DataType.ERROR) {
                // We just warn about mismatch type in non-strict mode
                if (!strictMode) {
                    String msg = "COGroup by incompatible types results in ByteArray" ;
                    msgCollector.collect(msg, MessageType.Warning) ;
                    groupType = DataType.BYTEARRAY ;
                }
                // We just die if in strict mode
                else {
                    String msg = "COGroup by incompatible types" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new VisitorException(msg) ;
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
            throw new AssertionError("getTupleGroupBySchema is used only when"
                                     + " dealing with tuple group col") ;
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
                        msgCollector.collect(msg, MessageType.Warning) ;
                        fsList.get(j).type = DataType.BYTEARRAY ;
                    }
                    // We just die if in strict mode
                    else {
                        String msg = "COGroup by incompatible types" ;
                        msgCollector.collect(msg, MessageType.Error) ;
                        throw new VisitorException(msg) ;
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

        try {

            // Have to resolve all inner plans before calling getSchema
            int outputSchemaIdx = 0 ;
            for(int i=0;i < plans.size(); i++) {

                LogicalPlan plan = plans.get(i) ;

                // Check that the inner plan has only 1 output port
                if (!plan.isSingleLeafPlan()) {
                    String msg = "Generate's expression plan can "
                                 + " only have one output (leaf)" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new VisitorException(msg) ;
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
                        throw new AssertionError("Unsupported root type in "
                            +"LOForEach:" + innerRoot.getClass().getSimpleName()) ;
                    }
                }

                checkInnerPlan(plan) ;

            }

            f.setSchemaComputed(false);
            f.getSchema();

        }
        catch (FrontendException pe) {
            String msg = "Problem resolving LOForEach schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            VisitorException vse = new VisitorException(msg) ;
            throw vse ;
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
            throw new AssertionError("Inner plan is poorly constructed") ;
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
            else {
                String msg = "Unsupported root operator in inner plan:"
                             + op.getClass().getSimpleName() ;
                throw new AssertionError(msg) ;
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
                AssertionError err = new AssertionError("Problem wiring the plan while insert casting") ;
                err.initCause(pe) ;
                throw err ;
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

}
