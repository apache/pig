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
package org.apache.pig.newplan.logical.visitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.EvalFunc.SchemaType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckerException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AllSameExpressionVisitor;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class TypeCheckingExpVisitor extends LogicalExpressionVisitor{

    private CompilationMessageCollector msgCollector;
    private LogicalRelationalOperator currentRelOp;
    private static final int INF = -1;

    public TypeCheckingExpVisitor(
            OperatorPlan expPlan,
            CompilationMessageCollector msgCollector,
            LogicalRelationalOperator relOp
    )
    throws FrontendException {

        super(expPlan, new ReverseDependencyOrderWalker(expPlan));
        this.msgCollector = msgCollector;
        this.currentRelOp = relOp;
        //reset field schema of all expression operators because
        // it needs to be re-evaluated after correct types are set
        FieldSchemaResetter sr = new FieldSchemaResetter(expPlan);
        sr.visit();

    }

    @Override
    public void visit(AddExpression binOp) throws FrontendException {
        addCastsToNumericBinExpression(binOp);
    }

    @Override
    public void visit(SubtractExpression binOp) throws FrontendException {
        addCastsToNumericBinExpression(binOp);
    }

    @Override
    public void visit(MultiplyExpression binOp) throws FrontendException {
        addCastsToNumericBinExpression(binOp);
    }

    @Override
    public void visit(DivideExpression binOp) throws FrontendException {
        addCastsToNumericBinExpression(binOp);
    }

    /**
     * Add casts to promote numeric type to larger of two input numeric types of
     * the {@link BinaryExpression}  binOp . If one of the inputs is numeric
     * and other bytearray, cast the bytearray type to other numeric type.
     * If both inputs are bytearray, cast them to double.
     * @param binOp
     * @throws FrontendException
     */
    private void addCastsToNumericBinExpression(BinaryExpression binOp)
    throws FrontendException {
        LogicalExpression lhs = binOp.getLhs() ;
        LogicalExpression rhs = binOp.getRhs() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;

        if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {

            // return the bigger type
            byte biggerType = lhsType > rhsType ? lhsType:rhsType ;

            // Cast smaller type to the bigger type
            if (lhsType != biggerType) {
                insertCast(binOp, biggerType, binOp.getLhs());
            }
            else if (rhsType != biggerType) {
                insertCast(binOp, biggerType, binOp.getRhs());
            }
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                (DataType.isNumberType(rhsType)) ) {
            insertCast(binOp, rhsType, binOp.getLhs());
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                (DataType.isNumberType(lhsType)) ) {
            insertCast(binOp, lhsType, binOp.getRhs());
        }
        else if ( (lhsType == DataType.BYTEARRAY) &&
                (rhsType == DataType.BYTEARRAY) ) {
            // Cast both operands to double
            insertCast(binOp, DataType.DOUBLE, binOp.getLhs());
            insertCast(binOp, DataType.DOUBLE, binOp.getRhs());
        }
        else {
            int errCode = 1039;
            String msg = generateIncompatibleTypesMessage(binOp);
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(binOp, msg, errCode, PigException.INPUT) ;
        }

    }

    @Override
    public void visit(ModExpression binOp) throws FrontendException {
        LogicalExpression lhs = binOp.getLhs() ;
        LogicalExpression rhs = binOp.getRhs() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        boolean error = false;

        if (lhsType == DataType.INTEGER) {
            if (rhsType == DataType.INTEGER) {
                //do nothing
            } else if (rhsType == DataType.LONG || rhsType == DataType.BIGINTEGER) {
                insertCast(binOp, rhsType, binOp.getLhs());
            } else {
                error = true;
            }
        } else if (lhsType == DataType.LONG) {
            if (rhsType == DataType.INTEGER) {
                insertCast(binOp, lhsType, binOp.getRhs());
            } else if (rhsType == DataType.BIGINTEGER) {
                insertCast(binOp, rhsType, binOp.getLhs());
            } else if (rhsType == DataType.LONG) {
                //do nothing
            } else {
                error = true;
            }
        } else if (lhsType == DataType.BIGINTEGER) {
            if (rhsType == DataType.INTEGER || rhsType == DataType.LONG) {
                insertCast(binOp, lhsType, binOp.getRhs());
            } else if (rhsType == DataType.BIGINTEGER) {
                //do nothing
            } else {
                error = true;
            }
        } else if (lhsType == DataType.BYTEARRAY) {
            if (rhsType == DataType.INTEGER || rhsType == DataType.LONG || rhsType == DataType.BIGINTEGER) {
                insertCast(binOp, rhsType, binOp.getLhs());
            } else {
                error = true;
            }
        } else {
            error = true;
        }
        if (error) {
            int errCode = 1039;
            String msg = generateIncompatibleTypesMessage(binOp);
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(binOp, msg, errCode, PigException.INPUT) ;
        }
    }

    private String generateIncompatibleTypesMessage(BinaryExpression binOp)
    throws FrontendException {
        String msg = binOp.toString();
        if (currentRelOp.getAlias()!=null){
            msg = "In alias " + currentRelOp.getAlias() + ", ";
        }
        LogicalFieldSchema lhsFs = binOp.getLhs().getFieldSchema();
        LogicalFieldSchema rhsFs = binOp.getRhs().getFieldSchema();

        msg = msg + "incompatible types in " + binOp.getName() + " Operator"
        + " left hand side:" + DataType.findTypeName(lhsFs.type)
        + (lhsFs.schema == null ? "" : " " + lhsFs.schema.toString(false) + " ")
        + " right hand side:" + DataType.findTypeName(rhsFs.type)
        + (rhsFs.schema == null ? "" : " " + rhsFs.schema.toString(false) + " ") ;
        return msg;
    }

    @Override
    public void visit(NegativeExpression negExp) throws FrontendException {
        byte type = negExp.getExpression().getType() ;
        if (DataType.isNumberType(type)) {
            //do nothing
        }
        else if (type == DataType.BYTEARRAY) {
            // cast bytearray to double
            insertCast(negExp, DataType.DOUBLE, negExp.getExpression());
        }
        else {
            int errCode = 1041;
            String msg = "NEG can be used with numbers or Bytearray only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(negExp, msg, errCode, PigException.INPUT) ;
        }

    }

    @Override
    public void visit(NotExpression notExp) throws FrontendException {
        if (notExp.getExpression()  instanceof ConstantExpression
                && ((ConstantExpression) notExp.getExpression()).getValue() == null) {
            insertCast(notExp, DataType.BOOLEAN, notExp.getExpression());
        }
        byte type = notExp.getExpression().getType();
        if (type != DataType.BOOLEAN) {
            int errCode = 1042;
            String msg = "NOT can be used with boolean only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException( notExp, msg, errCode, PigException.INPUT) ;
        }

    }

    @Override
    public void visit(OrExpression orExp) throws FrontendException {
        visitBooleanBinary(orExp);
    }

    @Override
    public void visit(AndExpression andExp) throws FrontendException {
        visitBooleanBinary(andExp);
    }

    private void visitBooleanBinary(BinaryExpression boolExp)
    throws FrontendException {
        // if lhs or rhs is null constant then cast it to boolean
        insertCastsForNullToBoolean(boolExp);
        LogicalExpression lhs = boolExp.getLhs();
        LogicalExpression rhs = boolExp.getRhs();

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;

        if (  (lhsType != DataType.BOOLEAN)  ||
                (rhsType != DataType.BOOLEAN)  ) {
            int errCode = 1038;
            String msg = "Operands of AND/OR can be boolean only" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(boolExp, msg, errCode, PigException.INPUT) ;
        }
    }

    @Override
    public void visit(LessThanExpression binOp)
    throws FrontendException {
        addCastsToCompareBinaryExp(binOp, false /*not equality op*/);
    }

    @Override
    public void visit(LessThanEqualExpression binOp)
    throws FrontendException {
        addCastsToCompareBinaryExp(binOp, false /*not equality op*/);
    }


    @Override
    public void visit(GreaterThanExpression binOp)
    throws FrontendException {
        addCastsToCompareBinaryExp(binOp, false /*not equality op*/);
    }

    @Override
    public void visit(GreaterThanEqualExpression binOp)
    throws FrontendException {
        addCastsToCompareBinaryExp(binOp, false /*not equality op*/);
    }


    @Override
    public void visit(EqualExpression binOp)
    throws FrontendException {
        addCastsToCompareBinaryExp(binOp, true /*equality op*/);
    }

    @Override
    public void visit(NotEqualExpression binOp)
    throws FrontendException {
        addCastsToCompareBinaryExp(binOp, true /*equality op*/);
    }

    private void addCastsToCompareBinaryExp(BinaryExpression binOp, boolean isEquality)
    throws FrontendException {
        LogicalExpression lhs = binOp.getLhs() ;
        LogicalExpression rhs = binOp.getRhs() ;

        byte lhsType = lhs.getType() ;
        byte rhsType = rhs.getType() ;
        if ( DataType.isNumberType(lhsType) &&
                DataType.isNumberType(rhsType) ) {
            // If not the same type, we cast them to the same
            byte biggerType = lhsType > rhsType ? lhsType:rhsType ;

            // Cast smaller type to the bigger type
            if (lhsType != biggerType) {
                insertCast(binOp, biggerType, binOp.getLhs());
            }
            else if (rhsType != biggerType) {
                insertCast(binOp, biggerType, binOp.getRhs());
            }
        }
        else if ( (lhsType == DataType.DATETIME) &&
                (rhsType == DataType.DATETIME) ) {
            // good
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
                ( (rhsType == DataType.CHARARRAY) || (DataType.isNumberType(rhsType)) || (rhsType == DataType.BOOLEAN) || (rhsType == DataType.DATETIME))
        ) {
            // Cast byte array to the type on rhs
            insertCast(binOp, rhsType, binOp.getLhs());
        }
        else if ( (rhsType == DataType.BYTEARRAY) &&
                ( (lhsType == DataType.CHARARRAY) || (DataType.isNumberType(lhsType)) || (lhsType == DataType.BOOLEAN) || (lhsType == DataType.DATETIME))
        ) {
            // Cast byte array to the type on lhs
            insertCast(binOp, lhsType, binOp.getRhs());
        }else if (isEquality){

            //in case of equality condition, allow boolean, tuples and maps as args
            if((lhsType == DataType.BOOLEAN) &&
                    (rhsType == DataType.BOOLEAN) ) {
                // good
            }
            else if((lhsType == DataType.TUPLE) &&
                    (rhsType == DataType.TUPLE) ) {
                // good
            }
            else if ( (lhsType == DataType.MAP) &&
                    (rhsType == DataType.MAP) ) {
                // good
            }
            else if (lhsType == DataType.BYTEARRAY &&
                    (rhsType == DataType.MAP || rhsType == DataType.TUPLE)){
                // Cast byte array to the type on lhs
                insertCast(binOp, rhsType, binOp.getLhs());
            }
            else if(rhsType == DataType.BYTEARRAY &&
                    (lhsType == DataType.MAP || lhsType == DataType.TUPLE)){
                // Cast byte array to the type on lhs
                insertCast(binOp, lhsType, binOp.getRhs());
            }
            else {
                throwIncompatibleTypeError(binOp);
            }
        }
        else {
            throwIncompatibleTypeError(binOp);
        }
    }

    private void throwIncompatibleTypeError(BinaryExpression binOp)
    throws FrontendException {
        int errCode = 1039;
        String msg = generateIncompatibleTypesMessage(binOp);
        msgCollector.collect(msg, MessageType.Error) ;
        throw new TypeCheckerException(binOp, msg, errCode, PigException.INPUT);
    }

    private void insertCastsForNullToBoolean(BinaryExpression binOp)
    throws FrontendException {
        if (binOp.getLhs() instanceof ConstantExpression
                && ((ConstantExpression) binOp.getLhs()).getValue() == null)
            insertCast(binOp, DataType.BOOLEAN, binOp.getLhs());
        if (binOp.getRhs() instanceof ConstantExpression
                && ((ConstantExpression) binOp.getRhs()).getValue() == null)
            insertCast(binOp, DataType.BOOLEAN, binOp.getRhs());
    }

    /**
     * add cast to convert the input of exp
     *  {@link LogicalExpression} arg to type toType
     * @param exp
     * @param toType
     * @param arg
     * @throws FrontendException
     */
    private void insertCast(LogicalExpression exp, byte toType, LogicalExpression arg)
    throws FrontendException {
        LogicalFieldSchema toFs = new LogicalSchema.LogicalFieldSchema(null, null, toType);
        insertCast(exp, toFs, arg);
    }

    private void insertCast(LogicalExpression node, LogicalFieldSchema toFs,
            LogicalExpression arg)
    throws FrontendException {
        collectCastWarning(node, arg.getType(), toFs.type, msgCollector);

        CastExpression cast = new CastExpression(plan, arg, toFs);
        try {
            // disconnect cast and arg because the connection is already
            // added by cast constructor and insertBetween call is going
            // to do it again
            plan.disconnect(cast, arg);
            plan.insertBetween(node, cast, arg);
        }
        catch (PlanException pe) {
            int errCode = 2059;
            String msg = "Problem with inserting cast operator for " + node + " in plan.";
            throw new TypeCheckerException(arg, msg, errCode, PigException.BUG, pe);
        }
        this.visit(cast);
    }


    /**
     * For Basic Types:
     * 0) Casting to itself is always ok
     * 1) Casting from number to number is always ok
     * 2) ByteArray to anything is ok
     * 3) number to chararray is ok
     * For Composite Types:
     * Recursively traverse the schemas till you get a basic type
     * @throws FrontendException
     */
    @Override
    public void visit(CastExpression cast) throws FrontendException {
        byte inType = cast.getExpression().getType();
        byte outType = cast.getType();
        if(outType == DataType.BYTEARRAY){
            int errCode = 1051;
            String msg = "Cannot cast to bytearray";
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(cast, msg, errCode, PigException.INPUT) ;
        }

        LogicalFieldSchema inFs = cast.getExpression().getFieldSchema();
        LogicalFieldSchema outFs = cast.getFieldSchema();

        if(inFs == null){
            //replace null schema with bytearray schema.
            inFs = new LogicalFieldSchema(null, null, DataType.BYTEARRAY);
        }

        //check if the field schemas are castable
        boolean castable = LogicalFieldSchema.castable(inFs, outFs);
        if(!castable) {
            int errCode = 1052;
            String msg = "Cannot cast "
                           + DataType.findTypeName(inType)
                           + ((DataType.isSchemaType(inType))? " with schema " + inFs.toString(false) : "")
                           + " to "
                           + DataType.findTypeName(outType)
                           + ((DataType.isSchemaType(outType))? " with schema " + outFs.toString(false) : "");
            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(cast, msg, errCode, PigException.INPUT) ;
        }

    }



    /**
     * {@link RegexExpression} expects CharArray as input
     * Itself always returns Boolean
     * @param rg
     * @throws FrontendException
     */
    @Override
    public void visit(RegexExpression rg) throws FrontendException {
        // We allow BYTEARRAY to be converted to CHARARRAY
        if (rg.getLhs().getType() == DataType.BYTEARRAY){
            insertCast(rg, DataType.CHARARRAY, rg.getLhs());
        }
        if (rg.getRhs().getType() == DataType.BYTEARRAY){
            insertCast(rg, DataType.CHARARRAY, rg.getRhs());
        }

        // Other than that if it's not CharArray just say goodbye
        if (rg.getLhs().getType() != DataType.CHARARRAY ||
                rg.getRhs().getType() != DataType.CHARARRAY)
        {
            int errCode = 1037;
            String msg = "Operands of Regex can be CharArray only :" + rg;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(rg, msg, errCode, PigException.INPUT) ;
        }
    }

    @Override
    public void visit(BinCondExpression binCond) throws FrontendException{
        // high-level type checking
        if (binCond.getCondition().getType() != DataType.BOOLEAN) {
            int errCode = 1047;
            String msg = "Condition in BinCond must be boolean" ;
            msgCollector.collect(msg, MessageType.Error);
            throw new TypeCheckerException(binCond, msg, errCode, PigException.INPUT) ;
        }

        byte lhsType = binCond.getLhs().getType() ;
        byte rhsType = binCond.getRhs().getType() ;

        // If both sides are number, we can convert the smaller type to the bigger type
        if (DataType.isNumberType(lhsType) && DataType.isNumberType(rhsType)) {
            byte biggerType = lhsType > rhsType ? lhsType:rhsType ;
            if (biggerType > lhsType) {
                insertCast(binCond, biggerType, binCond.getLhs());
            }
            else if (biggerType > rhsType) {
                insertCast(binCond, biggerType, binCond.getRhs());
            }
        }
        else if ((lhsType == DataType.BYTEARRAY)
                && ((rhsType == DataType.CHARARRAY) || (DataType
                        .isNumberType(rhsType))) || (rhsType == DataType.DATETIME)) { // need to add boolean as well
            // Cast byte array to the type on rhs
            insertCast(binCond, rhsType, binCond.getLhs());
        } else if ((rhsType == DataType.BYTEARRAY)
                && ((lhsType == DataType.CHARARRAY) || (DataType
                        .isNumberType(lhsType)) || (rhsType == DataType.DATETIME))) { // need to add boolean as well
            // Cast byte array to the type on lhs
            insertCast(binCond, lhsType, binCond.getRhs());
        }

        // A constant null is always bytearray - so cast it
        // to rhs type
        else if (binCond.getLhs() instanceof ConstantExpression
                && ((ConstantExpression) binCond.getLhs()).getValue() == null) {
            try {
                insertCast(binCond, binCond.getRhs().getFieldSchema(), binCond.getLhs());
            } catch (FrontendException e) {
                int errCode = 2216;
                String msg = "Problem getting fieldSchema for " +binCond.getRhs();
                throw new TypeCheckerException(binCond, msg, errCode, PigException.BUG, e);
            }
        } else if (binCond.getRhs() instanceof ConstantExpression
                && ((ConstantExpression) binCond.getRhs()).getValue() == null) {
            try {
                insertCast(binCond,  binCond.getLhs().getFieldSchema(), binCond.getRhs());
            } catch (FrontendException e) {
                int errCode = 2216;
                String msg = "Problem getting fieldSchema for " +binCond.getRhs();
                throw new TypeCheckerException(binCond, msg, errCode, PigException.BUG, e);
            }
        } else if (lhsType == rhsType) {
            // Matching schemas if we're working with tuples/bags
            if (DataType.isSchemaType(lhsType)) {
                try {
                    if(! binCond.getLhs().getFieldSchema().isEqual(binCond.getRhs().getFieldSchema())){
                        int errCode = 1048;
                        String msg = "Two inputs of BinCond must have compatible schemas."
                            + " left hand side: " + binCond.getLhs().getFieldSchema()
                            + " right hand side: " + binCond.getRhs().getFieldSchema();
                        msgCollector.collect(msg, MessageType.Error) ;
                        throw new TypeCheckerException(binCond, msg, errCode, PigException.INPUT) ;
                    }
                    // TODO: We may have to merge the schema here
                    //       if the previous check is not exact match
                }
                catch (FrontendException fe) {
                    int errCode = 1049;
                    String msg = "Problem during evaluaton of BinCond output type" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throw new TypeCheckerException(binCond, msg, errCode, PigException.INPUT, fe) ;
                }
            }
        }
        else {
            int errCode = 1050;
            String msg = "Unsupported input type for BinCond: left hand side: "
                + DataType.findTypeName(lhsType) + "; right hand side: "
                + DataType.findTypeName(rhsType);

            msgCollector.collect(msg, MessageType.Error) ;
            throw new TypeCheckerException(binCond, msg, errCode, PigException.INPUT) ;
        }


    }

    @Override
    public void visit(MapLookupExpression map)
    throws FrontendException{
        if(map.getMap().getType() != DataType.MAP) {
            // insert cast if the predecessor does not
            // return map
            insertCast(map, DataType.MAP, map.getMap());
        }
    }

    @Override
    public void visit(DereferenceExpression deref) throws FrontendException{
        byte inputType = deref.getReferredExpression().getType();
        switch(inputType){
        case DataType.TUPLE:
        case DataType.BAG:
        case DataType.BYTEARRAY: // ideally determine type at runtime
            //allowed types
            break;
        default:
            int errCode = 1129;
            String msg = "Referring to column(s) within a column of type " +
            DataType.findTypeName(inputType)
            + " is not allowed";
            throw new TypeCheckerException(deref, msg, errCode, PigException.INPUT);
        }
    }

    @Override
    public void visit(UserFuncExpression func) throws FrontendException{
        List<LogicalExpression> list = func.getArguments() ;

        // If the dependency graph is right, all the inputs
        // must already know the types
        Schema currentArgSchema = new Schema();
        for(LogicalExpression op: list) {
            if (!DataType.isUsableType(op.getType())) {
                int errCode = 1014;
                String msg = "Problem with input " + op + " of User-defined function: " + func;
                msgCollector.collect(msg, MessageType.Error);
                throw new TypeCheckerException(func, msg, errCode, PigException.INPUT) ;
            }
            try {
                currentArgSchema.add(Util.translateFieldSchema(op.getFieldSchema()));
            } catch (FrontendException e) {
                int errCode = 1043;
                String msg = "Unable to retrieve field schema.";
                throw new TypeCheckerException(func, msg, errCode, PigException.INPUT, e);
            }

        }

        EvalFunc<?> ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(func.getFuncSpec());

        // ask the EvalFunc what types of inputs it can handle
        List<FuncSpec> funcSpecs = null;
        try {
            funcSpecs = ef.getArgToFuncMapping();
            if (funcSpecs!=null) {
                for (FuncSpec funcSpec : funcSpecs) {
                    Schema s = funcSpec.getInputArgsSchema();
                    LogicalSchema ls = Util.translateSchema(s);
                    ls.normalize();
                    funcSpec.setInputArgsSchema(Util.translateSchema(ls));
                }
            }
        } catch (Exception e) {
            int errCode = 1044;
            String msg = "Unable to get list of overloaded methods.";
            throw new TypeCheckerException(func, msg, errCode, PigException.INPUT, e);
        }
        
        // EvalFunc's schema type
        SchemaType udfSchemaType = ef.getSchemaType();

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
            if((matchingSpec = exactMatch(funcSpecs, currentArgSchema, func, udfSchemaType))==null){
                //Oops, no exact match found. Trying to see if we
                //have mappings that we can fit using casts.
                notExactMatch = true;
                if(byteArrayFound(func, currentArgSchema)){
                    // try "exact" matching all other fields except the byte array
                    // fields and if they all exact match and we have only one candidate
                    // for the byte array cast then that's the matching one!
                    if((matchingSpec = exactMatchWithByteArrays(funcSpecs, currentArgSchema, func, udfSchemaType))==null){
                        // "exact" match with byte arrays did not work - try best fit match
                        if((matchingSpec = bestFitMatchWithByteArrays(funcSpecs, currentArgSchema, func, udfSchemaType)) == null) {
                            int errCode = 1045;
                            String msg = "Could not infer the matching function for "
                                + func.getFuncSpec()
                                + " as multiple or none of them fit. Please use an explicit cast.";
                            msgCollector.collect(msg, MessageType.Error);
                            throw new TypeCheckerException(func, msg, errCode, PigException.INPUT);
                        }
                    }
                } else if ((matchingSpec = bestFitMatch(funcSpecs, currentArgSchema, udfSchemaType)) == null) {
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
                    throw new TypeCheckerException(func, msg, errCode, PigException.INPUT);
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
            if (func.isViaDefine()) {
            matchingSpec.setCtorArgs(func.getFuncSpec().getCtorArgs());
            }
            func.setFuncSpec(matchingSpec);
            insertCastsForUDF(func, currentArgSchema, matchingSpec.getInputArgsSchema(), udfSchemaType);

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
     * @param func -
     *             udf expression
     * @param udfSchemaType - 
     *            schema type of the udf
     * @return the funcSpec that supports the schema that is best suited to s.
     *         The best suited schema is one that has the lowest score as
     *         returned by fitPossible().
     * @throws VisitorException
     */
    private FuncSpec bestFitMatchWithByteArrays(List<FuncSpec> funcSpecs,
            Schema s, UserFuncExpression func, SchemaType udfSchemaType) throws VisitorException {
                List<Pair<Long, FuncSpec>> scoreFuncSpecList = new ArrayList<Pair<Long,FuncSpec>>();
        for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator
                .hasNext();) {
            FuncSpec fs = iterator.next();
            long score = fitPossible(s, fs.getInputArgsSchema(), udfSchemaType);
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
                throw new TypeCheckerException(func, msg, errCode, PigException.INPUT);
            }

            // now consider the bytearray fields
            List<Integer> byteArrayPositions = getByteArrayPositions(func, s);
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
                                throw new TypeCheckerException(func, msg, errCode, PigException.INPUT);
                            }
                        }
                    } catch (FrontendException fee) {
                        int errCode = 1043;
                        String msg = "Unalbe to retrieve field schema.";
                        throw new TypeCheckerException(func, msg, errCode, PigException.INPUT, fee);
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


    private static class ScoreFuncSpecListComparator implements Comparator<Pair<Long, FuncSpec>> {

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
     * Finds if there is an exact match between the schema supported by
     * one of the funcSpecs and the input schema s. Here first exact match
     * for all non byte array fields is first attempted and if there is
     * exactly one candidate, it is chosen (since the bytearray(s) can
     * just be cast to corresponding type(s) in the candidate)
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param func - UserFuncExpression for which matching is requested
     * @param udfSchemaType - schema type of the udf
     * @return the matching spec if found else null
     * @throws FrontendException
     */
    private FuncSpec exactMatchWithByteArrays(List<FuncSpec> funcSpecs,
            Schema s, UserFuncExpression func, SchemaType udfSchemaType) throws FrontendException {
        // exact match all fields except byte array fields
        // ignore byte array fields for matching
        return exactMatchHelper(funcSpecs, s, func, udfSchemaType, true);
    }

    /**
     * Finds if there is an exact match between the schema supported by
     * one of the funcSpecs and the input schema s. Here an exact match
     * for all fields is attempted.
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param func - UserFuncExpression for which matching is requested
     * @param udfSchemaType - schema type of the user defined function
     * @return the matching spec if found else null
     * @throws FrontendException
     */
    private FuncSpec exactMatch(List<FuncSpec> funcSpecs, Schema s,
            UserFuncExpression func, SchemaType udfSchemaType) throws FrontendException {
        // exact match all fields, don't ignore byte array fields
        return exactMatchHelper(funcSpecs, s, func, udfSchemaType, false);
    }

    /**
     * Tries to find the schema supported by one of funcSpecs which can
     * be obtained by inserting a set of casts to the input schema
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param udfSchemaType - schema type of the udf
     * @return the funcSpec that supports the schema that is best suited
     *          to s. The best suited schema is one that has the
     *          lowest score as returned by fitPossible().
     */
    private FuncSpec bestFitMatch(List<FuncSpec> funcSpecs, Schema s, SchemaType udfSchemaType) {
        FuncSpec matchingSpec = null;
        long score = INF;
        long prevBestScore = Long.MAX_VALUE;
        long bestScore = Long.MAX_VALUE;
        for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator.hasNext();) {
            FuncSpec fs = iterator.next();
            score = fitPossible(s,fs.getInputArgsSchema(), udfSchemaType);
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

    /**
     * Checks to see if any field of the input schema is a byte array
     * @param func
     * @param s - input schema
     * @return true if found else false
     * @throws VisitorException
     */
    private boolean byteArrayFound(UserFuncExpression func, Schema s) throws VisitorException {
        for(int i=0;i<s.size();i++){
            try {
                FieldSchema fs=s.getField(i);
                if(fs == null)
                    return false;
                if(fs.type==DataType.BYTEARRAY){
                    return true;
                }
            } catch (FrontendException fee) {
                int errCode = 1043;
                String msg = "Unable to retrieve field schema.";
                throw new TypeCheckerException(func, msg, errCode, PigException.INPUT, fee);
            }
        }
        return false;
    }

    /**
     * Gets the positions in the schema which are byte arrays
     * @param func
     *
     * @param s -
     *            input schema
     * @throws VisitorException
     */
    private List<Integer> getByteArrayPositions(UserFuncExpression func, Schema s)
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
                throw new TypeCheckerException(func, msg, errCode, PigException.INPUT, fee);            }
        }
        return result;
    }

    /**
     * Finds if there is an exact match between the schema supported by
     * one of the funcSpecs and the input schema s
     * @param funcSpecs - mappings provided by udf
     * @param s - input schema
     * @param func user defined function
     * @param udfSchemaType - schema type of the user defined function
     * @param ignoreByteArrays - flag for whether the exact match is to computed
     * after ignoring bytearray (if true) or without ignoring bytearray (if false)
     * @return the matching spec if found else null
     * @throws FrontendException
     */
    private FuncSpec exactMatchHelper(List<FuncSpec> funcSpecs, Schema s,
            UserFuncExpression func, SchemaType udfSchemaType, boolean ignoreByteArrays)
    throws FrontendException {
        List<FuncSpec> matchingSpecs = new ArrayList<FuncSpec>();
        for (Iterator<FuncSpec> iterator = funcSpecs.iterator(); iterator.hasNext();) {
            FuncSpec fs = iterator.next();
            if (schemaEqualsForMatching(s, fs.getInputArgsSchema(), udfSchemaType, ignoreByteArrays)) {
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
            throw new TypeCheckerException(func, msg, errCode, PigException.INPUT);
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
     * @throws FrontendException
     */
    public static boolean schemaEqualsForMatching(Schema inputSchema,
            Schema udfSchema, SchemaType udfSchemaType, boolean ignoreByteArrays) throws FrontendException {


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

        // the old udf schemas might not have tuple inside bag
        // fix that!
        udfSchema = Util.fixSchemaAddTupleInBag(udfSchema);

        if ((udfSchemaType == SchemaType.NORMAL) && (inputSchema.size() != udfSchema.size()))
            return false;
        if ((udfSchemaType == SchemaType.VARARG) && inputSchema.size() < udfSchema.size())
            return false;

        Iterator<FieldSchema> i = inputSchema.getFields().iterator();
        Iterator<FieldSchema> j = udfSchema.getFields().iterator();

        FieldSchema udfFieldSchema = null;
        while (i.hasNext()) {

            FieldSchema inputFieldSchema = i.next();
            if(inputFieldSchema == null)
                return false;

            //if there's no more UDF field: take the last one which is the vararg field
            udfFieldSchema = j.hasNext() ? j.next() : udfFieldSchema;
            
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
            // if it is a bag with empty tuple, then just rely on the field type
            if (DataType.isSchemaType(udfFieldSchema.type)
                    && udfFieldSchema.schema != null
                    && isNotBagWithEmptyTuple(udfFieldSchema)
            ) {
                // Compare recursively using field schema
                if (!FieldSchema.equals(inputFieldSchema, udfFieldSchema,
                        false, true)) {
                    //try modifying any empty tuple to type of bytearray
                    // and see if that matches. Need to do this for
                    // backward compatibility -
                    // User might have specified tuple with a bytearray
                    // and this should also match an empty tuple

                    FieldSchema inputFSWithBytearrayinTuple =
                        new FieldSchema(inputFieldSchema);

                    convertEmptyTupleToBytearrayTuple(inputFSWithBytearrayinTuple);

                    if (!FieldSchema.equals(inputFSWithBytearrayinTuple, udfFieldSchema,
                            false, true)) {
                        return false;
                    }
                }
            }

        }
        return true;
    }

    /**
     * Check if the fieldSch is a bag with empty tuple schema
     * @param fieldSch
     * @return
     * @throws FrontendException
     */
    private static boolean isNotBagWithEmptyTuple(FieldSchema fieldSch)
    throws FrontendException {
        boolean isBagWithEmptyTuple = false;
        if(fieldSch.type == DataType.BAG
                && fieldSch.schema != null
                && fieldSch.schema.getField(0) != null
                && fieldSch.schema.getField(0).type == DataType.TUPLE
                && fieldSch.schema.getField(0).schema == null
        ){
            isBagWithEmptyTuple = true;
        }
        return !isBagWithEmptyTuple;
    }

    private static void convertEmptyTupleToBytearrayTuple(
            FieldSchema fs) {
        if(fs.type == DataType.TUPLE
                && fs.schema != null
                && fs.schema.size() == 0){
            fs.schema.add(new FieldSchema(null, DataType.BYTEARRAY));
            return;
        }

        if(fs.schema != null){
            for(FieldSchema inFs : fs.schema.getFields()){
                convertEmptyTupleToBytearrayTuple(inFs);
            }
        }

    }

    static final HashMap<Byte, List<Byte>> castLookup = new HashMap<Byte, List<Byte>>();
    static{
        //Ordering here decides the score for the best fit function.
        //Do not change the order. Conversions to a smaller type is preferred
        //over conversion to a bigger type where ordering of types is:
        //INTEGER, LONG, FLOAT, DOUBLE, DATETIME, CHARARRAY, TUPLE, BAG, MAP
        //from small to big

        List<Byte> boolToTypes = Arrays.asList(
                DataType.INTEGER,
                DataType.LONG,
                DataType.FLOAT,
                DataType.DOUBLE,
                DataType.BIGINTEGER,
                DataType.BIGDECIMAL
                // maybe more bigger types
        );
        castLookup.put(DataType.BOOLEAN, boolToTypes);

        List<Byte> intToTypes = Arrays.asList(
                DataType.LONG,
                DataType.FLOAT,
                DataType.DOUBLE,
                DataType.BIGINTEGER,
                DataType.BIGDECIMAL
        );
        castLookup.put(DataType.INTEGER, intToTypes);

        List<Byte> longToTypes = Arrays.asList(
                DataType.FLOAT,
                DataType.DOUBLE,
                DataType.BIGINTEGER,
                DataType.BIGDECIMAL
        );
        castLookup.put(DataType.LONG, longToTypes);

        List<Byte> floatToTypes = Arrays.asList(
                DataType.DOUBLE,
                DataType.BIGINTEGER,
                DataType.BIGDECIMAL
        );
        castLookup.put(DataType.FLOAT, floatToTypes);

        List<Byte> doubleToTypes = Arrays.asList(
                DataType.BIGINTEGER,
                DataType.BIGDECIMAL
        );
        castLookup.put(DataType.DOUBLE, doubleToTypes);

        List<Byte> bigIntegerToTypes = Arrays.asList(
                DataType.BIGDECIMAL
        );
        castLookup.put(DataType.BIGINTEGER, bigIntegerToTypes);

        List<Byte> byteArrayToTypes = Arrays.asList(
                DataType.BOOLEAN,
                DataType.INTEGER,
                DataType.LONG,
                DataType.FLOAT,
                DataType.DOUBLE,
                DataType.DATETIME,
                DataType.CHARARRAY,
                DataType.BIGINTEGER,
                DataType.BIGDECIMAL,
                DataType.TUPLE,
                DataType.BAG,
                DataType.MAP
        );
        castLookup.put(DataType.BYTEARRAY, byteArrayToTypes);

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
     * @param s2Type
     * @return
     */
    private long fitPossible(Schema s1, Schema s2, SchemaType s2Type) {
        if(s1==null || s2==null) return INF;
        List<FieldSchema> sFields = s1.getFields();
        List<FieldSchema> fsFields = s2.getFields();
        
        if((s2Type == SchemaType.NORMAL) && (sFields.size()!=fsFields.size()))
            return INF;
        if((s2Type == SchemaType.VARARG) && (sFields.size() < fsFields.size()))
            return INF;
        long score = 0;
        int castCnt=0;
        for(int i=0;i<sFields.size();i++){
            FieldSchema sFS = sFields.get(i);
            if(sFS == null){
                return INF;
            }

            // if we have a byte array do not include it
            // in the computation of the score - bytearray
            // fields will be looked at separately outside
            // of this function
            if (sFS.type == DataType.BYTEARRAY)
                continue;
            
            //if we get to the vararg field (if defined) : take it repeatedly
            FieldSchema fsFS = ((s2Type == SchemaType.VARARG) && i >= s2.size()) ? 
                    fsFields.get(s2.size() - 1) : fsFields.get(i);

            if(DataType.isSchemaType(sFS.type)){
                if(!FieldSchema.equals(sFS, fsFS, false, true))
                    return INF;
            }
            if(FieldSchema.equals(sFS, fsFS, true, true)) continue;
            if(!castLookup.containsKey(sFS.type))
                return INF;
            if(!(castLookup.get(sFS.type).contains(fsFS.type)))
                return INF;
            score += (castLookup.get(sFS.type)).indexOf(fsFS.type) + 1;
            ++castCnt;
        }
        return score * castCnt;
    }

    private void insertCastsForUDF(UserFuncExpression func, Schema fromSch, Schema toSch, SchemaType toSchType)
    throws FrontendException {
        List<FieldSchema> fsLst = fromSch.getFields();
        List<FieldSchema> tsLst = toSch.getFields();
        List<LogicalExpression> args = func.getArguments();
        int i=-1;
        for (FieldSchema fFSch : fsLst) {
            ++i;
            //if we get to the vararg field (if defined) : take it repeatedly
            FieldSchema tFSch = ((toSchType == SchemaType.VARARG) && i >= tsLst.size()) ? 
                    tsLst.get(tsLst.size() - 1) : tsLst.get(i);
            if (fFSch.type == tFSch.type) {
                continue;
            }
            insertCast(func, Util.translateFieldSchema(tFSch), args.get(i));
        }
    }

    /***
     * Helper for collecting warning when casting is inserted
     * to the plan (implicit casting)
     *
     * @param node
     * @param originalType
     * @param toType
     */
    static void collectCastWarning(Operator node,
            byte originalType,
            byte toType,
            CompilationMessageCollector msgCollector
    ) {
        String originalTypeName = DataType.findTypeName(originalType) ;
        String toTypeName = DataType.findTypeName(toType) ;
        String opName= node.getClass().getSimpleName() ;
        PigWarning kind = null;
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
        case DataType.BOOLEAN:
            kind = PigWarning.IMPLICIT_CAST_TO_BOOLEAN;
            break;
        case DataType.DATETIME:
            kind = PigWarning.IMPLICIT_CAST_TO_DATETIME;
            break;
        case DataType.MAP:
            kind = PigWarning.IMPLICIT_CAST_TO_MAP;
            break;
        case DataType.TUPLE:
            kind = PigWarning.IMPLICIT_CAST_TO_TUPLE;
            break;
        case DataType.BIGINTEGER:
            kind = PigWarning.IMPLICIT_CAST_TO_BIGINTEGER;
            break;
        case DataType.BIGDECIMAL:
            kind = PigWarning.IMPLICIT_CAST_TO_BIGDECIMAL;
            break;
        }
        msgCollector.collect(originalTypeName + " is implicitly cast to "
                + toTypeName +" under " + opName + " Operator",
                MessageType.Warning, kind) ;
    }



    static class FieldSchemaResetter extends AllSameExpressionVisitor {

        protected FieldSchemaResetter(OperatorPlan p) throws FrontendException {
            super(p, new ReverseDependencyOrderWalker(p));
        }

        @Override
        protected void execute(LogicalExpression op) throws FrontendException {
            op.resetFieldSchema();
        }

    }

}
