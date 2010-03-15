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

package org.apache.pig.experimental.logical.expression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOUserFunc;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlanCloner;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.VisitorException;

public class UserFuncExpression extends LogicalExpression {

    private FuncSpec mFuncSpec;
    
    public UserFuncExpression(OperatorPlan plan, FuncSpec funcSpec, byte b) {
        super("UserFunc", plan, b);
        mFuncSpec = funcSpec;
        plan.add(this);
    }

    public FuncSpec getFuncSpec() {
        return mFuncSpec;
    }
    
    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new IOException("Expected LogicalExpressionVisitor");
        }
        ((LogicalExpressionVisitor)v).visitUserFunc(this);
    }

    @Override
    public boolean isEqual(Operator other) {
        if( other instanceof UserFuncExpression ) {
            UserFuncExpression exp = (UserFuncExpression)other;
            return plan.isEqual(exp.plan) && mFuncSpec.equals(exp.mFuncSpec );
        } else {
            return false;
        }
    }

    public List<LogicalExpression> getArguments() {
        List<Operator> successors = null;
        List<LogicalExpression> args = new ArrayList<LogicalExpression>();
        try {
            successors = plan.getSuccessors(this);

            if(successors == null)
                return args;

            for(Operator lo : successors){
                args.add((LogicalExpression)lo);
            }
        } catch (IOException e) {
           return args;
        }
        return args;
    }

    /**
     * @param funcSpec the FuncSpec to set
     */
    public void setFuncSpec(FuncSpec funcSpec) {
        mFuncSpec = funcSpec;
    }
    
//    @Override
//    public Schema.FieldSchema getFieldSchema() throws FrontendException {
//        if(!mIsFieldSchemaComputed) {
//            Schema inputSchema = new Schema();
//            List<ExpressionOperator> args = getArguments();
//            for(ExpressionOperator op: args) {
//                if (!DataType.isUsableType(op.getType())) {
//                    mFieldSchema = null;
//                    mIsFieldSchemaComputed = false;
//                    int errCode = 1014;
//                    String msg = "Problem with input: " + op + " of User-defined function: " + this ;
//                    throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
//                }
//                inputSchema.add(op.getFieldSchema());    
//            }
//    
//            EvalFunc<?> ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(mFuncSpec);
//            Schema udfSchema = ef.outputSchema(inputSchema);
//            byte returnType = DataType.findType(ef.getReturnType());
//
//            if (null != udfSchema) {
//                Schema.FieldSchema fs;
////                try {
//                    if(udfSchema.size() == 0) {
//                        fs = new Schema.FieldSchema(null, null, returnType);
//                    } else if(udfSchema.size() == 1) {
//                        fs = new Schema.FieldSchema(udfSchema.getField(0));
//                    } else {
//                        fs = new Schema.FieldSchema(null, udfSchema, DataType.TUPLE);
//                    }
////                } catch (ParseException pe) {
////                    throw new FrontendException(pe.getMessage());
////                }
//                setType(fs.type);
//                mFieldSchema = fs;
//                mIsFieldSchemaComputed = true;
//            } else {
//                setType(returnType);
//                mFieldSchema = new Schema.FieldSchema(null, null, returnType);
//                mIsFieldSchemaComputed = true;
//            }
//        }
//        return mFieldSchema;
//    }
}
