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

package org.apache.pig.newplan.logical.expression;

import java.util.ArrayList;
import java.util.List;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class UserFuncExpression extends LogicalExpression {

    private FuncSpec mFuncSpec;
    private Operator implicitReferencedOperator = null; 
    
    public UserFuncExpression(OperatorPlan plan, FuncSpec funcSpec) {
        super("UserFunc", plan);
        mFuncSpec = funcSpec;
        plan.add(this);
    }

    public FuncSpec getFuncSpec() {
        return mFuncSpec;
    }
    
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new FrontendException("Expected LogicalExpressionVisitor", 2222);
        }
        ((LogicalExpressionVisitor)v).visit(this);
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if( other instanceof UserFuncExpression ) {
            UserFuncExpression exp = (UserFuncExpression)other;
            return plan.isEqual(exp.plan) && mFuncSpec.equals(exp.mFuncSpec );
        } else {
            return false;
        }
    }

    public List<LogicalExpression> getArguments() throws FrontendException {
        List<Operator> successors = null;
        List<LogicalExpression> args = new ArrayList<LogicalExpression>();
//        try {
            successors = plan.getSuccessors(this);

            if(successors == null)
                return args;

            for(Operator lo : successors){
                args.add((LogicalExpression)lo);
            }
//        } catch (FrontendException e) {
//           return args;
//        }
        return args;
    }

    /**
     * @param funcSpec the FuncSpec to set
     */
    public void setFuncSpec(FuncSpec funcSpec) {
        mFuncSpec = funcSpec;
    }
    
    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema() throws FrontendException {
        if (fieldSchema!=null)
            return fieldSchema;
        
        if(implicitReferencedOperator != null &&
                mFuncSpec.getClassName().equals("org.apache.pig.impl.builtin.ReadScalars")){
            // if this is a ReadScalars udf for scalar operation, use the 
            // FieldSchema corresponding to this position in input 
            List<Operator> args = plan.getSuccessors(this);
            if(args != null && args.size() > 0 ){
                int pos = (Integer)((ConstantExpression)args.get(0)).getValue();
                LogicalRelationalOperator inp = (LogicalRelationalOperator)implicitReferencedOperator;

                if( inp.getSchema() != null){
                    LogicalFieldSchema inpFs = inp.getSchema().getField(pos);
                    fieldSchema = new LogicalFieldSchema(inpFs);
                    //  fieldSchema.alias = "ReadScalars_" + fieldSchema.alias;
                }else{
                    fieldSchema = new LogicalFieldSchema(null, null, DataType.BYTEARRAY);
                }
                return fieldSchema;
            }else{
                //predecessors haven't been setup, return null
                return null;
            }
        }

        LogicalSchema inputSchema = new LogicalSchema();
        List<Operator> succs = plan.getSuccessors(this);

        if (succs!=null) {
            for(Operator lo : succs){
                if (((LogicalExpression)lo).getFieldSchema()==null) {
                    inputSchema = null;
                    break;
                }
                inputSchema.addField(((LogicalExpression)lo).getFieldSchema());
            }
        }

        EvalFunc<?> ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(mFuncSpec);
        Schema udfSchema = ef.outputSchema(Util.translateSchema(inputSchema));

        if (udfSchema != null) {
            Schema.FieldSchema fs;
            if(udfSchema.size() == 0) {
                fs = new Schema.FieldSchema(null, null, DataType.findType(ef.getReturnType()));
            } else if(udfSchema.size() == 1) {
                fs = new Schema.FieldSchema(udfSchema.getField(0));
            } else {
                fs = new Schema.FieldSchema(null, udfSchema, DataType.TUPLE);
            }
            fieldSchema = Util.translateFieldSchema(fs);
        } else {
            fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null, DataType.findType(ef.getReturnType()));
        }
        uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
        return fieldSchema;
    }
    
    public Operator getImplicitReferencedOperator() {
        return implicitReferencedOperator;
    }
    
    public void setImplicitReferencedOperator(Operator implicitReferencedOperator) {
        this.implicitReferencedOperator = implicitReferencedOperator;
    }

    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        UserFuncExpression copy =  null; 
        try {
            copy = new UserFuncExpression(
                    lgExpPlan,
                    this.getFuncSpec().clone() );
            copy.setImplicitReferencedOperator(this.getImplicitReferencedOperator());
        } catch(CloneNotSupportedException e) {
             e.printStackTrace();
        }
        return copy;
    }
}
