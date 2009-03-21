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
package org.apache.pig.impl.logicalLayer;

import java.util.List;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;

public class LOUserFunc extends ExpressionOperator {
    private static final long serialVersionUID = 2L;

    private FuncSpec mFuncSpec;
    
    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param k
     *            OperatorKey for this operator.
     * @param funcSpec
     *            name of the user defined function.
     * @param returnType
     *            return type of this function.
     */
    public LOUserFunc(LogicalPlan plan, OperatorKey k, FuncSpec funcSpec,
            byte returnType) {
        super(plan, k, -1);
        mFuncSpec = funcSpec;
        mType = returnType;
    }

    public FuncSpec getFuncSpec() {
        return mFuncSpec;
    }

    public List<ExpressionOperator> getArguments() {
        List<LogicalOperator> preds = getPlan().getPredecessors(this);
        List<ExpressionOperator> args = new ArrayList<ExpressionOperator>();
        if(preds == null)
                return args;
            
        for(LogicalOperator lo : preds){
            args.add((ExpressionOperator)lo);
        }
        return args;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public String name() {
        return "UserFunc " + mKey.scope + "-" + mKey.id + " function: " + mFuncSpec;
    }

    @Override
    public Schema getSchema() {
        return mSchema;
    }

    @Override
    public Schema.FieldSchema getFieldSchema() throws FrontendException {
        if(!mIsFieldSchemaComputed) {
            Schema inputSchema = new Schema();
            List<ExpressionOperator> args = getArguments();
            for(ExpressionOperator op: args) {
                if (!DataType.isUsableType(op.getType())) {
                    mFieldSchema = null;
                    mIsFieldSchemaComputed = false;
                    int errCode = 1014;
                    String msg = "Problem with input: " + op + " of User-defined function: " + this ;
                    throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
                }
                inputSchema.add(op.getFieldSchema());    
            }
    
            EvalFunc<?> ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(mFuncSpec);
            Schema udfSchema = ef.outputSchema(inputSchema);
            byte returnType = DataType.findType(ef.getReturnType());

            if (null != udfSchema) {
                Schema.FieldSchema fs;
//                try {
                    if(udfSchema.size() == 0) {
                        fs = new Schema.FieldSchema(null, null, returnType);
                    } else if(udfSchema.size() == 1) {
                        fs = new Schema.FieldSchema(udfSchema.getField(0));
                    } else {
                        fs = new Schema.FieldSchema(null, udfSchema, DataType.TUPLE);
                    }
//                } catch (ParseException pe) {
//                    throw new FrontendException(pe.getMessage());
//                }
                setType(fs.type);
                mFieldSchema = fs;
                mIsFieldSchemaComputed = true;
            } else {
                setType(returnType);
                mFieldSchema = new Schema.FieldSchema(null, null, returnType);
                mIsFieldSchemaComputed = true;
            }
        }
        return mFieldSchema;
    }


    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    /**
     * @param funcSpec the FuncSpec to set
     */
    public void setFuncSpec(FuncSpec funcSpec) {
        mFuncSpec = funcSpec;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.ExpressionOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOUserFunc clone = (LOUserFunc)super.clone();
        //note that mFuncSpec cannot be null in LOUserFunc
        clone.mFuncSpec = mFuncSpec.clone();
        return clone;
    }

}
