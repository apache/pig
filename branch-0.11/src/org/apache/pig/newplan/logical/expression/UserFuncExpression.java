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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.Nondeterministic;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.data.SchemaTupleFrontend;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.parser.SourceLocation;

public class UserFuncExpression extends LogicalExpression {

    private FuncSpec mFuncSpec;
    private EvalFunc<?> ef = null;
    private String signature;
    private static int sigSeq=0;
    private boolean viaDefine=false; //this represents whether the function was instantiate via a DEFINE statement or not
    
    public UserFuncExpression(OperatorPlan plan, FuncSpec funcSpec) {
        super("UserFunc", plan);
        mFuncSpec = funcSpec;
        plan.add(this);
        if (signature==null) {
            signature = Integer.toString(sigSeq++);
        }
    }
    
    
    public UserFuncExpression(OperatorPlan plan, FuncSpec funcSpec, List<LogicalExpression> args) {
        this( plan, funcSpec );
        
        for( LogicalExpression arg : args ) {
            plan.connect( this, arg );
        }
    }

    public UserFuncExpression(OperatorPlan plan, FuncSpec funcSpec, boolean viaDefine) {
        this( plan, funcSpec);
        this.viaDefine = viaDefine;
    }

    public UserFuncExpression(OperatorPlan plan, FuncSpec funcSpec, List<LogicalExpression> args, boolean viaDefine) {
        this( plan, funcSpec, args );
        this.viaDefine = viaDefine;
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
        
        //For the purpose of optimization rules (specially LogicalExpressionSimplifier)
        // a non deterministic udf is not equal to another. So returning false for
        //such cases.
        // Note that the function is also invoked by implementations of OperatorPlan.isEqual
        // that function is called from test cases to compare logical plans, and
        // it will return false even if the plans are clones. 
        if(!this.isDeterministic())
            return false;
        
        if( other instanceof UserFuncExpression ) {
            UserFuncExpression exp = (UserFuncExpression)other;
            if (!mFuncSpec.equals(exp.mFuncSpec ))
                return false;
            
            List<Operator> mySuccs = getPlan().getSuccessors(this);
            List<Operator> theirSuccs = other.getPlan().getSuccessors(other);
            if(mySuccs == null || theirSuccs == null){
                if(mySuccs == null && theirSuccs == null){
                    return true;
                }else{
                    //only one of the udfs has null successors
                    return false;
                }
            }
            if (mySuccs.size()!=theirSuccs.size())
                return false;
            for (int i=0;i<mySuccs.size();i++) {
                if (!mySuccs.get(i).isEqual(theirSuccs.get(i)))
                    return false;
            }
            return true;
        } else {
            return false;
        }
    }
    
    public boolean isDeterministic() throws FrontendException{
        Class<?> udfClass;
        try {
            udfClass = PigContext.resolveClassName(getFuncSpec().getClassName());
        }catch(IOException ioe) {
            throw new FrontendException("Cannot instantiate: " + getFuncSpec(), ioe) ;
        }

        if (udfClass.getAnnotation(Nondeterministic.class) == null) {
            return true;
        }
        return false;
        
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
        ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(mFuncSpec);
    }
    
    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema() throws FrontendException {
        if (fieldSchema!=null)
            return fieldSchema;
        
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

        // Since ef only set one time, we never change its value, so we can optimize it by instantiate only once.
        // This significantly optimize the performance of frontend (PIG-1738)
        if (ef==null)
            ef = (EvalFunc<?>) PigContext.instantiateFuncFromSpec(mFuncSpec);
        
        ef.setUDFContextSignature(signature);
        Properties props = UDFContext.getUDFContext().getUDFProperties(ef.getClass());
        Schema translatedInputSchema = Util.translateSchema(inputSchema);
        if(translatedInputSchema != null) {
    		props.put("pig.evalfunc.inputschema."+signature, translatedInputSchema);
        }
        // Store inputSchema into the UDF context
        ef.setInputSchema(translatedInputSchema);
;
        Schema udfSchema = ef.outputSchema(translatedInputSchema);
        
        //TODO appendability should come from a setting
        SchemaTupleFrontend.registerToGenerateIfPossible(translatedInputSchema, false, GenContext.UDF);
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, false, GenContext.UDF);

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
            fieldSchema.normalize();
        } else {
            fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null, DataType.findType(ef.getReturnType()));
        }
        uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
        return fieldSchema;
    }
    
    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        UserFuncExpression copy =  null; 
        try {
            copy = new UserFuncExpression(
                    lgExpPlan,
                    this.getFuncSpec().clone(),
                    viaDefine);
            
            copy.signature = signature;
            // Deep copy the input expressions.
            List<Operator> inputs = plan.getSuccessors( this );
            if( inputs != null ) {
                for( Operator op : inputs ) {
                    LogicalExpression input = (LogicalExpression)op;
                    LogicalExpression inputCopy = input.deepCopy( lgExpPlan );
                    lgExpPlan.add( inputCopy );
                    lgExpPlan.connect( copy, inputCopy );
                }
            }
            
        } catch(CloneNotSupportedException e) {
             e.printStackTrace();
        }
        copy.setLocation( new SourceLocation( location ) );
        return copy;
    }
    
    public String toString() {
        StringBuilder msg = new StringBuilder();
        msg.append("(Name: " + name + "(" + getFuncSpec() + ")" + " Type: ");
        if (fieldSchema!=null)
            msg.append(DataType.findTypeName(fieldSchema.type));
        else
            msg.append("null");
        msg.append(" Uid: ");
        if (fieldSchema!=null)
            msg.append(fieldSchema.uid);
        else
            msg.append("null");
        msg.append(")");

        return msg.toString();
    }
    
    public String getSignature() {
        return signature;
    }

    public boolean isViaDefine() {
        return viaDefine;
    }
}
