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

import java.util.Map;

import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;


/**
 * Find uid lineage information. Set the load function in CastExpression 
 * if it needs to convert bytearray to another type.
 */
public class CastLineageSetter extends AllExpressionVisitor{

    private CompilationMessageCollector msgCollector;
    private Map<Long, FuncSpec> uid2LoadFuncMap;
    public CastLineageSetter(
            LogicalPlan plan, 
            CompilationMessageCollector msgCollector
    ) 
    throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
        this.msgCollector = msgCollector;
        
        //find lineage of columns, get mapping of uid to load-functions
        LineageFindRelVisitor  lineageFinder = new LineageFindRelVisitor(plan);
        lineageFinder.visit();
        uid2LoadFuncMap = lineageFinder.getUid2LoadFuncMap();
        //plan.explain(System.out, "test", true);
        //System.out.println(uid2LoadFuncMap);
    }


    @Override
    protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan exprPlan)
    throws FrontendException {
        return new CastLineageSetterExpVisitor(exprPlan, uid2LoadFuncMap, msgCollector);
    }


    static class CastLineageSetterExpVisitor extends LogicalExpressionVisitor{
        private Map<Long, FuncSpec> uid2LoadFuncMap;
        private CompilationMessageCollector msgCollector;
        
        protected CastLineageSetterExpVisitor(
                OperatorPlan expPlan, 
                Map<Long, FuncSpec> uid2LoadFuncMap,
                CompilationMessageCollector msgCollector
        )
        throws FrontendException {
            super(expPlan, new ReverseDependencyOrderWalker(expPlan));
            this.uid2LoadFuncMap = uid2LoadFuncMap;
            this.msgCollector = msgCollector;
        }
        
        /* (non-Javadoc)
         * @see org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor#visit(org.apache.pig.newplan.logical.expression.CastExpression)
         *if input type is bytearray, find and set the corresponding load function
         * that this field comes from.
         * The load functions LoadCaster interface will help with conversion 
         * at runtime.
         * If there is no corresponding load function (eg if the input is an
         *  output of a udf), set nothing - it assumes that bytearray is being used
         *  as equivalent of 'unknown' type. It will try to identify the type
         *  at runtime and cast it. 
         */
        @Override
        public void visit(CastExpression cast) throws FrontendException{
            
            byte inType = cast.getExpression().getType();
            byte outType = cast.getType();

            if(containsByteArrayOrEmtpyInSchema(cast.getExpression().getFieldSchema())){
                long inUid = cast.getExpression().getFieldSchema().uid;
                FuncSpec inLoadFunc = uid2LoadFuncMap.get(inUid);
                if(inLoadFunc == null){
                    String msg = "Cannot resolve load function to use for casting from " + 
                                DataType.findTypeName(inType) + " to " +
                                DataType.findTypeName(outType) + ". ";
                    msgCollector.collect(msg, MessageType.Warning,
                           PigWarning.NO_LOAD_FUNCTION_FOR_CASTING_BYTEARRAY);
                }else {
                    cast.setFuncSpec(inLoadFunc);
                }
            }
        }

        /**
         * @param fs
         * @return true if fs is of complex type and contains a bytearray 
         *  or empty inner schema
         * @throws FrontendException
         */
        private boolean containsByteArrayOrEmtpyInSchema(LogicalFieldSchema fs)
        throws FrontendException {
            
            if(fs.type == DataType.BYTEARRAY)
                return true;
            
            if(DataType.isAtomic(fs.type))
                return false;
            
            if(fs.schema == null || fs.schema.size() == 0)
                return true;
            
            for(LogicalFieldSchema inFs : fs.schema.getFields()){
                if(containsByteArrayOrEmtpyInSchema(inFs))
                    return true;
            }

            return false;
        }



    }

}
