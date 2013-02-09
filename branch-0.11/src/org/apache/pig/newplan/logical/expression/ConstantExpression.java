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

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.parser.SourceLocation;

/**
 * A constant
 *
 */
public class ConstantExpression extends ColumnExpression {
    
    // Stupid Java needs a union
    Object val;
    
    /**
     * Adds expression to the plan 
     * @param plan LogicalExpressionPlan this constant is a part of.
     * @param val Value of this constant.
     */
    public ConstantExpression(OperatorPlan plan, Object val){
        super("Constant", plan);
        this.val = val;
        plan.add(this);
    }
    
    /**
     * @link org.apache.pig.newplan.Operator#accept(org.apache.pig.newplan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new FrontendException("Expected LogicalExpressionVisitor", 2222);
        }
        ((LogicalExpressionVisitor)v).visit(this);

    }

    /**
     * Get the value of this constant.
     * @return value of the constant
     */
    public Object getValue() {
        return val;
    }
    
    public void setValue(Object val) {
    	this.val = val;
    }
    

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof ConstantExpression) {
            ConstantExpression co = (ConstantExpression)other;
            return co.getFieldSchema().isEqual(getFieldSchema()) && ( ( co.val == null && val == null ) 
                    || ( co != null && co.val.equals(val) ) );
        } else {
            return false;
        }
    }
    
    @Override
    public LogicalFieldSchema getFieldSchema() throws FrontendException {
        if (fieldSchema!=null)
            return fieldSchema;
        try {
            fieldSchema =  Util.translateFieldSchema(DataType.determineFieldSchema(val));
        }catch (Exception e) {
            throw new FrontendException(
                    "Error determining field schema from object in constant expression",
                    1125,
                    PigException.INPUT,
                    e
            );
        }
        uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
        return fieldSchema;
    }
 
    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException{
        LogicalExpression copy = new ConstantExpression(lgExpPlan, this.getValue());
        copy.setLocation( new SourceLocation( location ) );
        return copy;
    }
 
}
