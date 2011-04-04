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

import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.builtin.ReadScalars;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.parser.SourceLocation;

public class ScalarExpression extends UserFuncExpression {
    private Operator implicitReferencedOperator = null;
    private Operator attachedLogicalOperator = null; // The logical Operator where this udf is used.
 
    private static FuncSpec funcSpec = new FuncSpec( ReadScalars.class.getName() );
    
    public ScalarExpression(OperatorPlan plan) {
        super( plan, funcSpec );
    }
    
    public ScalarExpression(OperatorPlan plan, Operator implicitReferencedOperator,
            Operator attachedLogicalOperator) {
        this( plan );
        this.implicitReferencedOperator = implicitReferencedOperator;
        this.attachedLogicalOperator = attachedLogicalOperator;
    }
    
    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema() throws FrontendException {
        if (fieldSchema!=null)
            return fieldSchema;

        // TODO: apparently there is a problem with some test cases where implicitReferencedOperator can be null,
        // even for a scalar UDF. Need more investigation. For now, simulate the old code to make these
        // test case pass.
        if( implicitReferencedOperator != null ) {
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
        } else {
        	return super.getFieldSchema();
        }
    }
    
    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        ScalarExpression copy =  null; 
        copy = new ScalarExpression( lgExpPlan, implicitReferencedOperator,
            attachedLogicalOperator );
        
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
        
        copy.setLocation( new SourceLocation( location ) );
        return copy;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if( !( v instanceof LogicalExpressionVisitor ) ) {
            throw new FrontendException( "Expected LogicalExpressionVisitor", 2222 );
        }
        ((LogicalExpressionVisitor)v).visit( this );
    }

    public Operator getImplicitReferencedOperator() {
        return implicitReferencedOperator;
    }
    
    public void setImplicitReferencedOperator(Operator implicitReferencedOperator) {
		this.implicitReferencedOperator = implicitReferencedOperator;
	}

	public void setAttachedLogicalOperator(Operator attachedLogicalOperator) {
		this.attachedLogicalOperator = attachedLogicalOperator;
	}

	public Operator getAttachedLogicalOperator() {
        return attachedLogicalOperator;
    }

    @Override
    public String toString() {
        StringBuilder msg = new StringBuilder();
        msg.append( "(Name: ScalarExpression)" + " Type: ");
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

}
