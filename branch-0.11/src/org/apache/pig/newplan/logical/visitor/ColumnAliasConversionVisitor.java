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
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

/**
 * Logical plan visitor which will convert all column alias references to column
 * indexes, using the underlying anonymous expression plan visitor.
 */
public class ColumnAliasConversionVisitor extends AllExpressionVisitor {
	
    public ColumnAliasConversionVisitor(OperatorPlan plan) throws FrontendException {
        super( plan, new DependencyOrderWalker( plan ) );
    }

    @Override
    protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan exprPlan)
    throws FrontendException {
        return new LogicalExpressionVisitor( exprPlan, new DependencyOrderWalker( exprPlan ) ) {
            @Override
            public void visit(ProjectExpression expr) throws FrontendException {
                expr.setColumnNumberFromAlias();
            }

            public void visit(DereferenceExpression expr) throws FrontendException {
                List<Object> rawCols = expr.getRawColumns();
                if( rawCols.isEmpty() ) {
                    return;
                }
                
                List<Integer> cols = new ArrayList<Integer>( rawCols.size() );
                LogicalExpressionPlan plan = (LogicalExpressionPlan)expr.getPlan();
                LogicalExpression pred = (LogicalExpression)plan.getSuccessors( expr ).get(0);
                
                LogicalSchema schema = null;
                if( pred.getFieldSchema().type == DataType.BAG ) {
                    if( pred.getFieldSchema().schema != null ) {
                        schema = pred.getFieldSchema().schema.getField(0).schema;
                        if (schema!=null && schema.size()==1 && schema.getField(0).type==DataType.TUPLE) {
                            schema = schema.getField(0).schema;
                        }
                    }
                }
                else {
                    schema = pred.getFieldSchema().schema;
                }
                
                int col = -1;
                for( Object rc : rawCols ) {
                    if( rc instanceof Integer ) {
                    	col = (Integer)rc;
                    	if( schema != null && schema.size()!=0 && col >= schema.size() ) {
                            throw new PlanValidationException( expr, "Out of bound access. Trying to access non-existent column: " + 
                                    col + ". Schema " + schema.toString(false) + " has " + schema.size() + " column(s).", 1000 );
                    	}
                        cols.add( (Integer)rc );
                    } else {
                        col = schema == null ? -1 : schema.getFieldPosition( (String)rc );
                        if( col == -1 ) {
                            throw new PlanValidationException( expr, "Invalid field reference. Referenced field [" + 
                            		rc + "] does not exist in schema: " + (schema!=null?schema.toString(false):"") + "." , 1000);
                        }
                        cols.add( col );
                    }
                }
                expr.setBagColumns( cols );
            }
        };
    }
}
