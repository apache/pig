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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
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
                LogicalRelationalOperator op = expr.getAttachedRelationalOp();
                LogicalPlan lp = (LogicalPlan)op.getPlan();
                List<Operator> inputs = lp.getPredecessors( op );
                LogicalRelationalOperator input = (LogicalRelationalOperator)inputs.get( expr.getInputNum() );
                LogicalSchema inputSchema = input.getSchema();
                String alias = expr.getColAlias();
                if( alias != null ) {
                    int colNum = inputSchema.getFieldPosition( alias );
                    if( colNum == -1 ) {
                        throw new FrontendException( "Invalid field projection: " + alias );
                    }
                    expr.setColNum( colNum );
                } else {
                    int col = expr.getColNum();
                    if( col >= inputSchema.size() ) {
                        throw new FrontendException( "Out of bound access. Trying to access non-existent column: " + 
                                                      col + ". Schema " + inputSchema + " has " + inputSchema.size() + " column(s)." );
                    }
                }
            }

            public void visit(DereferenceExpression expr) throws FrontendException {
                List<Object> rawCols = expr.getRawColumns();
                if( rawCols.isEmpty() ) {
                    return;
                }
                
                List<Integer> cols = new ArrayList<Integer>( rawCols.size() );
                LogicalExpressionPlan plan = (LogicalExpressionPlan)expr.getPlan();
                LogicalExpression pred = (LogicalExpression)plan.getPredecessors( expr ).get(0);
                LogicalSchema schema = pred.getFieldSchema().schema;
                
                for( Object rc : rawCols ) {
                    if( rc instanceof Integer ) {
                        cols.add( (Integer)rc );
                    } else {
                        int col = schema.getFieldPosition( (String)rc );
                        if( col == -1 ) {
                            throw new FrontendException( "Invalid field projection: " + rc );
                        }
                        cols.add( col );
                    }
                }
                expr.setBagColumns( cols );
            }
        };
    }
}
