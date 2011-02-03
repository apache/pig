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

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.InterStorage;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ScalarExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

/**
 * Logical plan visitor which will convert all column alias references to column
 * indexes, using the underlying anonymous expression plan visitor.
 */
public class ScalarVisitor extends AllExpressionVisitor {
    
    public ScalarVisitor(OperatorPlan plan) throws FrontendException {
        super( plan, new DependencyOrderWalker( plan ) );
    }

    @Override
    protected LogicalExpressionVisitor getVisitor(final LogicalExpressionPlan exprPlan)
    throws FrontendException {
        return new LogicalExpressionVisitor( exprPlan, new DependencyOrderWalker( exprPlan ) ) {
            
            @Override
            public void visit(ScalarExpression expr) throws FrontendException {
                // This is a scalar udf.
                ConstantExpression filenameConst = (ConstantExpression)exprPlan.getSuccessors( expr ).get( 1 );

                Operator refOp = expr.getImplicitReferencedOperator();
                Operator attachedOp = expr.getAttachedLogicalOperator();
                LogicalPlan lp = (LogicalPlan) attachedOp.getPlan();
                List<Operator> succs = lp.getSuccessors( refOp );
                LOStore store = null;
                if( succs != null ) {
                    for( Operator succ : succs ) {
                        if( succ instanceof LOStore ) {
                            store = (LOStore)succ;
                            break;
                        }
                    }
                }

                if( store == null ) {
                    FuncSpec funcSpec = new FuncSpec(InterStorage.class.getName());
                    FileSpec fileSpec;
                    //try {
                    // TODO: need to hookup the pigcontext.
                    fileSpec = new FileSpec( "/tmp/file.name", funcSpec );
                    //                        } catch (IOException e) {
                    //                            throw new PlanValidationException( "Failed to process scalar: " + e);
                    //                        }
                    store = new LOStore( lp, fileSpec );
                    lp.add( store );
                    lp.connect( refOp, store );
                }

                filenameConst.setValue( store.getOutputSpec().getFileName() );
                lp.createSoftLink( store, attachedOp );
            }

        };
    }
    
}
