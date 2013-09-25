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

import org.apache.pig.FuncSpec;
import org.apache.pig.StoreFuncInterface;
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
import org.apache.pig.parser.LogicalPlanBuilder;

/**
 * Logical plan visitor which handles scalar projections. It will find or create a LOStore 
 * and a soft link between the store operator to a scalar expression. It will also sync the file name
 * between the store and scalar expression.
 */
public class ScalarVisitor extends AllExpressionVisitor {
    private final PigContext pigContext;
    private final String scope;
    
    public ScalarVisitor(OperatorPlan plan, PigContext pigContext, String scope) throws FrontendException {
        super( plan, new DependencyOrderWalker( plan ) );
        this.pigContext = pigContext;
        this.scope = scope;
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
                FuncSpec interStorageFuncSpec = new FuncSpec(InterStorage.class.getName());
                if( succs != null ) {
                    for( Operator succ : succs ) {
                        if( succ instanceof LOStore
                                && ((LOStore)succ).isTmpStore()
                                && interStorageFuncSpec.equals(
                                    ((LOStore)succ).getOutputSpec().getFuncSpec() ) ) {
                            store = (LOStore)succ;
                            break;
                        }
                    }
                }

                if( store == null ) {
                    FileSpec fileSpec;
                    try {
                        fileSpec = new FileSpec( FileLocalizer.getTemporaryPath( pigContext ).toString(), interStorageFuncSpec );                    // TODO: need to hookup the pigcontext.
                    } catch (IOException e) {
                        throw new PlanValidationException( expr, "Failed to process scalar" + e);
                    }
                    StoreFuncInterface stoFunc = (StoreFuncInterface)PigContext.instantiateFuncFromSpec(interStorageFuncSpec);
                    String sig = LogicalPlanBuilder.newOperatorKey(scope);
                    stoFunc.setStoreFuncUDFContextSignature(sig);
                    store = new LOStore(lp, fileSpec, stoFunc, sig);
                    store.setTmpStore(true);
                    lp.add( store );
                    lp.connect( refOp, store );
                    expr.setImplicitReferencedOperator(store);
                }

                filenameConst.setValue( store.getOutputSpec().getFileName() );
                
                if( lp.getSoftLinkSuccessors( store ) == null || 
                    !lp.getSoftLinkSuccessors( store ).contains( attachedOp ) ) {
                    lp.createSoftLink( store, attachedOp );
                }
            }

        };
    }
    
}
