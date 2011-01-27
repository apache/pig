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

package org.apache.pig.parser;

import java.io.IOException;

import junit.framework.Assert;

import org.antlr.runtime.RecognitionException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.visitor.ColumnAliasConversionVisitor;

import org.junit.Test;

public class TestColumnAliasConversion {
    @Test
    public void test1() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = foreach A generate u, $1, w; " +
                       "C = store B into 'output';";
        verify( query );
    }

    @Test
    public void test2() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
                       "B = foreach A generate u.(x, y), v, w; " +
                       "C = store B into 'output';";
        verify( query );
    }
    
    @Test
    public void test3() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { R = a; S = R.u; T = limit S 100; generate S, T, c + d/5; };" +
                       "store B into 'y';";
        verify( query );
    }
    
    private void verify(String query) throws RecognitionException, ParsingFailureException, IOException {
        LogicalPlan plan = ParserTestingUtils.generateLogicalPlan( query );
        ColumnAliasConversionVisitor visitor = new ColumnAliasConversionVisitor( plan );
        visitor.visit();
        System.out.println( "Plan after setter: " + plan.toString() );
        new AllExpressionVisitor( plan, new DependencyOrderWalker( plan ) ) {
            @Override
            protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan exprPlan) throws FrontendException {
                return new LogicalExpressionVisitor( exprPlan, new DependencyOrderWalker( exprPlan ) ) {
                    @Override
                    public void visit(ProjectExpression expr) throws FrontendException {
                        Assert.assertTrue( null == expr.getColAlias() );
                        Assert.assertTrue( expr.getColNum() >= -1 );
                    }

                    public void visit(DereferenceExpression expr) throws FrontendException {
                        Assert.assertTrue( expr.getRawColumns().isEmpty() );
                        Assert.assertTrue( !expr.getBagColumns().isEmpty() );
                    }
                };
            }
            
        }.visit();
    }

}
