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

import org.junit.Assert;

import org.antlr.runtime.RecognitionException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.PlanValidationException;
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
                       "B = foreach A { R = a; P = c * 2; Q = P + d; S = R.u; T = limit S 100; generate Q, R, S, T, c + d/5; };" +
                       "store B into 'y';";
        verify( query );
    }
    
    @Test
    public void test4() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
                       "B = foreach A generate u.(x, $1), $1, w; " +
                       "C = store B into 'output';";
        validate( query );
    }
    
    @Test
    public void test5() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:bag{} ); " + 
                       "B = foreach A generate u.$100; " +
                       "C = store B into 'output';";
        validate( query );
    }
    
    @Test
    public void test6() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x'; " + 
                       "B = foreach A generate $1, $1000; " +
                       "C = store B into 'output';";
        validate( query );
    }

    @Test
    public void test7() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x'; " + 
                       "B = load 'y' as ( u : int, v : chararray );" +
                       "C = foreach A generate B.$1, $0; " +
                       "D = store C into 'output';";
        validate( query );
    }
    
   @Test
    public void testNegative1() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
                       "B = foreach A generate u.(x, $3), v, w; " +
                       "C = store B into 'output';";
        try {
            validate( query );
        }catch(ParserValidationException ex){
            return;
        }
        Assert.fail( "Query should fail to validate." );
    }
    
    @Test
    public void testNegative2() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
                       "B = foreach A generate u.(x, y), v, $5; " +
                       "C = store B into 'output';";
        try {
            validate( query );
        } catch(PlanValidationException ex) {
            return;
        }
        Assert.fail( "Query should fail to validate." );
    }
    
    @Test
    public void testNegative3() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
                       "B = foreach A generate u.(x, y), v, x; " +
                       "C = store B into 'output';";
        try {
            validate( query );
        } catch(PlanValidationException ex) {
            return;
        }
        Assert.fail( "Query should fail to validate." );
    }
    
    @Test
    public void testNegative4() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
                       "B = foreach A generate u.z, v, w; " +
                       "C = store B into 'output';";
        try {
            validate( query );
        } catch(ParserValidationException ex) {
            return;
        }
        Assert.fail( "Query should fail to validate." );
    }
    
    @Test
    public void testNegative5() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x';" + 
                       "B = foreach A generate u, $1; " +
                       "C = store B into 'output';";
        try {
            validate( query );
        } catch(PlanValidationException ex) {
            return;
        }
        Assert.fail( "Query should fail to validate." );
    }

    @Test
    public void testInvalidNestedProjection() throws Exception {
        String query = "A = load 'x' as (field);" +
                       "B = foreach A {" +
                       "  C = LIMIT invalidName 1;" +
                       "  generate C.foo;" +
                       "};";
        try {
            validate( query );
        } catch(PlanValidationException ex) {
            System.out.println(ex.getMessage());
            return;
        }
        Assert.fail( "Query should fail to validate." );
    }

    private LogicalPlan validate(String query) throws RecognitionException, ParsingFailureException, IOException {
        LogicalPlan plan = ParserTestingUtils.generateLogicalPlan( query );
        ColumnAliasConversionVisitor visitor = new ColumnAliasConversionVisitor( plan );
        visitor.visit();
        return plan;
    }

    private void verify(String query) throws RecognitionException, ParsingFailureException, IOException {
        LogicalPlan plan = validate( query );
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
