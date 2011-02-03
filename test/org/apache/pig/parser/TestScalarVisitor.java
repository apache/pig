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
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.visitor.ScalarVisitor;

import org.junit.Test;

public class TestScalarVisitor {

    @Test
    public void test1() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x'; " + 
                       "B = load 'y' as ( u : int, v : chararray );" +
                       "C = foreach A generate B.$1, $0; " +
                       "D = store C into 'output';";
        LogicalPlan plan = visit( query );
        Assert.assertEquals( 2, plan.getSources().size() ); // There should be two LOLoad op in the plan.
        Assert.assertEquals( 2, plan.getSinks().size() ); // There should be also two LOStore op in the plan.
        System.out.println( "New Logical Plan after scalar processing: " + plan );
    }
    
    @Test
    public void test2() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x'; " + 
                       "B = load 'y' as ( u : int, v : chararray );" +
                       "C = foreach A generate B.v, $0; " +
                       "D = store C into 'output';";
        LogicalPlan plan = visit( query );
        Assert.assertEquals( 2, plan.getSources().size() ); // There should be two LOLoad op in the plan.
        Assert.assertEquals( 2, plan.getSinks().size() ); // There should be also two LOStore op in the plan.
        System.out.println( "New Logical Plan after scalar processing: " + plan );
    }

    @Test
    public void testNegative1() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x'; " + 
                       "B = load 'y' as ( u : int, v : chararray );" +
                       "C = foreach A generate B.w, $0; " +
                       "D = store C into 'output';";
        try {
        	visit( query );
        } catch(ParsingFailureException ex) {
        	// Expected exception
        	return;
        }
        Assert.fail( "Test case should fail" );
    }

    private LogicalPlan visit(String query) throws RecognitionException, ParsingFailureException, IOException {
        LogicalPlan plan = ParserTestingUtils.generateLogicalPlan( query );
        ScalarVisitor visitor = new ScalarVisitor( plan );
        visitor.visit();
        return plan;
    }

}
