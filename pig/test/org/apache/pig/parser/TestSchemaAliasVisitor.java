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
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.visitor.SchemaAliasVisitor;

import org.junit.Test;

public class TestSchemaAliasVisitor {

    @Test
    public void test() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = foreach A generate u, v, $0 as x, u as z; " +
                       "C = store B into 'output';";
        validate( query );
    }
    
    @Test
    public void testNegative1() throws RecognitionException, ParsingFailureException, IOException {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = foreach A generate $0, v, $0; " +
                       "C = store B into 'output';";
        try {
            validate( query );
        } catch(PlanValidationException ex) {
            Assert.assertTrue( ex.getMessage().contains( "Duplicate schema alias" ) );
            return;
        }
        Assert.fail( "Query should fail to validate." );
    }

    @Test
    // See PIG-644
    public void testNegative2() throws RecognitionException, ParsingFailureException, IOException {
         String query = "a = load '1.txt' as (a0:int, a1:int);" + 
                        "b = foreach a generate a0, a1 as a0;" +
                        "c = store b into 'output';";
         try {
             validate( query );
         } catch(PlanValidationException ex) {
             Assert.assertTrue( ex.getMessage().contains( "Duplicate schema alias" ) );
             return;
         }
         Assert.fail( "Query should fail to validate." );
     }
     
    @Test
    // See PIG-644
    public void testNegative3() throws RecognitionException, ParsingFailureException, IOException {
        String query = "a = load '1.txt' as (a0:int, a0:int);" + 
                       "store a into 'output';";
        try {
            validate( query );
        } catch(RecognitionException ex) {
            Assert.assertTrue( ex.toString().contains( "Duplicated alias in schema" ) );
            return;
        }
        Assert.fail( "Query should fail to validate." );
    }

    private LogicalPlan validate(String query) throws RecognitionException, ParsingFailureException, IOException {
        LogicalPlan plan = ParserTestingUtils.generateLogicalPlan( query );
        SchemaAliasVisitor visitor = new SchemaAliasVisitor( plan );
        visitor.visit();
        return plan;
    }

}
