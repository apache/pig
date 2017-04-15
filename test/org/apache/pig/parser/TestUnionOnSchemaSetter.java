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

import org.junit.Assert;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.visitor.UnionOnSchemaSetter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestUnionOnSchemaSetter {
    @Test
    public void test1() throws FrontendException {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = load 'y' as ( u:int, v:int, z:long); " +
                       "C = union onschema A, B; " +
                       "D = store C into 'output';";
        LogicalPlan plan = generateLogicalPlan( query );
        if( plan != null ) {
            int nodeCount = plan.size();
            UnionOnSchemaSetter visitor = new UnionOnSchemaSetter( plan );
            visitor.visit();
            System.out.println( "Plan after setter: " + plan.toString() );
            Assert.assertEquals( nodeCount + 2, plan.size() );
        }
    }

    @Test
    public void test2() throws FrontendException {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = load 'y' as ( u:int, v:int, w:bytearray); " +
                       "C = union onschema A, B; " +
                       "D = store C into 'output';";
        LogicalPlan plan = generateLogicalPlan( query );
        if( plan != null ) {
            int nc = plan.size();
            UnionOnSchemaSetter visitor = new UnionOnSchemaSetter( plan );
            visitor.visit();
            System.out.println( "Plan after setter: " + plan.toString() );
            Assert.assertEquals( nc + 1, plan.size() );
        }
    }

    @Test
    public void test3() throws FrontendException {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = load 'y' as ( u:int, v:long, w:bytearray); " +
                       "C = union onschema A, B; " +
                       "D = store C into 'output';";
        LogicalPlan plan = generateLogicalPlan( query );
        if( plan != null ) {
            int nc = plan.size();
            UnionOnSchemaSetter visitor = new UnionOnSchemaSetter( plan );
            visitor.visit();
            System.out.println( "Plan after setter: " + plan.toString() );
            Assert.assertEquals( nc, plan.size() );
        }
    }

    @Test
    public void testMergeCompatibleSchema() throws FrontendException {
        String query = "A = load 'x' as ( u:int, v:long, w:int); " + 
                       "B = load 'y' as ( u:int, v:long, w:long); " +
                       "C = union onschema A, B; " +
                       "D = store C into 'output';";
        LogicalPlan plan = generateLogicalPlan( query );
        if( plan != null ) {
            int nc = plan.size();
            UnionOnSchemaSetter visitor = new UnionOnSchemaSetter( plan );
            visitor.visit();
            System.out.println( "Plan after setter: " + plan.toString() );
            Assert.assertEquals( nc+1, plan.size() ); // ForEach inserted before union
        }
    }
    
    private LogicalPlan generateLogicalPlan(String query) {
        try {
            return ParserTestingUtils.generateLogicalPlan( query );
        } catch(Exception ex) {
            Assert.fail( "Failed to generate logical plan for query [" + query + "] due to exception: " + ex );
        }
        return null;
    }

}
