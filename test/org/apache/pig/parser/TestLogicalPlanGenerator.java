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


import junit.framework.Assert;

import org.junit.Test;

public class TestLogicalPlanGenerator {
    @Test
    public void test1() {
        String query = "A = load 'x' using org.apache.pig.TextLoader( 'a', 'b' ) as ( u:int, v:long, w:bytearray); " + 
                       "B = limit A 100; " +
                       "C = filter B by 2 > 1; " +
                       "D = load 'y' as (d1, d2); " +
                       "E = join C by ( $0, $1 ), D by ( d1, d2 ) using 'replicated' parallel 16; " +
                       "F = store E into 'output';";
        generateLogicalPlan( query );
    }

    @Test
    public void test2() {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = distinct A partition by org.apache.pig.Identity; " +
                       "C = sample B 0.49; " +
                       "D = order C by $0, $1; " +
                       "E = load 'y' as (d1, d2); " +
                       "F = union onschema D, E; " +
                       "G = load 'z' as (g1:int, g2:tuple(g21, g22)); " +
                       "H = cross F, G; " +
                       "split H into I if 10 > 5, J if 'world' eq 'hello', K if 77 <= 200; " +
                       "L = store J into 'output';";
        generateLogicalPlan( query );
    }

    @Test
    public void test3() {
    }
    
    private void generateLogicalPlan(String query) {
        try {
            ParserTestingUtils.generateLogicalPlan( query );
        } catch(Exception ex) {
            Assert.fail( "Failed to generate logical plan for query [" + query + "] due to exception: " + ex );
        }
    }

    @Test
    public void test4() {
        String query = "A = load 'x'; " + 
                       "B = mapreduce '" + "myjar.jar" + "' " +
                           "Store A into 'table_testNativeMRJobSimple_input' "+
                           "Load 'table_testNativeMRJobSimple_output' "+
                           "`org.apache.pig.test.utils.WordCount -files " + "file " +
                           "table_testNativeMRJobSimple_input table_testNativeMRJobSimple_output " +
                           "stopworld.file" + "`;" +
                        "C = Store B into 'output';";
        generateLogicalPlan( query );
    }

    // Test define function.
    @Test
    public void test5() {
        String query = "define myudf org.apache.pig.TextLoader( 'test', 'data' );" +
                       "A = load 'x' using myudf;" +
                       "store A into 'y';";
        generateLogicalPlan( query );
    }
}
