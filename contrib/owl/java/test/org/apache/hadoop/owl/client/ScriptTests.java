/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.owl.client;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.testdriver.ScriptDriver;
import org.junit.Test;

public class ScriptTests extends OwlTestCase {

    public ScriptTests() {
    }

    @Test
    public static void testBoundedList() throws Exception {
        ScriptDriver.runTest(getUri(), "boundedList.owl", "boundedList.exp");
    }

    @Test
    public static void testPropertyKey() throws Exception {
        ScriptDriver.runTest(getUri(), "propertyKey.owl", "propertyKey.exp");
    }

    @Test
    public static void testInterval1() throws Exception {
        ScriptDriver.runTest(getUri(), "interval1.owl", "interval1.exp");
    }

    @Test
    public static void testInterval2() throws Exception {
        ScriptDriver.runTest(getUri(), "interval2.owl", "interval2.exp");
    }

    @Test
    public static void testIntervalEndToEnd() throws Exception {
        ScriptDriver.runTest(getUri(), "interval-end-to-end.owl", "interval-end-to-end.exp");
    }

    @Test
    public static void testOperator() throws Exception {
        ScriptDriver.runTest(getUri(), "operatorTest.owl", "operatorTest.exp");
    }


    @Test
    public static void testFilter() throws Exception {
        ScriptDriver.runTest(getUri(), "testFilter.owl", "testFilter.exp");
    }
    @Test
    public static void testSelectLikeOwlTable() throws Exception {
        ScriptDriver.runTest(getUri(), "testSelectLikeOwlTable.owl", "testSelectLikeOwlTable.exp");
    }

    @Test
    public static void testNestedSchema() throws Exception {
        ScriptDriver.runTest(getUri(), "testNestedSchema.owl", "testNestedSchema.exp");
    }

    @Test
    public static void testSchemaEvolution() throws Exception {
        ScriptDriver.runTest(getUri(), "schemaEvolution.owl", "schemaEvolution.exp");
    }

    @Test
    public static void testSchemaAlter() throws Exception {
        ScriptDriver.runTest(getUri(), "schemaAlter.owl", "schemaAlter.exp");
    }

    @Test
    public static void testAlterOwlTableKeyValue() throws Exception {
        ScriptDriver.runTest(getUri(), "alterOwlTableKeyValue.owl", "alterOwlTableKeyValue.exp");
    }
}
