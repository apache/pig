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
package org.apache.pig.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.optimizer.*;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.apache.pig.test.utils.TypeCheckingTestUtil;

import org.junit.Test;
import org.junit.Before;

/**
 * Test the logical optimizer.
 */

public class TestLogicalOptimizer extends junit.framework.TestCase {

    final String FILE_BASE_LOCATION = "test/org/apache/pig/test/data/DotFiles/" ;
    static final int MAX_SIZE = 100000;

    LogicalPlanTester planTester = new LogicalPlanTester() ;

    /*
    @Before
    public void setUp() {
        planTester.reset();
    }*/

    public static String printLimitGraph(LogicalPlan plan) {
    	OpLimitOptimizerPrinter printer = new OpLimitOptimizerPrinter(plan) ;
        String rep = "digraph graph1 {\n";
    	rep = rep + printer.printToString() ;
    	rep = rep + "}";
        return rep;
    }
    
    public static void optimizePlan(LogicalPlan plan) throws Exception
    {
        LogicalOptimizer optimizer = new LogicalOptimizer(plan);
        optimizer.optimize();
    }
    
    void compareWithGoldenFile(LogicalPlan plan, String filename) throws Exception
    {
        FileInputStream fis = new FileInputStream(filename);
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        
		String actualPlan = printLimitGraph(plan);		
		assertEquals(goldenPlan, actualPlan);
    }
    
    @Test
    public void testTypeCastInsertion() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (p:int, q:long, r:float, "
            + "s:double, t:map [], u:tuple (x:int, y:int), " + 
            "v:bag {x:tuple(z:int)});");
        LogicalPlan plan = planTester.buildPlan("B = order A by p;");
        planTester.typeCheckAgainstDotFile(plan, FILE_BASE_LOCATION +
            "optplan1.dot", true);
    }
    
    @Test
    // Merget limit into sort
	public void testOPLimit1Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile';");
	    planTester.buildPlan("B = order A by $0;");
	    LogicalPlan plan = planTester.buildPlan("C = limit B 100;");
	    optimizePlan(plan);
	    compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan1.dot");
	}

	@Test
	// Merge limit into limit
	public void testOPLimit2Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile';");
	    planTester.buildPlan("B = limit A 10;");
	    LogicalPlan plan = planTester.buildPlan("C = limit B 100;");
	    optimizePlan(plan);
	    compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan2.dot");
	}

	@Test
	// Duplicate limit with two inputs
	public void testOPLimit3Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile1';");
	    planTester.buildPlan("B = load 'myfile2';");
	    planTester.buildPlan("C = cross A, B;");
	    LogicalPlan plan = planTester.buildPlan("D = limit C 100;");
	    optimizePlan(plan);
	    compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan3.dot");
	}

	@Test
	// Duplicte limit with one input
	public void testOPLimit4Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile1';");
	    planTester.buildPlan("B = distinct A;");
	    LogicalPlan plan = planTester.buildPlan("C = limit B 100;");
	    optimizePlan(plan);
	    compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan4.dot");
	}

	@Test
	// Move limit up
    public void testOPLimit5Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile1';");
        planTester.buildPlan("B = foreach A generate $0;");
        LogicalPlan plan = planTester.buildPlan("C = limit B 100;");
        optimizePlan(plan);
        compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan5.dot");
    }
	
    @Test
    // Multiple LOLimit
	public void testOPLimit6Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile';");
	    planTester.buildPlan("B = limit A 50;");
	    planTester.buildPlan("C = limit B 20;");
	    LogicalPlan plan = planTester.buildPlan("D = limit C 100;");
	    optimizePlan(plan);
	    compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan6.dot");
	}
}

