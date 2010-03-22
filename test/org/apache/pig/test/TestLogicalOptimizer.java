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

import java.io.FileInputStream;

import org.apache.pig.ExecType;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.optimizer.ImplicitSplitInserter;
import org.apache.pig.impl.logicalLayer.optimizer.LogicalOptimizer;
import org.apache.pig.impl.logicalLayer.optimizer.OpLimitOptimizer;
import org.apache.pig.impl.logicalLayer.optimizer.TypeCastInserter;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.Test;

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
    
    public static int optimizePlan(LogicalPlan plan) throws Exception
    {
        LogicalOptimizer optimizer = new LogicalOptimizer(plan);
        return optimizer.optimize();
    }
    
    public static void optimizePlan(LogicalPlan plan, ExecType mode) throws OptimizerException {
        LogicalOptimizer optimizer = new LogicalOptimizer(plan, mode);
        optimizer.optimize();
    }
    
    void compareWithGoldenFile(LogicalPlan plan, String filename) throws Exception
    {
        FileInputStream fis = new FileInputStream(filename);
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        
        String actualPlan = printLimitGraph(plan);
        System.out.println("We get:");
        System.out.println(actualPlan);
		assertEquals(goldenPlan, actualPlan + "\n");
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
	    planTester.buildPlan("B = group A by $0;");
	    planTester.buildPlan("C = foreach B generate flatten(A);");
	    LogicalPlan plan = planTester.buildPlan("D = limit C 100;");
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
    
    @Test
    // Limit stay the same for ForEach with a flatten
    public void testOPLimit7Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile1';");
        planTester.buildPlan("B = foreach A generate flatten($0);");
        LogicalPlan plan = planTester.buildPlan("C = limit B 100;");
        optimizePlan(plan);
        compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan7.dot");
    }
    
    @Test
    //Limit in the local mode, need to make sure limit stays after a sort
    public void testOPLimit8Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile';");
        planTester.buildPlan("B = order A by $0;");
        LogicalPlan plan = planTester.buildPlan("C = limit B 10;");
        optimizePlan(plan, ExecType.LOCAL);
        compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan8.dot");
        
    }
    
    @Test
    //Limit in the local mode, need to make sure limit stays after a sort
    public void testOPLimit9Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile';");
        planTester.buildPlan("B = order A by $0;");
        LogicalPlan plan = planTester.buildPlan("C = limit B 10;");
        optimizePlan(plan);
        compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan9.dot");
        
    }

    @Test
    //See bug PIG-913
    public void testOPLimit10Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile' AS (s:chararray);");
        planTester.buildPlan("B = limit A 100;");
        LogicalPlan plan = planTester.buildPlan("C = GROUP B by $0;");
        optimizePlan(plan);
        compareWithGoldenFile(plan, FILE_BASE_LOCATION + "optlimitplan10.dot");
    }

    /**
     * Test that {@link OpLimitOptimizer} returns false on the check if 
     * pre-conditions for pushing limit up are not met
     * @throws Exception
     */
    @Test
    public void testOpLimitOptimizerCheck() throws Exception {
        planTester.buildPlan("A = load 'myfile';");
        planTester.buildPlan("B = foreach A generate $0;");
        LogicalPlan plan = planTester.buildPlan("C = limit B 100;");
        LogicalOptimizerDerivative optimizer = new LogicalOptimizerDerivative(plan);
        int numIterations = optimizer.optimize();
        assertFalse("Checking number of iterations of the optimizer [actual = "
                + numIterations + ", expected < " + optimizer.getMaxIterations() + 
                "]", optimizer.getMaxIterations() == numIterations);
    
    }
    
    @Test
    //Test to ensure that the right exception is thrown
    public void testErrImplicitSplitInserter() throws Exception {
        LogicalPlan lp = new LogicalPlan();
        ImplicitSplitInserter isi = new ImplicitSplitInserter(lp);
        try {
            isi.transform(lp.getRoots());
        } catch(Exception e) {
            assertTrue(((OptimizerException)e).getErrorCode() == 2052);
        }
    }
    
    @Test
    //Test to ensure that the right exception is thrown
    public void testErrTypeCastInserter() throws Exception {
        LogicalPlan lp = new LogicalPlan();
        TypeCastInserter tci = new TypeCastInserter(lp, "hello");
        try {
            tci.transform(lp.getRoots());
        } catch(Exception e) {
            assertTrue(((OptimizerException)e).getErrorCode() == 2052);
        }
    }
    
    @Test
    //Test to ensure that the right exception is thrown
    public void testErrOpLimitOptimizer() throws Exception {
        LogicalPlan lp = new LogicalPlan();
        OpLimitOptimizer olo = new OpLimitOptimizer(lp);
        try {
            olo.transform(lp.getRoots());
        } catch(Exception e) {
            assertTrue(((OptimizerException)e).getErrorCode() == 2052);
        }
    }
    
    @Test
    //See bug PIG-995
    //We shall throw no exception here
    public void testOPLimit11Optimizer() throws Exception {
        LogicalPlan plan = planTester.buildPlan("B = foreach (limit (order (load 'myfile' AS (a0, a1, a2)) by $1) 10) generate $0;");
        optimizePlan(plan);
    }

    // a subclass of LogicalOptimizer which can return the maximum iterations
    // the optimizer would try the check() and transform() methods 
    static class LogicalOptimizerDerivative extends LogicalOptimizer {
        public LogicalOptimizerDerivative(LogicalPlan plan) {
            super(plan);
        }
        
        public int getMaxIterations() {
            return mMaxIterations;
        }
    }
}

