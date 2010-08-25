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
import java.io.IOException;
import java.util.*;

import org.apache.pig.ExecType;
import org.apache.pig.newplan.logical.LogicalPlanMigrationVistor;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.OptimizeLimit;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.optimizer.OpLimitOptimizer;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.test.TestLogicalOptimizer.LogicalOptimizerDerivative;
import org.apache.pig.test.utils.LogicalPlanTester;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOptimizeLimit {
    final String FILE_BASE_LOCATION = "test/org/apache/pig/test/data/DotFiles/" ;
    static final int MAX_SIZE = 100000;
    PigContext pc = new PigContext( ExecType.LOCAL, new Properties() );
  
    LogicalPlanTester planTester = new LogicalPlanTester(pc) ;
    
    @BeforeClass
    public static void setup() {
        
    }
    
    @AfterClass
    public static void tearDown() {
        
    }
    
    void compareWithGoldenFile(LogicalPlan plan, String filename) throws Exception {
        String actualPlan = printLimitGraph(plan);
        System.out.println("We get:");
        System.out.println(actualPlan);
        
        FileInputStream fis = new FileInputStream(filename);
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        System.out.println("Expected:");
        System.out.println(goldenPlan);
        
		Assert.assertEquals(goldenPlan, actualPlan + "\n");
    }

    public static String printLimitGraph(LogicalPlan plan) throws IOException {
    	OptimizeLimitPlanPrinter printer = new OptimizeLimitPlanPrinter(plan) ;
        String rep = "digraph graph1 {\n";
    	rep = rep + printer.printToString() ;
    	rep = rep + "}";
        return rep;
    }

    @Test
    // Merget limit into sort
	public void testOPLimit1Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile';");
	    planTester.buildPlan("B = order A by $0;");
	    planTester.buildPlan("C = limit B 100;");
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan( "store C into 'empty';" );  
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan1.dot");
	}

	@Test
	// Merge limit into limit
	public void testOPLimit2Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile';");
	    planTester.buildPlan("B = limit A 10;");
	    planTester.buildPlan("C = limit B 100;");
	    org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan( "store C into 'empty';" );
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan2.dot");
	}

	@Test
	// Duplicate limit with two inputs
	public void testOPLimit3Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile1';");
	    planTester.buildPlan("B = load 'myfile2';");
	    planTester.buildPlan("C = cross A, B;");
	    org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan("D = limit C 100;");
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan3.dot");
	}

	@Test
	// Duplicte limit with one input
	public void testOPLimit4Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile1';");
	    planTester.buildPlan("B = group A by $0;");
	    planTester.buildPlan("C = foreach B generate flatten(A);");
	    org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan("D = limit C 100;");
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan4.dot");
	}

	@Test
	// Move limit up
    public void testOPLimit5Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile1';");
        planTester.buildPlan("B = foreach A generate $0;");
        planTester.buildPlan("C = limit B 100;");
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan( "store C into 'empty';" );
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
        compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan5.dot");
    }
	
    @Test
    // Multiple LOLimit
	public void testOPLimit6Optimizer() throws Exception {
	    planTester.buildPlan("A = load 'myfile';");
	    planTester.buildPlan("B = limit A 50;");
	    planTester.buildPlan("C = limit B 20;");
	    planTester.buildPlan("D = limit C 100;");
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan( "store D into 'empty';" );
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan6.dot");
	}
    
    @Test
    // Limit stay the same for ForEach with a flatten
    public void testOPLimit7Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile1';");
        planTester.buildPlan("B = foreach A generate flatten($0);");
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan("C = limit B 100;");
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
        compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan7.dot");
    }
    
    @Test
    //Limit in the local mode, need to make sure limit stays after a sort
    public void testOPLimit8Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile';");
        planTester.buildPlan("B = order A by $0;");
        planTester.buildPlan("C = limit B 10;");
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan( "store C into 'empty';" );
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
        compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan8.dot");
        
    }
    
    @Test
    public void testOPLimit9Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile';");
        planTester.buildPlan("B = order A by $0;");
        planTester.buildPlan("C = limit B 10;");
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan( "store C into 'empty';" );
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
        compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan9.dot");
        
    }

    @Test
    //See bug PIG-913
    public void testOPLimit10Optimizer() throws Exception {
        planTester.buildPlan("A = load 'myfile' AS (s:chararray);");
        planTester.buildPlan("B = limit A 100;");
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan("C = GROUP B by $0;");
	    LogicalPlan newLogicalPlan = migrateAndOptimizePlan(plan);
        compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan10.dot");
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
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan("C = limit B 100;");
        LogicalOptimizerDerivative optimizer = new LogicalOptimizerDerivative(plan);
        int numIterations = optimizer.optimize();
        Assert.assertFalse("Checking number of iterations of the optimizer [actual = "
                + numIterations + ", expected < " + optimizer.getMaxIterations() + 
                "]", optimizer.getMaxIterations() == numIterations);
    
    }

    @Test
    //Test to ensure that the right exception is thrown
    public void testErrOpLimitOptimizer() throws Exception {
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = new org.apache.pig.impl.logicalLayer.LogicalPlan();
        OpLimitOptimizer olo = new OpLimitOptimizer(lp);
        try {
            olo.transform(lp.getRoots());
        } catch(Exception e) {
            Assert.assertTrue(((OptimizerException)e).getErrorCode() == 2052);
        }
    }
    
    @Test
    //See bug PIG-995
    //We shall throw no exception here
    public void testOPLimit11Optimizer() throws Exception {
    	org.apache.pig.impl.logicalLayer.LogicalPlan plan = planTester.buildPlan("B = foreach (limit (order (load 'myfile' AS (a0, a1, a2)) by $1) 10) generate $0;");
	    migrateAndOptimizePlan(plan);
    }


    public class MyPlanOptimizer extends LogicalPlanOptimizer {
        protected MyPlanOptimizer(OperatorPlan p,  int iterations) {
            super( p, iterations, new HashSet<String>() );
        }
        
        protected List<Set<Rule>> buildRuleSets() {            
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
            
            Set<Rule> s = null;
            Rule r = null;
            
            s = new HashSet<Rule>();
            r = new LoadTypeCastInserter( "TypeCastInserter");
            s.add(r);
            ls.add(s);
            
            s = new HashSet<Rule>();
            r = new OptimizeLimit("OptimizeLimit");
            s.add(r);
            ls.add(s);
            
            return ls;
        }
    }    

    private LogicalPlan migrateAndOptimizePlan(org.apache.pig.impl.logicalLayer.LogicalPlan plan) throws IOException {
        LogicalPlan newLogicalPlan = migratePlan( plan );
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
    }

    private LogicalPlan migratePlan(org.apache.pig.impl.logicalLayer.LogicalPlan lp) throws VisitorException{
        LogicalPlanMigrationVistor visitor = new LogicalPlanMigrationVistor(lp);        
        visitor.visit();
        org.apache.pig.newplan.logical.relational.LogicalPlan newPlan = visitor.getNewLogicalPlan();
        return newPlan;
    }
}
