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
import org.apache.pig.PigServer;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.LimitOptimizer;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;

import org.junit.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOptimizeLimit {
    final String FILE_BASE_LOCATION = "test/org/apache/pig/test/data/DotFiles/" ;
    static final int MAX_SIZE = 100000;
    PigContext pc = new PigContext( ExecType.LOCAL, new Properties() );
  
    PigServer pigServer;
    
    @Before
    public void setup() throws ExecException {
        pigServer = new PigServer( pc );
    }
    
    @After
    public void tearDown() {
        
    }
    
    void compareWithGoldenFile(LogicalPlan plan, String filename) throws Exception {
        String actualPlan = printLimitGraph(plan);
        String actualPlanClean = Util.standardizeNewline(actualPlan + "\n");
        System.out.println("We get:");
        System.out.println(actualPlanClean);
        
        FileInputStream fis = new FileInputStream(filename);
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        String goldenPlanClean = Util.standardizeNewline(goldenPlan);
        System.out.println("Expected:");
        System.out.println(goldenPlanClean);
        
		Assert.assertEquals(goldenPlanClean, actualPlanClean);
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
	    String query = "A = load 'myfile';" + 
	                   "B = order A by $0;" +
	                   "C = limit B 100;" +
                       "store C into 'empty';";  
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan1.dot");
	}

	@Test
	// Merge limit into limit
	public void testOPLimit2Optimizer() throws Exception {
	    String query = "A = load 'myfile';" + 
	                   "B = limit A 10;" + 
	                   "C = limit B 100;" + "store C into 'empty';";
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan2.dot");
        Assert.assertTrue(((LOLoad) newLogicalPlan.getSources().get(0)).getLimit() == 10);
	}

	@Test
	// Duplicate limit with two inputs
	public void testOPLimit3Optimizer() throws Exception {
	    String query = "A = load 'myfile1';" +
	    "B = load 'myfile2';" +
	    "C = cross A, B;" +
	    "D = limit C 100;" + "store D into 'empty';";
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan3.dot");
	}

	@Test
	// Duplicte limit with one input
	public void testOPLimit4Optimizer() throws Exception {
	    String query = "A = load 'myfile1';" + 
	                   "B = group A by $0;" + "C = foreach B generate flatten(A);" + "D = limit C 100;" +
	                   "store D into 'empty';";
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan4.dot");
	}

	@Test
	// Move limit up
    public void testOPLimit5Optimizer() throws Exception {
        String query = "A = load 'myfile1';" + 
        "B = foreach A generate $0;" +
        "C = limit B 100;" + "store C into 'empty';" ;
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
        compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan5.dot");
        Assert.assertTrue(((LOLoad) newLogicalPlan.getSources().get(0)).getLimit() == 100);
    }
	
    @Test
    // Multiple LOLimit
	public void testOPLimit6Optimizer() throws Exception {
	    String query = "A = load 'myfile';" +
	    "B = limit A 50;" + 
	    "C = limit B 20;" +
	    "D = limit C 100;" +  "store D into 'empty';";
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
	    compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan6.dot");
        Assert.assertTrue(((LOLoad) newLogicalPlan.getSources().get(0)).getLimit() == 20);
	}
    
    @Test
    // Limit stay the same for ForEach with a flatten
    public void testOPLimit7Optimizer() throws Exception {
        String query = "A = load 'myfile1';" + 
        "B = foreach A generate flatten($0);" +
        "C = limit B 100;" + "store C into 'empty';";
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
        compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan7.dot");
    }
    
    @Test
    //Limit in the local mode, need to make sure limit stays after a sort
    public void testOPLimit8Optimizer() throws Exception {
        String query = "A = load 'myfile';" + 
        "B = order A by $0;" +
        "C = limit B 10;" + "store C into 'empty';";
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
       compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan8.dot");
        
    }
    
    @Test
    public void testOPLimit9Optimizer() throws Exception {
        String query = "A = load 'myfile';" + 
        "B = order A by $0;" +
        "C = limit B 10;" + "store C into 'empty';";
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
       compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan9.dot");
        
    }

    @Test
    //See bug PIG-913
    public void testOPLimit10Optimizer() throws Exception {
        String query = "A = load 'myfile' AS (s:chararray);" +
        "B = limit A 100;" + "C = GROUP B by $0;" + "store C into 'empty';";
	    LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
	    optimizePlan(newLogicalPlan);
        compareWithGoldenFile(newLogicalPlan, FILE_BASE_LOCATION + "new-optlimitplan10.dot");
        Assert.assertTrue(((LOLoad) newLogicalPlan.getSources().get(0)).getLimit() == 100);
    }

    @Test
    //See bug PIG-995
    //We shall throw no exception here
    public void testOPLimit11Optimizer() throws Exception {
    	String query = "B = foreach (limit (order (load 'myfile' AS (a0, a1, a2)) by $1) 10) generate $0;";
    	LogicalPlan plan = Util.buildLp(pigServer, query);
	    optimizePlan(plan);
    }
    
    @Test
    // See PIG-2570
    public void testLimitSoftLink() throws Exception {
        String query = "A = LOAD 'data1.txt' AS (owner:chararray,pet:chararray,age:int,phone:chararray);"
            + "B = group A all; "
            + "C = foreach B generate SUM(A.age) as total; "
            + "D = foreach A generate owner, age/(double)C.total AS percentAge; "
            + "F = LIMIT D C.total/8;"
            + "store F into 'output';";
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);;
        optimizePlan(newLogicalPlan);
        LOStore store = (LOStore)newLogicalPlan.getSinks().get(0);
        LOForEach foreach1 = (LOForEach)newLogicalPlan.getPredecessors(store).get(0);
        LOLimit limit = (LOLimit)newLogicalPlan.getPredecessors(foreach1).get(0);
        Assert.assertTrue(newLogicalPlan.getSoftLinkPredecessors(limit).get(0) instanceof LOStore);
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
            r = new LimitOptimizer("OptimizeLimit");
            s.add(r);
            ls.add(s);
            
            return ls;
        }
    }    

    private LogicalPlan optimizePlan(LogicalPlan plan) throws IOException {
        PlanOptimizer optimizer = new MyPlanOptimizer( plan, 3 );
        optimizer.optimize();
        return plan;
    }

}
