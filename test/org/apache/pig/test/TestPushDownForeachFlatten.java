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


import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.optimizer.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.test.utils.Identity;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.apache.pig.impl.plan.optimizer.OptimizerException;

import org.junit.Test;
import org.junit.Before;

/**
 * Test the logical optimizer.
 */

public class TestPushDownForeachFlatten extends junit.framework.TestCase {

    final String FILE_BASE_LOCATION = "test/org/apache/pig/test/data/DotFiles/" ;
    static final int MAX_SIZE = 100000;

    private final Log log = LogFactory.getLog(getClass());
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    LogicalPlanTester planTester = new LogicalPlanTester(pc) ;
    

    private static final String simpleEchoStreamingCommand;
    static {
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            simpleEchoStreamingCommand = "perl -ne 'print \\\"$_\\\"'";
        else
            simpleEchoStreamingCommand = "perl -ne 'print \"$_\"'";
    }

    
    @Before
    public void tearDown() {
        planTester.reset();
    }

    /**
     * 
     * A simple filter UDF for testing
     *
     */
    static public class MyFilterFunc extends FilterFunc {
    	
    	@Override
    	public Boolean exec(Tuple input) {
    		return false;
    	}
    }
    
    @Test
    //Test to ensure that the right exception is thrown when the input list is empty
    public void testErrorEmptyInput() throws Exception {
        LogicalPlan lp = new LogicalPlan();
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        try {
            pushDownForeach.check(lp.getRoots());
            fail("Exception Expected!");
        } catch(Exception e) {
            assertTrue(((OptimizerException)e).getErrorCode() == 2052);
        }
    }

    @Test
    //Test to ensure that the right exception is thrown when the input list is empty
    public void testErrorNonForeachInput() throws Exception {
        LogicalPlan lp = planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");;
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        try {
            pushDownForeach.check(lp.getRoots());
            fail("Exception Expected!");
        } catch(Exception e) {
            assertTrue(((OptimizerException)e).getErrorCode() == 2005);
        }
    }
    
    @Test
    public void testForeachNoFlatten() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        LogicalPlan lp = planTester.buildPlan("B = foreach A generate $1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        assertTrue(!pushDownForeach.check(lp.getLeaves()));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
    }
    
    @Test
    public void testForeachNoSuccessors() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        LogicalPlan lp = planTester.buildPlan("B = foreach A generate flatten($1);");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        assertTrue(!pushDownForeach.check(lp.getLeaves()));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
    }
    
    @Test
    public void testForeachStreaming() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate flatten($1);");
        LogicalPlan lp = planTester.buildPlan("C = stream B through `" + simpleEchoStreamingCommand + "`;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
    }
    
    @Test
    public void testForeachDistinct() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate flatten($1);");
        LogicalPlan lp = planTester.buildPlan("C = distinct B;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
    }
    
    @Test
    public void testForeachForeach() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");        
        LogicalPlan lp = planTester.buildPlan("C = foreach B generate $0;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
    }
    

    @Test
    public void testForeachFilter() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $1 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
    }

    @Test
    public void testForeachSplitOutput() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        LogicalPlan lp = planTester.buildPlan("split B into C if $1 < 18, D if $1 >= 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
    }

    @Test
    public void testForeachLimit() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        LogicalPlan lp = planTester.buildPlan("B = limit B 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
    }

    @Test
    public void testForeachUnion() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference);");
        LogicalPlan lp = planTester.buildPlan("D = union B, C;");        
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);        
    }
    
    @Test
    public void testForeachCogroup() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference);");
        LogicalPlan lp = planTester.buildPlan("D = cogroup B by $0, C by $0;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);        
    }
    
    @Test
    public void testForeachGroupBy() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        LogicalPlan lp = planTester.buildPlan("C = group B by $0;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
    }
    
    @Test
    public void testForeachSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");        
        LogicalPlan lp = planTester.buildPlan("C = order B by $0, $1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOSort sort = (LOSort) lp.getLeaves().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(sort).get(0);
        
        assertTrue(pushDownForeach.check(lp.getPredecessors(sort)));
        assertTrue(pushDownForeach.getSwap() == true);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
        pushDownForeach.transform(lp.getPredecessors(sort));
        
        assertEquals(foreach, lp.getLeaves().get(0));
        assertEquals(sort, lp.getPredecessors(foreach).get(0));
        
    }
    
    @Test
    public void testForeachFlattenAddedColumnSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");
        LogicalPlan lp = planTester.buildPlan("C = order B by $0, $1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOSort sort = (LOSort) lp.getLeaves().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getPredecessors(sort)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
    }
    
    @Test
    public void testForeachUDFSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $0, $1, " + Identity.class.getName() + "($2) ;");
        LogicalPlan lp = planTester.buildPlan("C = order B by $0, $1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOSort sort = (LOSort) lp.getLeaves().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getPredecessors(sort)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
    }
    
    @Test
    public void testForeachCastSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate (chararray)$0, $1, flatten($2);");        
        LogicalPlan lp = planTester.buildPlan("C = order B by $0, $1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);
        
        LOSort sort = (LOSort) lp.getLeaves().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getPredecessors(sort)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);
        
    }
    
    @Test
    public void testForeachCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("D = cross B, C;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad load = (LOLoad) lp.getRoots().get(0);
        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOCross cross = (LOCross)lp.getPredecessors(limit).get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(cross).get(0);
        
        Schema limitSchema = limit.getSchema();
        
        assertTrue(pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == true);
        assertTrue(pushDownForeach.getFlattenedColumnMap() != null);

        pushDownForeach.transform(lp.getSuccessors(load));
        
        planTester.rebuildSchema(lp);
        
        for(Boolean b: foreach.getFlatten()) {
            assertEquals(b.booleanValue(), false);
        }
        
        LOForEach newForeach = (LOForEach)lp.getSuccessors(cross).get(0);
        
        
        List<Boolean> newForeachFlatten = newForeach.getFlatten();
        Map<Integer, Integer> remap = pushDownForeach.getFlattenedColumnMap();        
        for(Integer key: remap.keySet()) {
            Integer value = remap.get(key);
            assertEquals(newForeachFlatten.get(value).booleanValue(), true);
        }
        
        assertTrue(Schema.equals(limitSchema, limit.getSchema(), false, true));        
        
    }

    @Test
    public void testForeachCross1() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("C = foreach B generate $0, $1, flatten($2);");
        planTester.buildPlan("D = cross A, C;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad load = (LOLoad) lp.getRoots().get(1);
        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOCross cross = (LOCross)lp.getPredecessors(limit).get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(cross).get(1);
        
        Schema limitSchema = limit.getSchema();
        
        assertTrue(pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == true);
        assertTrue(pushDownForeach.getFlattenedColumnMap() != null);

        pushDownForeach.transform(lp.getSuccessors(load));
        
        planTester.rebuildSchema(lp);
        
        for(Boolean b: foreach.getFlatten()) {
            assertEquals(b.booleanValue(), false);
        }
        
        LOForEach newForeach = (LOForEach)lp.getSuccessors(cross).get(0);
        
        
        List<Boolean> newForeachFlatten = newForeach.getFlatten();
        Map<Integer, Integer> remap = pushDownForeach.getFlattenedColumnMap();        
        for(Integer key: remap.keySet()) {
            Integer value = remap.get(key);
            assertEquals(newForeachFlatten.get(value).booleanValue(), true);
        }
        
        assertTrue(Schema.equals(limitSchema, limit.getSchema(), false, true));       
        
    }

    // TODO
    // The following test case testForeachCross2 has multiple foreach flatten
    // A new rule should optimize this case
    @Test
    public void testForeachCross2() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = foreach C generate $0, $1, flatten($2);");
        planTester.buildPlan("E = cross B, D;");
        LogicalPlan lp = planTester.buildPlan("F = limit E 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }
    
    @Test
    public void testForeachFlattenAddedColumnCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = cross B, C;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }

    @Test
    public void testForeachUDFCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, flatten($1), " + Identity.class.getName() + "($2) ;");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = cross B, C;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }

    @Test
    public void testForeachCastCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, (int)$1, $2;");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = cross B, C;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }
    
    @Test
    public void testForeachFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("D = join B by $0, C by $0 using \"replicated\";");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad load = (LOLoad) lp.getRoots().get(0);
        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOJoin frjoin = (LOJoin)lp.getPredecessors(limit).get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(frjoin).get(0);
        
        Schema limitSchema = limit.getSchema();
        
        assertTrue(pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == true);
        assertTrue(pushDownForeach.getFlattenedColumnMap() != null);

        pushDownForeach.transform(lp.getSuccessors(load));
        
        planTester.rebuildSchema(lp);
        
        for(Boolean b: foreach.getFlatten()) {
            assertEquals(b.booleanValue(), false);
        }
        
        LOForEach newForeach = (LOForEach)lp.getSuccessors(frjoin).get(0);
        
        
        List<Boolean> newForeachFlatten = newForeach.getFlatten();
        Map<Integer, Integer> remap = pushDownForeach.getFlattenedColumnMap();        
        for(Integer key: remap.keySet()) {
            Integer value = remap.get(key);
            assertEquals(newForeachFlatten.get(value).booleanValue(), true);
        }
        
        assertTrue(Schema.equals(limitSchema, limit.getSchema(), false, true));        

    }
    

    @Test
    public void testForeachFRJoin1() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("C = foreach B generate $0, $1, flatten($2);");
        planTester.buildPlan("D = join A by $0, C by $0 using \"replicated\";");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad load = (LOLoad) lp.getRoots().get(1);
        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOJoin frjoin = (LOJoin)lp.getPredecessors(limit).get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(frjoin).get(1);
        
        Schema limitSchema = limit.getSchema();
        
        assertTrue(pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == true);
        assertTrue(pushDownForeach.getFlattenedColumnMap() != null);

        pushDownForeach.transform(lp.getSuccessors(load));
        
        planTester.rebuildSchema(lp);
        
        for(Boolean b: foreach.getFlatten()) {
            assertEquals(b.booleanValue(), false);
        }
        
        LOForEach newForeach = (LOForEach)lp.getSuccessors(frjoin).get(0);
        
        
        List<Boolean> newForeachFlatten = newForeach.getFlatten();
        Map<Integer, Integer> remap = pushDownForeach.getFlattenedColumnMap();        
        for(Integer key: remap.keySet()) {
            Integer value = remap.get(key);
            assertEquals(newForeachFlatten.get(value).booleanValue(), true);
        }
        
        assertTrue(Schema.equals(limitSchema, limit.getSchema(), false, true));       

    }

    // TODO
    // The following test case testForeachFRJoin2 has multiple foreach flatten
    // A new rule should optimize this case
    @Test
    public void testForeachFRJoin2() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = foreach C generate $0, $1, flatten($2);");
        planTester.buildPlan("E = join B by $0, D by $0 using \"replicated\";");
        LogicalPlan lp = planTester.buildPlan("F = limit E 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }
    
    @Test
    public void testForeachFlattenAddedColumnFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0 using \"replicated\";");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }

    @Test
    public void testForeachUDFFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, flatten($1), " + Identity.class.getName() + "($2) ;");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0 using \"replicated\";");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }

    @Test
    public void testForeachCastFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, (int)$1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0 using \"replicated\";");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }

    @Test
    public void testForeachInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad load = (LOLoad) lp.getRoots().get(0);
        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOJoin join = (LOJoin)lp.getPredecessors(limit).get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(join).get(0);
        
        Schema limitSchema = limit.getSchema();
        
        assertTrue(pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == true);
        assertTrue(pushDownForeach.getFlattenedColumnMap() != null);

        pushDownForeach.transform(lp.getSuccessors(load));
        
        planTester.rebuildSchema(lp);
        
        for(Boolean b: foreach.getFlatten()) {
            assertEquals(b.booleanValue(), false);
        }
        
        LOForEach newForeach = (LOForEach)lp.getSuccessors(join).get(0);
        
        
        List<Boolean> newForeachFlatten = newForeach.getFlatten();
        Map<Integer, Integer> remap = pushDownForeach.getFlattenedColumnMap();        
        for(Integer key: remap.keySet()) {
            Integer value = remap.get(key);
            assertEquals(newForeachFlatten.get(value).booleanValue(), true);
        }
        
        assertTrue(Schema.equals(limitSchema, limit.getSchema(), false, true));        

    }
    
    @Test
    public void testForeachInnerJoin1() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("C = foreach B generate $0, $1, flatten($2);");
        planTester.buildPlan("D = join A by $0, C by $0;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad load = (LOLoad) lp.getRoots().get(1);
        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOJoin join = (LOJoin)lp.getPredecessors(limit).get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(join).get(1);
        
        Schema limitSchema = limit.getSchema();
        
        assertTrue(pushDownForeach.check(lp.getSuccessors(load)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == true);
        assertTrue(pushDownForeach.getFlattenedColumnMap() != null);

        pushDownForeach.transform(lp.getSuccessors(load));
        
        planTester.rebuildSchema(lp);
        
        for(Boolean b: foreach.getFlatten()) {
            assertEquals(b.booleanValue(), false);
        }
        
        LOForEach newForeach = (LOForEach)lp.getSuccessors(join).get(0);
        
        
        List<Boolean> newForeachFlatten = newForeach.getFlatten();
        Map<Integer, Integer> remap = pushDownForeach.getFlattenedColumnMap();        
        for(Integer key: remap.keySet()) {
            Integer value = remap.get(key);
            assertEquals(newForeachFlatten.get(value).booleanValue(), true);
        }
        
        assertTrue(Schema.equals(limitSchema, limit.getSchema(), false, true));       

    }


    // TODO
    // The following test case testForeachInnerJoin2 has multiple foreach flatten
    // A new rule should optimize this case
    @Test
    public void testForeachInnerJoin2() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = foreach C generate $0, $1, flatten($2);");
        planTester.buildPlan("E = join B by $0, D by $0;");
        LogicalPlan lp = planTester.buildPlan("F = limit E 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }
    
    @Test
    public void testForeachFlattenAddedColumnInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, $1, flatten(1);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }

    @Test
    public void testForeachUDFInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, flatten($1), " + Identity.class.getName() + "($2) ;");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }

    @Test
    public void testForeachCastInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa:(letter_grade, point_score));");
        planTester.buildPlan("B = foreach A generate $0, (int)$1, flatten($2);");
        planTester.buildPlan("C = load 'anotherfile' as (name, age, preference:(course_name, instructor));");
        planTester.buildPlan("D = join B by $0, C by $0;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
        assertTrue(pushDownForeach.getFlattenedColumnMap() == null);

    }

    // See PIG-1172
    @Test
    public void testForeachJoinRequiredField() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (bg:bag{t:tuple(a0,a1)});");
        planTester.buildPlan("B = FOREACH A generate flatten($0);");
        planTester.buildPlan("C = load '3.txt' AS (c0, c1);");
        planTester.buildPlan("D = JOIN B by a1, C by c1;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        planTester.rebuildSchema(lp);
        
        PushDownForeachFlatten pushDownForeach = new PushDownForeachFlatten(lp);

        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        
        assertTrue(!pushDownForeach.check(lp.getSuccessors(loada)));
        assertTrue(pushDownForeach.getSwap() == false);
        assertTrue(pushDownForeach.getInsertBetween() == false);
    }

}

