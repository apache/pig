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


import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.optimizer.*;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.apache.pig.impl.plan.optimizer.OptimizerException;

import org.junit.Test;
import org.junit.Before;

/**
 * Test the logical optimizer.
 */

public class TestPushUpFilter extends junit.framework.TestCase {

    final String FILE_BASE_LOCATION = "test/org/apache/pig/test/data/DotFiles/" ;
    static final int MAX_SIZE = 100000;

    LogicalPlanTester planTester = new LogicalPlanTester() ;

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
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        try {
            pushUpFilter.check(lp.getRoots());
            fail("Exception Expected!");
        } catch(Exception e) {
            assertTrue(((OptimizerException)e).getErrorCode() == 2052);
        }
    }

    @Test
    //Test to ensure that the right exception is thrown when the input list is empty
    public void testErrorNonFilterInput() throws Exception {
        LogicalPlan lp = planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");;
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        try {
            pushUpFilter.check(lp.getRoots());
            fail("Exception Expected!");
        } catch(Exception e) {
            assertTrue(((OptimizerException)e).getErrorCode() == 2005);
        }
    }
    
    @Test
    public void testFilterLoad() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        LogicalPlan lp = planTester.buildPlan("B = filter A by $1 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterStreaming() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = stream A through `" + simpleEchoStreamingCommand + "`;");
        LogicalPlan lp = planTester.buildPlan("C = filter B by $1 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = order A by $1, $2;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $1 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == true);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
        LOFilter filter = (LOFilter) lp.getLeaves().get(0);
        LOSort sort = (LOSort)lp.getPredecessors(filter).get(0);
        
        pushUpFilter.transform(lp.getLeaves());
        
        assertEquals(sort, lp.getLeaves().get(0));
        assertEquals(filter, lp.getPredecessors(sort).get(0));
        
    }

    @Test
    public void testFilterConstantConditionSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = order A by $1, $2;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterUDFSort() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = order A by $1, $2;");        
        LogicalPlan lp = planTester.buildPlan("D = filter B by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterDistinct() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = distinct A;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $1 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == true);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);

        LOFilter filter = (LOFilter) lp.getLeaves().get(0);
        LODistinct distinct = (LODistinct)lp.getPredecessors(filter).get(0);

        pushUpFilter.transform(lp.getLeaves());
        
        assertEquals(distinct, lp.getLeaves().get(0));
        assertEquals(filter, lp.getPredecessors(distinct).get(0));

    }

    @Test
    public void testFilterConstantConditionDistinct() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = distinct A;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterUDFDistinct() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = distinct A;");        
        LogicalPlan lp = planTester.buildPlan("D = filter B by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterForeach() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $1, $2;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $0 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == true);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
        LOFilter filter = (LOFilter) lp.getLeaves().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(filter).get(0);
        
        pushUpFilter.transform(lp.getLeaves());
        
        assertEquals(foreach, lp.getLeaves().get(0));
        assertEquals(filter, lp.getPredecessors(foreach).get(0));
        
    }

    @Test
    public void testFilterForeachCast() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate (chararray)$1, $2;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $0 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterCastForeach() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $1, $2;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by (int)$0 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    
    @Test
    public void testFilterConstantConditionForeach() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $1, $2;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterUDFForeach() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $1, $2;");        
        LogicalPlan lp = planTester.buildPlan("D = filter B by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterForeachFlatten() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = foreach A generate $1, flatten($2);");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $0 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterFilter() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = filter A by $0 != 'name';");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $1 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterSplitOutput() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("split A into B if $1 < 18, C if $1 >= 18;");
        LogicalPlan lp = planTester.buildPlan("D = filter B by $1 < 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterLimit() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = limit A 10;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $1 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    
    @Test
    public void testFilterUnion() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = union A, B;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by $1 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterConstantConditionUnion() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = union A, B;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterUDFUnion() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = union A, B;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cross A, B;");
        planTester.buildPlan("D = filter C by $5 < 18;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);

        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOFilter filter = (LOFilter) lp.getPredecessors(limit).get(0);
        LOCross cross = (LOCross)lp.getPredecessors(filter).get(0);

        
        assertTrue(pushUpFilter.check(lp.getPredecessors(limit)));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == true);
        assertTrue(pushUpFilter.getPushBeforeInput() == 1);

        pushUpFilter.transform(lp.getPredecessors(limit));
        
        assertEquals(cross, lp.getPredecessors(limit).get(0));
        assertEquals(filter, lp.getPredecessors(cross).get(1));
        
    }

    @Test
    public void testFilterCross1() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cross A, B;");
        planTester.buildPlan("D = filter C by $1 < 18;");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");

        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);

        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOFilter filter = (LOFilter) lp.getPredecessors(limit).get(0);
        LOCross cross = (LOCross)lp.getPredecessors(filter).get(0);

        
        assertTrue(pushUpFilter.check(lp.getPredecessors(limit)));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == true);
        assertTrue(pushUpFilter.getPushBeforeInput() == 0);

        pushUpFilter.transform(lp.getPredecessors(limit));
        
        assertEquals(cross, lp.getPredecessors(limit).get(0));
        assertEquals(filter, lp.getPredecessors(cross).get(0));
        
    }

    
    @Test
    public void testFilterCross2() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cross A, B;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by $1 < 18 and $5 < 18;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterConstantConditionCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cross A, B;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterUDFCross() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cross A, B;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterCogroup() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cogroup A by $0, B by $0;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by $0 < 'name';");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterConstantConditionCogroup() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cogroup A by $0, B by $0;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterUDFCogroup() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cogroup A by $0, B by $0;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterCogroupOuter() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cogroup A by $0, B by $0 outer;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by $0 < 'name';");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterConstantConditionCogroupOuter() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cogroup A by $0, B by $0 outer;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterUDFCogroupOuter() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = cogroup A by $0, B by $0 outer;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterGroupBy() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = group A by $0;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $0 < 'name';");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == true);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);

        LOFilter filter = (LOFilter) lp.getLeaves().get(0);
        LOCogroup groupBy = (LOCogroup)lp.getPredecessors(filter).get(0);

        pushUpFilter.transform(lp.getLeaves());
        
        assertEquals(groupBy, lp.getLeaves().get(0));
        assertEquals(filter, lp.getPredecessors(groupBy).get(0));
    }
    
    @Test
    public void testFilterConstantConditionGroupBy() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = group A by $0;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterUDFGroupBy() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = group A by $0;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by " + MyFilterFunc.class.getName() + "() ;");

        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterGroupByOuter() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = group A by $0 outer;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by $0 < 'name';");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == true);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
        LOFilter filter = (LOFilter) lp.getLeaves().get(0);
        LOCogroup groupBy = (LOCogroup)lp.getPredecessors(filter).get(0);

        pushUpFilter.transform(lp.getLeaves());
        
        assertEquals(groupBy, lp.getLeaves().get(0));
        assertEquals(filter, lp.getPredecessors(groupBy).get(0));
        
    }
    
    @Test
    public void testFilterConstantConditionGroupByOuter() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = group A by $0 outer;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by 1 == 1;");

        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterUDFGroupByOuter() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = group A by $0 outer;");        
        LogicalPlan lp = planTester.buildPlan("C = filter B by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

    @Test
    public void testFilterFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = join A by $0, B by $0 using \"replicated\";");        
        planTester.buildPlan("D = filter C by $0 < 'name';");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);

        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOFilter filter = (LOFilter) lp.getPredecessors(limit).get(0);
        LOFRJoin frjoin = (LOFRJoin)lp.getPredecessors(filter).get(0);

        assertTrue(pushUpFilter.check(lp.getPredecessors(limit)));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == true);
        assertTrue(pushUpFilter.getPushBeforeInput() == 0);

        
        pushUpFilter.transform(lp.getPredecessors(limit));
        
        assertEquals(frjoin, lp.getPredecessors(limit).get(0));
        assertEquals(filter, lp.getPredecessors(frjoin).get(0));

    }
    
    @Test
    public void testFilterFRJoin1() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = join A by $0, B by $0 using \"replicated\";");        
        planTester.buildPlan("D = filter C by $4 < 'name';");
        LogicalPlan lp = planTester.buildPlan("E = limit D 10;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);

        LOLimit limit = (LOLimit) lp.getLeaves().get(0);
        LOFilter filter = (LOFilter) lp.getPredecessors(limit).get(0);
        LOFRJoin frjoin = (LOFRJoin)lp.getPredecessors(filter).get(0);

        assertTrue(pushUpFilter.check(lp.getPredecessors(limit)));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == true);
        assertTrue(pushUpFilter.getPushBeforeInput() == 1);

        
        pushUpFilter.transform(lp.getPredecessors(limit));
        
        assertEquals(frjoin, lp.getPredecessors(limit).get(0));
        assertEquals(filter, lp.getPredecessors(frjoin).get(1));

    }
    
    
    @Test
    public void testFilterConstantConditionFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = join A by $0, B by $0 using \"replicated\";");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterUDFFRJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = join A by $0, B by $0 using \"replicated\";");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = join A by $0, B by $0;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by $0 < 'name';");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterConstantConditionInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = join A by $0, B by $0;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by 1 == 1;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }
    
    @Test
    public void testFilterUDFInnerJoin() throws Exception {
        planTester.buildPlan("A = load 'myfile' as (name, age, gpa);");
        planTester.buildPlan("B = load 'anotherfile' as (name, age, preference);");
        planTester.buildPlan("C = join A by $0, B by $0;");        
        LogicalPlan lp = planTester.buildPlan("D = filter C by " + MyFilterFunc.class.getName() + "() ;");
        
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        PushUpFilter pushUpFilter = new PushUpFilter(lp);
        
        assertTrue(!pushUpFilter.check(lp.getLeaves()));
        assertTrue(pushUpFilter.getSwap() == false);
        assertTrue(pushUpFilter.getPushBefore() == false);
        assertTrue(pushUpFilter.getPushBeforeInput() == -1);
        
    }

}

