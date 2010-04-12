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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.test.utils.LogicalPlanTester;


public class TestRewire extends junit.framework.TestCase {

    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    private final Log log = LogFactory.getLog(getClass());
    LogicalPlanTester planTester = new LogicalPlanTester(pc);
    
    @After
    @Override
    public void tearDown() throws Exception{
        planTester.reset(); 
    }

    private static final String simpleEchoStreamingCommand;
    static {
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            simpleEchoStreamingCommand = "perl -ne 'print \\\"$_\\\"'";
        else
            simpleEchoStreamingCommand = "perl -ne 'print \"$_\"'";
    }

    
    @Test
    public void testQueryForeachFilterSwap() throws Exception {
        String query = "filter (foreach (load 'a') generate $1,$2) by $1 > 0;";
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        LOForEach foreach = (LOForEach)lp.getSuccessors(load).get(0);
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        
        lp.swap(filter, foreach);
        
        LogicalPlan filterPlan = filter.getComparisonPlan();
        LOProject filterProject;
        List<LogicalOperator> filterRoots = filterPlan.getRoots();
        if(filterRoots.get(0) instanceof LOProject) {
            filterProject = (LOProject)filterRoots.get(0);
        } else {
            filterProject = (LOProject)filterRoots.get(1);
        }
        assertTrue(filterProject.getExpression().equals(load));
        assertTrue(filterProject.getCol() == 2);
        
        LogicalPlan foreachPlan = foreach.getForEachPlans().get(0);
        LOProject foreachProject = (LOProject) foreachPlan.getRoots().get(0);
        assertTrue(foreachProject.getExpression().equals(filter));
        assertTrue(foreachProject.getCol() == 1);
        
        foreachPlan = foreach.getForEachPlans().get(1);
        foreachProject = (LOProject) foreachPlan.getRoots().get(0);
        assertTrue(foreachProject.getExpression().equals(filter));
        assertTrue(foreachProject.getCol() == 2);
        
    }
    
    @Test
    public void testQueryForeachFilterSwap1() throws Exception {
        planTester.buildPlan("a = foreach (foreach (load 'a') generate $1,$2 )  generate $0, $1;");
        String query = "b = filter a by $1 > 0;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        LOForEach foreach = (LOForEach)lp.getSuccessors(load).get(0);
        LOForEach foreach2 = (LOForEach)lp.getSuccessors(foreach).get(0);
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        
        lp.swap(filter, foreach2);
        
        LogicalPlan filterPlan = filter.getComparisonPlan();
        LOProject filterProject;
        List<LogicalOperator> filterRoots = filterPlan.getRoots();
        if(filterRoots.get(0) instanceof LOProject) {
            filterProject = (LOProject)filterRoots.get(0);
        } else {
            filterProject = (LOProject)filterRoots.get(1);
        }
        assertTrue(filterProject.getExpression().equals(foreach));
        assertTrue(filterProject.getCol() == 1);
        
        LogicalPlan foreach2Plan = foreach2.getForEachPlans().get(0);
        LOProject foreachProject = (LOProject) foreach2Plan.getRoots().get(0);
        assertTrue(foreachProject.getExpression().equals(filter));
        assertTrue(foreachProject.getCol() == 0);
        
        foreach2Plan = foreach2.getForEachPlans().get(1);
        foreachProject = (LOProject) foreach2Plan.getRoots().get(0);
        assertTrue(foreachProject.getExpression().equals(filter));
        assertTrue(foreachProject.getCol() == 1);
        
        lp.swap(filter, foreach);
        
        filterRoots = filterPlan.getRoots();
        if(filterRoots.get(0) instanceof LOProject) {
            filterProject = (LOProject)filterRoots.get(0);
        } else {
            filterProject = (LOProject)filterRoots.get(1);
        }
        assertTrue(filterProject.getExpression().equals(load));
        assertTrue(filterProject.getCol() == 2);
        
        LogicalPlan foreachPlan = foreach.getForEachPlans().get(0);
        foreachProject = (LOProject) foreachPlan.getRoots().get(0);
        assertTrue(foreachProject.getExpression().equals(filter));
        assertTrue(foreachProject.getCol() == 1);
        
        foreachPlan = foreach.getForEachPlans().get(1);
        foreachProject = (LOProject) foreachPlan.getRoots().get(0);
        assertTrue(foreachProject.getExpression().equals(filter));
        assertTrue(foreachProject.getCol() == 2);
    }

    @Test
    public void testNegativeQueryForeach1() throws Exception {
        String query = "foreach (load 'a') generate $1,$2;";
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        
        try {
            lp.swap(load, foreach);
            fail("Expected failure.");
        } catch (Exception e){
            PigException pe = LogUtils.getPigException(e);
            assertTrue(pe.getErrorCode() == 1100);
        }
        
    }

    @Test
    public void testQuerySortFilterSwap() throws Exception {
        String query = "filter (order (load 'a') by $1,$2) by $1 > 0;";
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        LOSort sort = (LOSort)lp.getSuccessors(load).get(0);
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        
        lp.swap(filter, sort);
        
        LogicalPlan filterPlan = filter.getComparisonPlan();
        LOProject filterProject;
        List<LogicalOperator> filterRoots = filterPlan.getRoots();
        if(filterRoots.get(0) instanceof LOProject) {
            filterProject = (LOProject)filterRoots.get(0);
        } else {
            filterProject = (LOProject)filterRoots.get(1);
        }
        assertTrue(filterProject.getExpression().equals(load));
        assertTrue(filterProject.getCol() == 1);
        
        LogicalPlan sortPlan = sort.getSortColPlans().get(0);
        LOProject sortProject = (LOProject) sortPlan.getRoots().get(0);
        assertTrue(sortProject.getExpression().equals(filter));
        assertTrue(sortProject.getCol() == 1);
        
        sortPlan = sort.getSortColPlans().get(1);
        sortProject = (LOProject) sortPlan.getRoots().get(0);
        assertTrue(sortProject.getExpression().equals(filter));
        assertTrue(sortProject.getCol() == 2);
        
    }
    
    @Test
    public void testQuerySplitFilterInsertBetween() throws Exception {
        planTester.buildPlan("a = load 'a';");
        planTester.buildPlan("b = foreach a generate $0, $1, $2;");
        planTester.buildPlan("split b into d if $0 == '3', c if $1 == '3';");
        String query = "filter d by $2 > 0;";
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        LOForEach foreach = (LOForEach)lp.getSuccessors(load).get(0);
        LOSplit split = (LOSplit)lp.getSuccessors(foreach).get(0);
        LOSplitOutput splitd = (LOSplitOutput)lp.getSuccessors(split).get(0);
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        
        lp.insertBetween(foreach, filter, split);
        
        LogicalPlan filterPlan = filter.getComparisonPlan();
        LOProject filterProject;
        List<LogicalOperator> filterRoots = filterPlan.getRoots();
        if(filterRoots.get(0) instanceof LOProject) {
            filterProject = (LOProject)filterRoots.get(0);
        } else {
            filterProject = (LOProject)filterRoots.get(1);
        }
        assertTrue(filterProject.getExpression().equals(foreach));
        assertTrue(filterProject.getCol() == 2);
        
        LogicalPlan splitdPlan = splitd.getConditionPlan();
        LOProject splitdProject;
        List<LogicalOperator> splitdRoots = splitdPlan.getRoots();
        if(splitdRoots.get(0) instanceof LOProject) {
            splitdProject = (LOProject)splitdRoots.get(0);
        } else {
            splitdProject = (LOProject)splitdRoots.get(1);
        }

        assertTrue(splitdProject.getExpression().equals(filter));
        assertTrue(splitdProject.getCol() == 0);
        
    }

    @Test
    public void testQueryFRJoinFilterPushBefore() throws Exception {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        planTester.buildPlan("b = load 'b' as (url, rank);");
        planTester.buildPlan("c = join a by $0, b by $0 using \"replicated\" ;");
        planTester.buildPlan("d = filter c by $0 > 0;");
        String query = "e = foreach d generate $2 + 0;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        LOJoin frjoin = (LOJoin)lp.getSuccessors(loada).get(0);
        LOFilter filter = (LOFilter)lp.getSuccessors(frjoin).get(0);
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        
        lp.pushBefore(frjoin, filter, 0);
        
        LogicalPlan filterPlan = filter.getComparisonPlan();
        LOProject filterProject;
        List<LogicalOperator> filterRoots = filterPlan.getRoots();
        if(filterRoots.get(0) instanceof LOProject) {
            filterProject = (LOProject)filterRoots.get(0);
        } else {
            filterProject = (LOProject)filterRoots.get(1);
        }
        assertTrue(filterProject.getExpression().equals(loada));
        assertTrue(filterProject.getCol() == 0);
        
        
        LogicalPlan joinPlan = ((List<LogicalPlan>)(frjoin.getJoinPlans().get(filter))).get(0);
        LOProject joinProject = (LOProject)joinPlan.getRoots().get(0);
        assertTrue(joinProject.getExpression().equals(filter));
        
        LogicalPlan foreachPlan = foreach.getForEachPlans().get(0);
        LOProject foreachProject;
        
        if(foreachPlan.getRoots().get(0) instanceof LOProject) {
            foreachProject = (LOProject) foreachPlan.getRoots().get(0);
        } else {
            foreachProject = (LOProject) foreachPlan.getRoots().get(1);
        }
        
        assertTrue(foreachProject.getExpression().equals(frjoin));
        assertTrue(foreachProject.getCol() == 2);
        
    }
    
    @Test
    public void testQueryCogroupFilterPushBefore() throws Exception {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        planTester.buildPlan("b = load 'b' as (url, rank);");
        planTester.buildPlan("c = cogroup a by $1, b by $1;");
        planTester.buildPlan("d = filter c by $0 > 0;");
        String query = "e = foreach d generate $0 + 0;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        LOFilter filter = (LOFilter)lp.getSuccessors(cogroup).get(0);
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        
        lp.pushBefore(cogroup, filter, 1);
        
        LogicalPlan filterPlan = filter.getComparisonPlan();
        LOProject filterProject;
        List<LogicalOperator> filterRoots = filterPlan.getRoots();
        if(filterRoots.get(0) instanceof LOProject) {
            filterProject = (LOProject)filterRoots.get(0);
        } else {
            filterProject = (LOProject)filterRoots.get(1);
        }
        assertTrue(filterProject.getExpression().equals(loadb));
        assertTrue(filterProject.getCol() == 1);
        
        LogicalPlan cogroupPlan = ((List<LogicalPlan>)(cogroup.getGroupByPlans().get(filter))).get(0);
        LOProject cogroupProject = (LOProject)cogroupPlan.getRoots().get(0);
        assertTrue(cogroupProject.getExpression().equals(filter));
        
        LogicalPlan foreachPlan = foreach.getForEachPlans().get(0);
        LOProject foreachProject;
        
        if(foreachPlan.getRoots().get(0) instanceof LOProject) {
            foreachProject = (LOProject) foreachPlan.getRoots().get(0);
        } else {
            foreachProject = (LOProject) foreachPlan.getRoots().get(1);
        }
        
        assertTrue(foreachProject.getExpression().equals(cogroup));
        assertTrue(foreachProject.getCol() == 0);
        
    }
    
    @Test
    public void testQueryFRJoinFilterPushBeforeNegative() throws Exception {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        planTester.buildPlan("b = load 'b' ;");
        planTester.buildPlan("c = join a by $0, b by $0 using \"replicated\" ;");
        planTester.buildPlan("d = filter c by $0 > 0;");
        String query = "e = foreach d generate $2 + 0;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        LOJoin frjoin = (LOJoin)lp.getSuccessors(loada).get(0);
        LOFilter filter = (LOFilter)lp.getSuccessors(frjoin).get(0);
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        
        try {
            lp.pushBefore(frjoin, filter, 0);
            fail("Expected failure.");
        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            assertTrue(pe.getErrorCode() == 2156);
        }        
    }
    
    @Test
    public void testQueryCogroupFilterPushBeforeNegative() throws Exception {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        planTester.buildPlan("b = load 'b' ;");
        planTester.buildPlan("c = cogroup a by $0, b by $0;");
        planTester.buildPlan("d = filter c by $1 > 0;");
        String query = "e = foreach d generate $0 + 0;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        LOFilter filter = (LOFilter)lp.getSuccessors(cogroup).get(0);
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        
        try {
            lp.pushBefore(cogroup, filter, 0);
            fail("Expected failure.");
        } catch(Exception e) {
            PigException pe = LogUtils.getPigException(e);
            assertTrue(pe.getErrorCode() == 2158);
        }
        
    }

    
    @Test
    public void testQueryCogroupTrimAboveNegative() throws Exception {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        planTester.buildPlan("b = load 'b' as (url, rank);");
        planTester.buildPlan("c = cogroup a by $0, b by $0;");
        planTester.buildPlan("d = filter c by $0 > 0;");
        String query = "e = foreach d generate $0 + 0;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);

        try {
            lp.trimAbove(cogroup);
            fail("Excepted failure.");
        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            assertTrue(pe.getErrorCode() == 1097);
        }
        
    }
    
    @Test
    public void testQueryCogroupTrimAboveNegative1() throws Exception {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        planTester.buildPlan("b = load 'b' as (url, rank);");
        planTester.buildPlan("c = cogroup a by 0, b by 0;");
        planTester.buildPlan("d = filter c by $0 > 0;");
        String query = "e = foreach d generate $0 + 0;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        
        try {
            lp.trimAbove(cogroup);
            fail("Excepted failure");
        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            assertTrue(pe.getErrorCode() == 1097);
        }
        
    }
    
    @Test
    public void testQueryReplaceFilter() throws Exception {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        planTester.buildPlan("b = load 'b' as (url, rank);");
        planTester.buildPlan("c = cogroup a by $0, b by $0;");
        planTester.buildPlan("d = filter c by $0 > 0;");
        String query = "e = foreach d generate $0 + 0;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        LOFilter filter = (LOFilter)lp.getSuccessors(cogroup).get(0);
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        
        LogicalPlan foreachPlan = foreach.getForEachPlans().get(0);
        LogicalPlanCloner lpCloner = new LogicalPlanCloner(foreachPlan);
        LogicalPlan foreachPlanClone = lpCloner.getClonedPlan();
        ArrayList<LogicalPlan> newForeachPlans = new ArrayList<LogicalPlan>();
        newForeachPlans.add(foreachPlanClone);
        
        LOProject newForeachProject;
        if(foreachPlanClone.getRoots().get(0) instanceof LOProject) {
            newForeachProject = (LOProject)foreachPlanClone.getRoots().get(0); 
        } else {
            newForeachProject = (LOProject)foreachPlanClone.getRoots().get(1);
        }
        
        newForeachProject.setExpression(cogroup);
        
        ArrayList<Boolean> flattenList = new ArrayList<Boolean>();
        flattenList.add(true);
        LOForEach newForeach = new LOForEach(lp, new OperatorKey("", 1000), newForeachPlans, flattenList);        
        lp.replace(filter, newForeach);
        
        LOProject foreachProject;
        if(foreachPlan.getRoots().get(0) instanceof LOProject) {
            foreachProject = (LOProject)foreachPlan.getRoots().get(0); 
        } else {
            foreachProject = (LOProject)foreachPlan.getRoots().get(1);
        }
        
        assertTrue(foreachProject.getExpression().equals(newForeach));
        assertTrue(foreachProject.getCol() == 0);
        
        //ensure that the new foreach projection is not altered
        assertTrue(newForeachProject.getExpression().equals(cogroup));
        assertTrue(newForeachProject.getCol() == 0);
    }
    
    @Test
    public void testQueryReemoveFilterAndReconnect() throws Exception {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        planTester.buildPlan("b = load 'b' as (url, rank);");
        planTester.buildPlan("c = cogroup a by $0, b by $0;");
        planTester.buildPlan("d = filter c by $0 > 0;");
        String query = "e = foreach d generate $0 + 0;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        planTester.setPlan(lp);
        planTester.setProjectionMap(lp);
        
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        LOFilter filter = (LOFilter)lp.getSuccessors(cogroup).get(0);
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        
        LogicalPlan foreachPlan = foreach.getForEachPlans().get(0);

        lp.removeAndReconnect(filter);
        
        LOProject foreachProject;
        if(foreachPlan.getRoots().get(0) instanceof LOProject) {
            foreachProject = (LOProject)foreachPlan.getRoots().get(0); 
        } else {
            foreachProject = (LOProject)foreachPlan.getRoots().get(1);
        }
        
        assertTrue(foreachProject.getExpression().equals(cogroup));
        assertTrue(foreachProject.getCol() == 0);
        
    }
    
    //TODO
    /*
     * Currently, there are no use cases for pushAfter making it hard to test rewire
     * in the case of pushAfter. This test case will fail as the foreach is pushed
     * after the split and before the splitOutput instead of being pushed after
     * the splitOutput
     */
/*
    @Test
    public void testQueryForeachSplitPushAfter()  {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        planTester.buildPlan("b = group a by $0;");
        planTester.buildPlan("c = foreach b generate group, flatten(a);");
        String query = "split c into d if $0 > 10, e if $0 < 10;";
        
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        LOForEach foreach = (LOForEach)lp.getSuccessors(cogroup).get(0);
        LOSplit split = (LOSplit)lp.getSuccessors(foreach).get(0);
        LOSplitOutput splitd = (LOSplitOutput)lp.getLeaves().get(0);
        LOSplitOutput splite = (LOSplitOutput)lp.getLeaves().get(1);
        
        try {
            lp.pushAfter(split, foreach, 0);
        } catch (PlanException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        LogicalPlan splitdPlan = splitd.getConditionPlan();
        LOProject splitdProject;
        List<LogicalOperator> splitdRoots = splitdPlan.getRoots();
        if(splitdRoots.get(0) instanceof LOProject) {
            splitdProject = (LOProject)splitdRoots.get(0);
        } else {
            splitdProject = (LOProject)splitdRoots.get(1);
        }
        log.info("splitdProject.getExpression(): " + splitdProject.getExpression());
        assertTrue(splitdProject.getExpression().equals(foreach));

        LogicalPlan splitePlan = splite.getConditionPlan();
        LOProject spliteProject;
        List<LogicalOperator> spliteRoots = splitePlan.getRoots();
        if(spliteRoots.get(0) instanceof LOProject) {
            spliteProject = (LOProject)spliteRoots.get(0);
        } else {
            spliteProject = (LOProject)spliteRoots.get(1);
        }
        assertTrue(spliteProject.getExpression().equals(foreach));

        
        LogicalPlan foreachPlan = foreach.getForEachPlans().get(0);
        LOProject foreachProject;
        
        if(foreachPlan.getRoots().get(0) instanceof LOProject) {
            foreachProject = (LOProject) foreachPlan.getRoots().get(0);
        } else {
            foreachProject = (LOProject) foreachPlan.getRoots().get(1);
        }
        
        assertTrue(foreachProject.getExpression().equals(split));
        
    }
*/
}
