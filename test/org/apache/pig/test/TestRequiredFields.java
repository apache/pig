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
import java.util.ArrayList;
import java.util.Set;

import org.junit.After;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.test.utils.LogicalPlanTester;


public class TestRequiredFields extends junit.framework.TestCase {

    private final Log log = LogFactory.getLog(getClass());
    LogicalPlanTester planTester = new LogicalPlanTester();
    
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
    public void testQueryForeach1() {
        String query = "foreach (load 'a') generate $1,$2;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load required fields is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadRequiredFields = load.getRequiredFields();
        assertTrue(loadRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>, <0, 2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);
        
    }

    @Test
    public void testQueryForeach2() {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "(':')) generate $1, 'aoeuaoeu' ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load required fields is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadRequiredFields = load.getRequiredFields();
        assertTrue(loadRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }

    @Test
    public void testQueryCogroup1() {
        String query = "foreach (cogroup (load 'a') by $1, (load 'b') by $1) generate org.apache.pig.builtin.AVG($1) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        
        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 1);
        
        //check that the foreach required fields contain [<0, 1>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }

    @Test
    public void testQueryGroupAll() throws Exception {
        String query = "foreach (group (load 'a') ALL) generate $1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 1);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }

    @Test
    public void testQueryGroup2() {
        String query = "foreach (group (load 'a') by $1) generate group, '1' ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 1);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        
        //check that the foreach required fields contain [<0, 0>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
    }

    @Test
    public void testQueryCogroup2() {
        String query = "foreach (cogroup (load 'a') by ($1), (load 'b') by ($1)) generate $1.$1, $2.$1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);


        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        
        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 1);
        
        //check that the foreach required fields contain [<0, 1>, <0,2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);
    }

    @Test
    public void testQueryGroup3() {
        String query = "foreach (group (load 'a') by ($6, $7)) generate flatten(group) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 1);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 6);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 7);
        
        //check that the foreach required fields contain [<0, 0>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
    }

    @Test
    public void testQueryFilterNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = filter a by $1 == '3';");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check filter required fields
        LOFilter filter = (LOFilter)lp.getSuccessors(loada).get(0);
        List<RequiredFields> filterRequiredFields = filter.getRequiredFields();
        assertTrue(filterRequiredFields.size() == 1);
        
        requiredField = filterRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }
    
    @Test
    public void testQuerySplitNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("split a into b if $0 == '3', c if $1 == '3';");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOSplit split = (LOSplit)lp.getSuccessors(loada).get(0);
        List<RequiredFields> splitRequiredFields = split.getRequiredFields();
        assertTrue(splitRequiredFields.size() == 1);
        
        requiredField = splitRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.getFields() == null);
        
        //check split outputs' required fields
        LOSplitOutput splitb = (LOSplitOutput)lp.getSuccessors(split).get(0);
        List<RequiredFields> splitbRequiredFields = splitb.getRequiredFields();
        assertTrue(splitbRequiredFields.size() == 1);
        
        requiredField = splitbRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        
        LOSplitOutput splitc = (LOSplitOutput)lp.getSuccessors(split).get(1);
        List<RequiredFields> splitcRequiredFields = splitc.getRequiredFields();
        assertTrue(splitcRequiredFields.size() == 1);
        
        requiredField = splitcRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }

    @Test
    public void testQueryOrderByNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = order a by $1;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check order by required fields
        LOSort sort = (LOSort)lp.getSuccessors(loada).get(0);
        List<RequiredFields> sortRequiredFields = sort.getRequiredFields();
        assertTrue(sortRequiredFields.size() == 1);
        
        requiredField = sortRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }
    
    @Test
    public void testQueryLimitNoSchema() {
        planTester.buildPlan("a = load 'a';");
        planTester.buildPlan("b = order a by $1;");
        LogicalPlan lp = planTester.buildPlan("c = limit b 10;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check order by required fields
        LOSort sort = (LOSort)lp.getSuccessors(loada).get(0);
        List<RequiredFields> sortRequiredFields = sort.getRequiredFields();
        assertTrue(sortRequiredFields.size() == 1);
        
        requiredField = sortRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);

        //check limit required fields
        LOLimit limit = (LOLimit)lp.getLeaves().get(0);
        List<RequiredFields> limitRequiredFields = limit.getRequiredFields();
        assertTrue(limitRequiredFields.size() == 1);
        
        requiredField = limitRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.getFields() == null);

    }

    @Test
    public void testQueryDistinctNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = distinct a;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check distinct required fields
        LODistinct distinct = (LODistinct)lp.getSuccessors(loada).get(0);
        List<RequiredFields> distinctRequiredFields = distinct.getRequiredFields();
        assertTrue(distinctRequiredFields.size() == 1);
        
        requiredField = distinctRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

    }

    @Test
    public void testQueryStreamingNoSchema() {
        String query = "stream (load 'a') through `" + simpleEchoStreamingCommand + "`;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check streaming required fields
        LOStream stream = (LOStream)lp.getSuccessors(loada).get(0);
        List<RequiredFields> streamRequiredFields = stream.getRequiredFields();
        assertTrue(streamRequiredFields.size() == 1);
        
        requiredField = streamRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

    }
    
    @Test
    public void testQueryStreamingNoSchema1() {
        String query = "stream (load 'a' as (url, hitCount)) through `" + simpleEchoStreamingCommand + "` ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check streaming required fields
        LOStream stream = (LOStream)lp.getSuccessors(loada).get(0);
        List<RequiredFields> streamRequiredFields = stream.getRequiredFields();
        assertTrue(streamRequiredFields.size() == 1);
        
        requiredField = streamRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

    }
    
    @Test
    public void testQueryForeach3() {
        String query = "foreach (load 'a') generate ($1 == '3'? $2 : $3) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>, <0, 2>, <0,3>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 3);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);
        assertTrue(fields.get(2).first == 0);
        assertTrue(fields.get(2).second == 3);
    }

    @Test
    public void testQueryForeach4() {
        planTester.buildPlan("A = load 'a';");
        planTester.buildPlan("B = load 'b';");
        LogicalPlan lp = planTester.buildPlan("foreach (cogroup A by ($1), B by ($1)) generate A, flatten(B.($1, $2, $3));");
        
        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        
        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 1);
        
        //check that the foreach required fields contain [<0, 1>, <0, 2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);
    }

    @Test
    public void testForeach5() {
        planTester.buildPlan("A = load 'a';");
        planTester.buildPlan("B = load 'b';");
        planTester.buildPlan("C = cogroup A by ($1), B by ($1);");
        String query = "foreach C { " +
                "B = order B by $0; " +
                "generate FLATTEN(A), B.($1, $2, $3) ;" +
                "};" ;
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        
        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 1);
        
        //check that the foreach required fields contain [<0, 1>, <0, 2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);

    }

    @Test
    public void testQueryCrossNoSchema(){
        String query = "c = cross (load 'a'), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cross required fields
        LOCross cross = (LOCross)lp.getSuccessors(loada).get(0);
        List<RequiredFields> crossRequiredFields = cross.getRequiredFields();
        assertTrue(crossRequiredFields.size() == 2);
        
        requiredField = crossRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

        requiredField = crossRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
        
    }

    @Test
    public void testQueryUnionNoSchema(){
        String query = "c = union (load 'a'), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check union required fields
        LOUnion union = (LOUnion)lp.getSuccessors(loada).get(0);
        List<RequiredFields> unionRequiredFields = union.getRequiredFields();
        assertTrue(unionRequiredFields.size() == 2);
        
        requiredField = unionRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

        requiredField = unionRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
        
    }

    @Test
    public void testQueryFRJoinNoSchema(){
        String query = "c = join (load 'a') by $0, (load 'b') by $0 using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check frjoin required fields
        LOFRJoin frjoin = (LOFRJoin)lp.getSuccessors(loada).get(0);
        List<RequiredFields> frjoinRequiredFields = frjoin.getRequiredFields();
        assertTrue(frjoinRequiredFields.size() == 2);
        
        requiredField = frjoinRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);


        requiredField = frjoinRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);
        
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 0);
    }

    @Test
    public void testQueryJoinNoSchema(){
        String query = "c = join (load 'a') by $0, (load 'b') by $0;";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        
        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 0);
        
        //check that the foreach required fields contain [<0, 1>, <0, 2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);

    }

    @Test
    public void testQueryFilterWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = filter a by $1 == '3';");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check filter required fields
        LOFilter filter = (LOFilter)lp.getSuccessors(loada).get(0);
        List<RequiredFields> filterRequiredFields = filter.getRequiredFields();
        assertTrue(filterRequiredFields.size() == 1);
        
        requiredField = filterRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }
    
    
    @Test
    public void testQuerySplitWithSchema() {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        LogicalPlan lp = planTester.buildPlan("split a into b if url == '3', c if hitCount == '3';");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOSplit split = (LOSplit)lp.getSuccessors(loada).get(0);
        List<RequiredFields> splitRequiredFields = split.getRequiredFields();
        assertTrue(splitRequiredFields.size() == 1);
        
        requiredField = splitRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.getFields() == null);
        
        //check split outputs' required fields
        LOSplitOutput splitb = (LOSplitOutput)lp.getSuccessors(split).get(0);
        List<RequiredFields> splitbRequiredFields = splitb.getRequiredFields();
        assertTrue(splitbRequiredFields.size() == 1);
        
        requiredField = splitbRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        
        LOSplitOutput splitc = (LOSplitOutput)lp.getSuccessors(split).get(1);
        List<RequiredFields> splitcRequiredFields = splitc.getRequiredFields();
        assertTrue(splitcRequiredFields.size() == 1);
        
        requiredField = splitcRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }


    @Test
    public void testQueryOrderByWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = order a by $1;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check order by required fields
        LOSort sort = (LOSort)lp.getSuccessors(loada).get(0);
        List<RequiredFields> sortRequiredFields = sort.getRequiredFields();
        assertTrue(sortRequiredFields.size() == 1);
        
        requiredField = sortRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }

    @Test
    public void testQueryLimitWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        planTester.buildPlan("b = order a by $1;");
        LogicalPlan lp = planTester.buildPlan("c = limit b 10;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check order by required fields
        LOSort sort = (LOSort)lp.getSuccessors(loada).get(0);
        List<RequiredFields> sortRequiredFields = sort.getRequiredFields();
        assertTrue(sortRequiredFields.size() == 1);
        
        requiredField = sortRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);

        //check limit required fields
        LOLimit limit = (LOLimit)lp.getLeaves().get(0);
        List<RequiredFields> limitRequiredFields = limit.getRequiredFields();
        assertTrue(limitRequiredFields.size() == 1);
        
        requiredField = limitRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.getFields() == null);

    }
    
    @Test
    public void testQueryDistinctWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = distinct a;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check distinct required fields
        LODistinct distinct = (LODistinct)lp.getSuccessors(loada).get(0);
        List<RequiredFields> distinctRequiredFields = distinct.getRequiredFields();
        assertTrue(distinctRequiredFields.size() == 1);
        
        requiredField = distinctRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

    }

    @Test
    public void testQueryStreamingWithSchema() {
        String query = "stream (load 'a') through `" + simpleEchoStreamingCommand + "` as (x, y);";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check streaming required fields
        LOStream stream = (LOStream)lp.getSuccessors(loada).get(0);
        List<RequiredFields> streamRequiredFields = stream.getRequiredFields();
        assertTrue(streamRequiredFields.size() == 1);
        
        requiredField = streamRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

    }

    @Test
    public void testQueryStreamingWithSchema1() {
        String query = "stream (load 'a' as (url, hitCount)) through `" + simpleEchoStreamingCommand + "` as (x, y);";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check streaming required fields
        LOStream stream = (LOStream)lp.getSuccessors(loada).get(0);
        List<RequiredFields> streamRequiredFields = stream.getRequiredFields();
        assertTrue(streamRequiredFields.size() == 1);
        
        requiredField = streamRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
    }
    
    @Test
    public void testQueryImplicitJoinWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        planTester.buildPlan("b = load 'b' as (url,rank);");
        planTester.buildPlan("c = cogroup a by url, b by url;");
        LogicalPlan lp = planTester.buildPlan("d = foreach c generate group,flatten(a),flatten(b);");

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        
        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 0);
        
        //check that the foreach required fields contain [<0, 0>, <0, 1>, <0, 2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 3);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 1);
        assertTrue(fields.get(2).first == 0);
        assertTrue(fields.get(2).second == 2);

    }
    
    @Test
    public void testQueryCrossWithSchema(){
        String query = "c = cross (load 'a' as (url, hitcount)), (load 'b' as (url, rank));";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cross required fields
        LOCross cross = (LOCross)lp.getSuccessors(loada).get(0);
        List<RequiredFields> crossRequiredFields = cross.getRequiredFields();
        assertTrue(crossRequiredFields.size() == 2);
        
        requiredField = crossRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

        requiredField = crossRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
    }

    @Test
    public void testQueryUnionWithSchema(){
        String query = "c = union (load 'a' as (url, hitcount)), (load 'b' as (url, rank));";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check union required fields
        LOUnion union = (LOUnion)lp.getSuccessors(loada).get(0);
        List<RequiredFields> unionRequiredFields = union.getRequiredFields();
        assertTrue(unionRequiredFields.size() == 2);
        
        requiredField = unionRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

        requiredField = unionRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
        
    }

    @Test
    public void testQueryFRJoinWithSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b' as (url, rank)) by $0 using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check frjoin required fields
        LOFRJoin frjoin = (LOFRJoin)lp.getSuccessors(loada).get(0);
        List<RequiredFields> frjoinRequiredFields = frjoin.getRequiredFields();
        assertTrue(frjoinRequiredFields.size() == 2);
        
        requiredField = frjoinRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);


        requiredField = frjoinRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);
        
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 0);

    }

    @Test
    public void testQueryJoinWithSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b' as (url, rank)) by $0;";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        
        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 0);
        
        //check that the foreach required fields contain [<0, 1>, <0, 2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);

    }

    @Test
    public void testQueryCrossWithMixedSchema(){
        String query = "c = cross (load 'a' as (url, hitcount)), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cross required fields
        LOCross cross = (LOCross)lp.getSuccessors(loada).get(0);
        List<RequiredFields> crossRequiredFields = cross.getRequiredFields();
        assertTrue(crossRequiredFields.size() == 2);
        
        requiredField = crossRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

        requiredField = crossRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
    }

    @Test
    public void testQueryUnionWithMixedSchema(){
        String query = "c = union (load 'a' as (url, hitcount)), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check union required fields
        LOUnion union = (LOUnion)lp.getSuccessors(loada).get(0);
        List<RequiredFields> unionRequiredFields = union.getRequiredFields();
        assertTrue(unionRequiredFields.size() == 2);
        
        requiredField = unionRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

        requiredField = unionRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
        
    }

    @Test
    public void testQueryFRJoinWithMixedSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b') by $0 using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check frjoin required fields
        LOFRJoin frjoin = (LOFRJoin)lp.getSuccessors(loada).get(0);
        List<RequiredFields> frjoinRequiredFields = frjoin.getRequiredFields();
        assertTrue(frjoinRequiredFields.size() == 2);
        
        requiredField = frjoinRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);


        requiredField = frjoinRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);
        
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 0);
        
    }
    
    @Test
    public void testQueryJoinWithMixedSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b') by $0;";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        
        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 0);
        
        //check that the foreach required fields contain [<0, 1>, <0, 2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);
    }

    @Test
    public void testQueryFilterWithStarNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = filter a by COUNT(*) == 3;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check filter required fields
        LOFilter filter = (LOFilter)lp.getSuccessors(loada).get(0);
        List<RequiredFields> filterRequiredFields = filter.getRequiredFields();
        assertTrue(filterRequiredFields.size() == 1);
        
        requiredField = filterRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.getFields() == null);
        
    }

    @Test
    public void testQueryOrderByStarNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = order a by *;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check order by required fields
        LOSort sort = (LOSort)lp.getSuccessors(loada).get(0);
        List<RequiredFields> sortRequiredFields = sort.getRequiredFields();
        assertTrue(sortRequiredFields.size() == 1);
        
        requiredField = sortRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.getFields() == null);
        
    }
    
    @Test
    public void testQueryGroupByStarNoSchema() throws Exception {
        String query = "foreach (group (load 'a') by *) generate $1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 1);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }

    @Test
    public void testQueryFRJoinOnStarNoSchema(){
        String query = "c = join (load 'a') by *, (load 'b') by * using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check frjoin required fields
        LOFRJoin frjoin = (LOFRJoin)lp.getSuccessors(loada).get(0);
        List<RequiredFields> frjoinRequiredFields = frjoin.getRequiredFields();
        assertTrue(frjoinRequiredFields.size() == 2);
        
        requiredField = frjoinRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

        requiredField = frjoinRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
        
    }
    
    @Test
    public void testQueryJoinOnStarNoSchema(){
        String query = "c = join (load 'a') by *, (load 'b') by *;";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

        //check that the foreach required fields contain [<0, 1>, <0, 2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);
    }

    @Test
    public void testQueryFilterStarWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = filter a by COUNT(*) == 3;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check filter required fields
        LOFilter filter = (LOFilter)lp.getSuccessors(loada).get(0);
        List<RequiredFields> filterRequiredFields = filter.getRequiredFields();
        assertTrue(filterRequiredFields.size() == 1);
        
        requiredField = filterRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.getFields() == null);
        
    }
    
    @Test
    public void testQuerySplitWithStarSchema() {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        LogicalPlan lp = planTester.buildPlan("split a into b if url == '3', c if COUNT(*) == '3';");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOSplit split = (LOSplit)lp.getSuccessors(loada).get(0);
        List<RequiredFields> splitRequiredFields = split.getRequiredFields();
        assertTrue(splitRequiredFields.size() == 1);
        
        requiredField = splitRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.getFields() == null);
        
        //check split outputs' required fields
        LOSplitOutput splitb = (LOSplitOutput)lp.getSuccessors(split).get(0);
        List<RequiredFields> splitbRequiredFields = splitb.getRequiredFields();
        assertTrue(splitbRequiredFields.size() == 1);
        
        requiredField = splitbRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        
        LOSplitOutput splitc = (LOSplitOutput)lp.getSuccessors(split).get(1);
        List<RequiredFields> splitcRequiredFields = splitc.getRequiredFields();
        assertTrue(splitcRequiredFields.size() == 1);
        
        requiredField = splitcRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.getFields() == null);
        
    }
    
    @Test
    public void testQueryOrderByStarWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = order a by *;");
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check order by required fields
        LOSort sort = (LOSort)lp.getSuccessors(loada).get(0);
        List<RequiredFields> sortRequiredFields = sort.getRequiredFields();
        assertTrue(sortRequiredFields.size() == 1);
        
        requiredField = sortRequiredFields.get(0);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.needNoFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields();
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 1);
    }
    
    @Test
    public void testQueryGroupByStarWithSchema() throws Exception {
        String query = "foreach (group (load 'a' as (url, hitCount)) by *) generate $1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        
        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 1);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 1);
        
        //check that the foreach required fields contain [<0, 1>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 1);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
    }

    @Test
    public void testQueryFRJoinOnStarWithSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by *, (load 'b' as (url, rank)) by * using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check frjoin required fields
        LOFRJoin frjoin = (LOFRJoin)lp.getSuccessors(loada).get(0);
        List<RequiredFields> frjoinRequiredFields = frjoin.getRequiredFields();
        assertTrue(frjoinRequiredFields.size() == 2);
        
        requiredField = frjoinRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 1);

        requiredField = frjoinRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);
        
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 0);
        assertTrue(fields.get(1).first == 1);
        assertTrue(fields.get(1).second == 1);

    }

    @Test
    public void testQueryJoinOnStarWithSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by *, (load 'b' as (url, rank)) by *;";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        List<RequiredFields> loadbRequiredFields = loadb.getRequiredFields();
        assertTrue(loadbRequiredFields.size() == 1);
        
        requiredField = loadbRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        List<RequiredFields> cogroupRequiredFields = cogroup.getRequiredFields();
        assertTrue(cogroupRequiredFields.size() == 2);
        
        requiredField = cogroupRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 1);
        
        requiredField = cogroupRequiredFields.get(1);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 1);
        assertTrue(fields.get(0).second == 0);
        assertTrue(fields.get(1).first == 1);
        assertTrue(fields.get(1).second == 1);
        
        //check that the foreach required fields contain [<0, 1>, <0, 2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 1);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 2);

    }
    
    @Test
    public void testQueryForeachGenerateStarNoSchema() {
        String query = "foreach (load 'a') generate * ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>, <0, 3>, <0,2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

    }

    @Test
    public void testQueryForeachGenerateCountStarNoSchema() {
        String query = "foreach (load 'a') generate COUNT(*) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>, <0, 3>, <0,2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);

    }
    
    @Test
    public void testQueryForeachGenerateStarNoSchema1() {
        String query = "foreach (load 'a') generate *, COUNT(*) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>, <0, 3>, <0,2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
    }

    @Test
    public void testQueryForeachGenerateStarNoSchema2() {
        String query = "foreach (load 'a') generate *, $0 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>, <0, 3>, <0,2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
    }
    
    @Test
    public void testQueryForeachGenerateStarWithSchema() {
        String query = "foreach (load 'a' as (url, hitCount)) generate * ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 1>, <0, 3>, <0,2>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);

        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 1);

    }

    @Test
    public void testQueryForeachGenerateCountStarWithSchema() {
        String query = "foreach (load 'a' as (url, hitCount)) generate COUNT(*) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check the foreach required fields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);
        
        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
    }

    @Test
    public void testQueryForeachGenerateStarWithSchema1() {
        String query = "foreach (load 'a' as (url, hitCount)) generate *, COUNT(*) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check the foreach required fields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == true);
        assertTrue(requiredField.getFields() == null);
    }

    @Test
    public void testQueryForeachGenerateStarWithSchema2() {
        String query = "foreach (load 'a' as (url, hitCount)) generate *, url ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load's required fields is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        List<RequiredFields> loadaRequiredFields = loada.getRequiredFields();
        assertTrue(loadaRequiredFields.size() == 1);
        
        RequiredFields requiredField = loadaRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == true);
        assertTrue(requiredField.needAllFields() == false);
        assertTrue(requiredField.getFields() == null);
        
        //check that the foreach required fields contain [<0, 0>, <0, 1>]
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        List<RequiredFields> foreachRequiredFields = foreach.getRequiredFields();
        assertTrue(foreachRequiredFields.size() == 1);

        requiredField = foreachRequiredFields.get(0);
        assertTrue(requiredField.needNoFields() == false);
        assertTrue(requiredField.needAllFields() == false);
        
        List<Pair<Integer, Integer>> fields = requiredField.getFields(); 
        assertTrue(fields.size() == 2);
        assertTrue(fields.get(0).first == 0);
        assertTrue(fields.get(0).second == 0);
        assertTrue(fields.get(1).first == 0);
        assertTrue(fields.get(1).second == 1);
    }

}
