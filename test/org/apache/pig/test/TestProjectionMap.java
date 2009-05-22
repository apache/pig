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


import org.junit.After;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.test.utils.LogicalPlanTester;


public class TestProjectionMap extends junit.framework.TestCase {

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
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the foreach projection map has null mappedFields
        //and null removed fields since the input schema is null
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        assertTrue(foreachProjectionMap.getMappedFields() == null);
        assertTrue(foreachProjectionMap.getRemovedFields() == null);
        
        //check that added fields contain [0, 1]
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields.size() == 2);
        assertTrue(foreachAddedFields.get(0) == 0);
        assertTrue(foreachAddedFields.get(1) == 1);
    }

    @Test
    public void testQueryForeach2() {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "(':')) generate $1, 'aoeuaoeu' ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the foreach projection map has null mappedFields
        //and null removed fields since the input schema is null
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        assertTrue(foreachProjectionMap.getMappedFields() == null);
        assertTrue(foreachProjectionMap.getRemovedFields() == null);
        
        //check that added fields contain [0, 1]
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields.size() == 2);
        assertTrue(foreachAddedFields.get(0) == 0);
        assertTrue(foreachAddedFields.get(1) == 1);
    }

    @Test
    public void testQueryCogroup1() {
        String query = "foreach (cogroup (load 'a') by $1, (load 'b') by $1) generate org.apache.pig.builtin.AVG($1) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);

        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> mapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);
        assertTrue(mapValues.get(1).first == 1);
        assertTrue(mapValues.get(1).second == 1);
        
        //check the cogroup removed fields is null
        assertTrue(cogroupProjectionMap.getRemovedFields() == null);
        
        //check that cogroup added fields contain [1, 2]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 2);
        assertTrue(cogroupAddedFields.get(0) == 1);
        assertTrue(cogroupAddedFields.get(1) == 2);
        
        //check that the foreach projection map has null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        assertTrue(foreachProjectionMap.getMappedFields() == null);

        //check that removed fields has all the columns from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachProjectionMap.getRemovedFields().size() == 3);
        int expectedColumn = 0;
        for(Pair<Integer, Integer> removedField: foreachRemovedFields) {
            assertTrue(removedField.first == 0);
            assertTrue(removedField.second == expectedColumn++);
        }
        
        //check that added fields contain [0]
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields.size() == 1);
        assertTrue(foreachAddedFields.get(0) == 0);
    }

    @Test
    public void testQueryGroupAll() throws Exception {
        String query = "foreach (group (load 'a') ALL) generate $1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields == null);
        
        //check the cogroup removed fields is null
        assertTrue(cogroupProjectionMap.getRemovedFields() == null);
        
        //check that cogroup added fields contain [0, 1]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 2);
        assertTrue(cogroupAddedFields.get(0) == 0);
        assertTrue(cogroupAddedFields.get(1) == 1);
        
        //check that the foreach projection map has non-null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        MultiMap<Integer, Pair<Integer, Integer>> foreachMappedFields = foreachProjectionMap.getMappedFields();
        assertTrue(foreachMappedFields != null);
        
        List<Pair<Integer, Integer>> mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMappedFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);


        //check that removed fields has all the columns from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachRemovedFields.size() == 1);

        Pair<Integer, Integer> removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 0);
        
        //check that added fields is null
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields == null);
    }

    @Test
    public void testQueryGroup2() {
        String query = "foreach (group (load 'a') by $1) generate group, '1' ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> mapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);

        //check the cogroup removed fields is null
        assertTrue(cogroupProjectionMap.getRemovedFields() == null);
        
        //check that cogroup added fields contain [0, 1]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 1);
        assertTrue(cogroupAddedFields.get(0) == 1);
        
        //check that the foreach projection map has non-null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);        
        
        MultiMap<Integer, Pair<Integer, Integer>> foreachMapFields = foreachProjectionMap.getMappedFields(); 
        assertTrue(foreachMapFields != null);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 0);


        //check that removed fields has all the columns from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachRemovedFields.size() == 1);

        Pair<Integer, Integer> removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 1);
        
        //check that added fields contain [1]
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields != null);
        assertTrue(foreachAddedFields.size() == 1);
        assertTrue(foreachAddedFields.get(0) == 1);
    }

    @Test
    public void testQueryCogroup2() {
        String query = "foreach (cogroup (load 'a') by ($1), (load 'b') by ($1)) generate $1.$1, $2.$1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);

        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> mapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);
        assertTrue(mapValues.get(1).first == 1);
        assertTrue(mapValues.get(1).second == 1);
        
        //check the cogroup removed fields is null
        assertTrue(cogroupProjectionMap.getRemovedFields() == null);
        
        //check that cogroup added fields contain [1, 2]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 2);
        assertTrue(cogroupAddedFields.get(0) == 1);
        assertTrue(cogroupAddedFields.get(1) == 2);
        
        //check that the foreach projection map has null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> foreachMapFields = foreachProjectionMap.getMappedFields(); 
        assertTrue(foreachMapFields != null);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(1);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 2);

        //check that removed fields has all the group column from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachRemovedFields.size() == 1);
        Pair<Integer, Integer> removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 0);
        
        //check that added fields is null
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields == null);
    }

    @Test
    public void testQueryGroup3() {
        String query = "foreach (group (load 'a') by ($6, $7)) generate flatten(group) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);

        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> mapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 6);
        assertTrue(mapValues.get(1).first == 0);
        assertTrue(mapValues.get(1).second == 7);
        
        //check the cogroup removed fields is null
        assertTrue(cogroupProjectionMap.getRemovedFields() == null);
        
        //check that cogroup added fields contain [1, 2]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 1);
        assertTrue(cogroupAddedFields.get(0) == 1);
        
        //check that the foreach projection map has null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> foreachMapFields = foreachProjectionMap.getMappedFields(); 
        assertTrue(foreachMapFields != null);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 0);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(1);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 0);

        //check that removed fields has all the group column from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachRemovedFields.size() == 1);
        Pair<Integer, Integer> removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 1);
        
        //check that added fields is null
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields == null);

    }

    @Test
    public void testQueryFilterNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = filter a by $1 == '3';");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the filter projection map has null mappedFields
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        ProjectionMap filterProjectionMap = filter.getProjectionMap();
        assertTrue(filterProjectionMap == null);
    }
    
    @Test
    public void testQuerySplitNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("split a into b if $0 == '3', c if $1 == '3';");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the split projection map has null mappedFields
        LOSplit split = (LOSplit)lp.getSuccessors(load).get(0);
        ProjectionMap splitProjectionMap = split.getProjectionMap();
        assertTrue(splitProjectionMap == null);
        
        LOSplitOutput splitb = (LOSplitOutput)lp.getSuccessors(split).get(0);
        ProjectionMap splitbProjectionMap = splitb.getProjectionMap();
        assertTrue(splitbProjectionMap == null);
        
        LOSplitOutput splitc = (LOSplitOutput)lp.getSuccessors(split).get(1);
        ProjectionMap splitcProjectionMap = splitc.getProjectionMap();
        assertTrue(splitcProjectionMap == null);
    }
    
    @Test
    public void testQueryOrderByNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = order a by $1;");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the order by projection map has null mappedFields
        LOSort sort = (LOSort)lp.getLeaves().get(0);
        ProjectionMap sortProjectionMap = sort.getProjectionMap();
        assertTrue(sortProjectionMap == null);
    }
    
    @Test
    public void testQueryLimitNoSchema() {
        planTester.buildPlan("a = load 'a';");
        planTester.buildPlan("b = order a by $1;");
        LogicalPlan lp = planTester.buildPlan("c = limit b 10;");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the order by projection map is not null
        LOSort sort = (LOSort)lp.getSuccessors(load).get(0);
        ProjectionMap sortProjectionMap = sort.getProjectionMap();
        assertTrue(sortProjectionMap == null);
        
        //check that the limit projection map is null
        LOLimit limit = (LOLimit)lp.getLeaves().get(0);
        ProjectionMap limitProjectionMap = limit.getProjectionMap();
        assertTrue(limitProjectionMap == null);
    }
    
    @Test
    public void testQueryDistinctNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = distinct a;");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the distinct projection map has null mappedFields
        LODistinct distinct = (LODistinct)lp.getLeaves().get(0);
        ProjectionMap distinctProjectionMap = distinct.getProjectionMap();
        assertTrue(distinctProjectionMap == null);
    }
    
    @Test
    public void testQueryStreamingNoSchema() {
        String query = "stream (load 'a') through `" + simpleEchoStreamingCommand + "`;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the stream projection map is null
        LOStream stream = (LOStream)lp.getLeaves().get(0);
        ProjectionMap streamProjectionMap = stream.getProjectionMap();
        assertTrue(streamProjectionMap == null);
    }
    
    @Test
    public void testQueryStreamingNoSchema1() {
        String query = "stream (load 'a' as (url, hitCount)) through `" + simpleEchoStreamingCommand + "` ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the stream projection map is null
        LOStream stream = (LOStream)lp.getLeaves().get(0);
        ProjectionMap streamProjectionMap = stream.getProjectionMap();
        assertTrue(streamProjectionMap == null);
    }
    
    @Test
    public void testQueryForeach3() {
        String query = "foreach (load 'a') generate ($1 == '3'? $2 : $3) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the foreach projection map has null mappedFields
        //and null removed fields since the input schema is null
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        assertTrue(foreachProjectionMap.getMappedFields() == null);
        assertTrue(foreachProjectionMap.getRemovedFields() == null);
        
        //check that added fields contain [0]
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields.size() == 1);
        assertTrue(foreachAddedFields.get(0) == 0);
    }
    
    @Test
    public void testQueryForeach4() {
        planTester.buildPlan("A = load 'a';");
        planTester.buildPlan("B = load 'b';");
        LogicalPlan lp = planTester.buildPlan("foreach (cogroup A by ($1), B by ($1)) generate A, flatten(B.($1, $2, $3));");
        
        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);

        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> cogroupMapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(cogroupMapValues.get(0).first == 0);
        assertTrue(cogroupMapValues.get(0).second == 1);
        assertTrue(cogroupMapValues.get(1).first == 1);
        assertTrue(cogroupMapValues.get(1).second == 1);
        
        //check the cogroup removed fields is null
        assertTrue(cogroupProjectionMap.getRemovedFields() == null);
        
        //check that cogroup added fields contain [1, 2]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 2);
        assertTrue(cogroupAddedFields.get(0) == 1);
        assertTrue(cogroupAddedFields.get(1) == 2);
        
        //check that the foreach projection map has non-null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> foreachMapFields = foreachProjectionMap.getMappedFields();
        assertTrue(foreachMapFields != null);
        
        List<Pair<Integer, Integer>> foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 1);
        
        for(int i = 1; i < 4; ++i) {
            foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(i);
            assertTrue(foreachMapValues.get(0).first == 0);
            assertTrue(foreachMapValues.get(0).second == 2);
        }
        
        //check that removed fields has all the group column from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachProjectionMap.getRemovedFields().size() == 1);
        Pair<Integer, Integer> removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 0);
        
        //check that added fields is null
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields == null);
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

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);

        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> cogroupMapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(cogroupMapValues.get(0).first == 0);
        assertTrue(cogroupMapValues.get(0).second == 1);
        assertTrue(cogroupMapValues.get(1).first == 1);
        assertTrue(cogroupMapValues.get(1).second == 1);
        
        //check the cogroup removed fields is null
        assertTrue(cogroupProjectionMap.getRemovedFields() == null);
        
        //check that cogroup added fields contain [1, 2]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 2);
        assertTrue(cogroupAddedFields.get(0) == 1);
        assertTrue(cogroupAddedFields.get(1) == 2);
        
        //check that the foreach projection map has null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> foreachMapFields = foreachProjectionMap.getMappedFields();
        assertTrue(foreachMapFields != null);
        
        List<Pair<Integer, Integer>> foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 1);
        
        //check that removed fields has all the columns from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachProjectionMap.getRemovedFields().size() == 2);
        Pair<Integer, Integer> removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 0);
        
        removedField = foreachRemovedFields.get(1);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 2);

        //check that added fields contain [0]
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields.size() == 1);
        assertTrue(foreachAddedFields.get(0) == 1);
    }
    
    
    @Test
    public void testQueryCrossNoSchema(){
        String query = "c = cross (load 'a'), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check cross projection map
        LOCross cross = (LOCross)lp.getSuccessors(loada).get(0);
        ProjectionMap crossProjectionMap = cross.getProjectionMap();
        assertTrue(crossProjectionMap == null);
        
    }
    
    @Test
    public void testQueryUnionNoSchema(){
        String query = "c = union (load 'a'), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check union projection map
        LOUnion union = (LOUnion)lp.getSuccessors(loada).get(0);
        ProjectionMap unionProjectionMap = union.getProjectionMap();
        assertTrue(unionProjectionMap == null);
        
    }
    
    @Test
    public void testQueryFRJoinNoSchema(){
        String query = "c = join (load 'a') by $0, (load 'b') by $0 using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check cross projection map
        LOFRJoin frjoin = (LOFRJoin)lp.getSuccessors(loada).get(0);
        ProjectionMap frjoinProjectionMap = frjoin.getProjectionMap();
        assertTrue(frjoinProjectionMap == null);
        
    }

    @Test
    public void testQueryJoinNoSchema(){
        String query = "c = join (load 'a') by $0, (load 'b') by $0;";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> mapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 0);
        assertTrue(mapValues.get(1).first == 1);
        assertTrue(mapValues.get(1).second == 0);
        
        //check the cogroup removed fields is null
        assertTrue(cogroupProjectionMap.getRemovedFields() == null);
        
        //check that cogroup added fields contain [1, 2]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 2);
        assertTrue(cogroupAddedFields.get(0) == 1);
        assertTrue(cogroupAddedFields.get(1) == 2);
        
        //check that the foreach projection map has non-null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> foreachMapFields = foreachProjectionMap.getMappedFields(); 
        assertTrue(foreachMapFields != null);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(1);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 2);

        //check that removed fields has all the group column from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachRemovedFields.size() == 1);
        Pair<Integer, Integer> removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 0);
        
        //check that added fields is null
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields == null);
    }
    
    @Test
    public void testQueryFilterWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = filter a by $1 == '3';");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the filter projection map is not null
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        ProjectionMap filterProjectionMap = filter.getProjectionMap();
        assertTrue(filterProjectionMap != null);
        assertTrue(filterProjectionMap.changes() == false);
    }
    
    @Test
    public void testQuerySplitWithSchema() {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        LogicalPlan lp = planTester.buildPlan("split a into b if url == '3', c if hitCount == '3';");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the split projection map has null mappedFields
        LOSplit split = (LOSplit)lp.getSuccessors(load).get(0);
        ProjectionMap splitProjectionMap = split.getProjectionMap();
        assertTrue(splitProjectionMap.changes() == false);
        
        LOSplitOutput splitb = (LOSplitOutput)lp.getSuccessors(split).get(0);
        ProjectionMap splitbProjectionMap = splitb.getProjectionMap();
        assertTrue(splitbProjectionMap.changes() == false);
        
        LOSplitOutput splitc = (LOSplitOutput)lp.getSuccessors(split).get(1);
        ProjectionMap splitcProjectionMap = splitc.getProjectionMap();
        assertTrue(splitcProjectionMap.changes() == false);
    }
    
    @Test
    public void testQueryOrderByWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = order a by $1;");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the order by projection map is not null
        LOSort sort = (LOSort)lp.getLeaves().get(0);
        ProjectionMap sortProjectionMap = sort.getProjectionMap();
        assertTrue(sortProjectionMap != null);
        assertTrue(sortProjectionMap.changes() == false);
    }
    
    @Test
    public void testQueryLimitWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        planTester.buildPlan("b = order a by $1;");
        LogicalPlan lp = planTester.buildPlan("c = limit b 10;");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the order by projection map is not null
        LOSort sort = (LOSort)lp.getSuccessors(load).get(0);
        ProjectionMap sortProjectionMap = sort.getProjectionMap();
        assertTrue(sortProjectionMap != null);
        assertTrue(sortProjectionMap.changes() == false);
        
        //check that the limit projection map is not null
        LOLimit limit = (LOLimit)lp.getLeaves().get(0);
        ProjectionMap limitProjectionMap = limit.getProjectionMap();
        assertTrue(limitProjectionMap != null);
        assertTrue(limitProjectionMap.changes() == false);
    }
    
    @Test
    public void testQueryDistinctWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = distinct a;");
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the distinct projection map is not null
        LODistinct distinct = (LODistinct)lp.getLeaves().get(0);
        ProjectionMap distinctProjectionMap = distinct.getProjectionMap();
        assertTrue(distinctProjectionMap != null);
        assertTrue(distinctProjectionMap.changes() == false);
    }

    @Test
    public void testQueryStreamingWithSchema() {
        String query = "stream (load 'a') through `" + simpleEchoStreamingCommand + "` as (x, y);";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the stream projection map is null
        LOStream stream = (LOStream)lp.getLeaves().get(0);
        ProjectionMap streamProjectionMap = stream.getProjectionMap();
        assertTrue(streamProjectionMap.changes() == true);
        assertTrue(streamProjectionMap.getMappedFields() == null);
        assertTrue(streamProjectionMap.getRemovedFields() == null);
        
        List<Integer> streamAddedFields = streamProjectionMap.getAddedFields();
        assertTrue(streamAddedFields.size() == 2);
        assertTrue(streamAddedFields.get(0) == 0);
        assertTrue(streamAddedFields.get(1) == 1);
    }

    @Test
    public void testQueryStreamingWithSchema1() {
        String query = "stream (load 'a' as (url, hitCount)) through `" + simpleEchoStreamingCommand + "` as (x, y);";
        LogicalPlan lp = planTester.buildPlan(query);
        
        //check that the load projection map is null
        LOLoad load = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadProjectionMap = load.getProjectionMap();
        assertTrue(loadProjectionMap == null);
        
        //check that the stream projection map is null
        LOStream stream = (LOStream)lp.getLeaves().get(0);
        ProjectionMap streamProjectionMap = stream.getProjectionMap();
        assertTrue(streamProjectionMap.changes() == true);
        assertTrue(streamProjectionMap.getMappedFields() == null);
        
        //check that removed fields has all the columns from the input load
        List<Pair<Integer, Integer>> streamRemovedFields = streamProjectionMap.getRemovedFields();
        assertTrue(streamRemovedFields.size() == 2);
        Pair<Integer, Integer> removedField = streamRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 0);
        
        removedField = streamRemovedFields.get(1);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 1);
        
        List<Integer> streamAddedFields = streamProjectionMap.getAddedFields();
        assertTrue(streamAddedFields.size() == 2);
        assertTrue(streamAddedFields.get(0) == 0);
        assertTrue(streamAddedFields.get(1) == 1);
    }
    
    @Test
    public void testQueryImplicitJoinWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        planTester.buildPlan("b = load 'b' as (url,rank);");
        planTester.buildPlan("c = cogroup a by url, b by url;");
        LogicalPlan lp = planTester.buildPlan("d = foreach c generate group,flatten(a),flatten(b);");

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);

        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> cogroupMapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(cogroupMapValues.get(0).first == 0);
        assertTrue(cogroupMapValues.get(0).second == 0);
        assertTrue(cogroupMapValues.get(1).first == 1);
        assertTrue(cogroupMapValues.get(1).second == 0);
        
        //check that removed fields has hitCount from a and rank from b
        List<Pair<Integer, Integer>> cogroupRemovedFields = cogroupProjectionMap.getRemovedFields();
        assertTrue(cogroupRemovedFields.size() == 2);
        Pair<Integer, Integer> removedField = cogroupRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 1);
        
        removedField = cogroupRemovedFields.get(1);
        assertTrue(removedField.first == 1);
        assertTrue(removedField.second == 1);

        
        //check that cogroup added fields contain [1, 2]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 2);
        assertTrue(cogroupAddedFields.get(0) == 1);
        assertTrue(cogroupAddedFields.get(1) == 2);
        
        //check that the foreach projection map has null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> foreachMapFields = foreachProjectionMap.getMappedFields();
        assertTrue(foreachMapFields != null);
        
        List<Pair<Integer, Integer>> foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 0);
        
        foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(1);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 1);
        
        foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(2);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 1);
        
        foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(3);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 2);
        
        foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(4);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 2);
        
        //check that removed fields is null
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachRemovedFields == null);
        
        //check that added fields is null
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields == null);
        
        lp = planTester.buildPlan("e = foreach d generate group, a::url, b::url, b::rank, rank;");
        
        foreach = (LOForEach)lp.getLeaves().get(0);
        foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        
        foreachMapFields = foreachProjectionMap.getMappedFields();
        assertTrue(foreachMapFields != null);
        
        foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 0);
        
        foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(1);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 1);
        
        foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(2);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 3);
        
        foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(3);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 4);
        
        foreachMapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(4);
        assertTrue(foreachMapValues.get(0).first == 0);
        assertTrue(foreachMapValues.get(0).second == 4);
        
        //check that removed fields is null
        foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachRemovedFields != null);
        assertTrue(foreachRemovedFields.size() == 1);
        
        removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 2);
        
        //check that added fields is null
        foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields == null);
    }
    
    @Test
    public void testQueryCrossWithSchema(){
        String query = "c = cross (load 'a' as (url, hitcount)), (load 'b' as (url, rank));";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check cross projection map
        LOCross cross = (LOCross)lp.getSuccessors(loada).get(0);
        ProjectionMap crossProjectionMap = cross.getProjectionMap();
        assertTrue(crossProjectionMap != null);
        
        MultiMap<Integer, Pair<Integer, Integer>> crossMapFields = crossProjectionMap.getMappedFields();
        assertTrue(crossMapFields != null);
        
        List<Pair<Integer, Integer>> crossMapValues = (ArrayList<Pair<Integer, Integer>>)crossMapFields.get(0);
        assertTrue(crossMapValues.get(0).first == 0);
        assertTrue(crossMapValues.get(0).second == 0);
        
        crossMapValues = (ArrayList<Pair<Integer, Integer>>)crossMapFields.get(1);
        assertTrue(crossMapValues.get(0).first == 0);
        assertTrue(crossMapValues.get(0).second == 1);
        
        crossMapValues = (ArrayList<Pair<Integer, Integer>>)crossMapFields.get(2);
        assertTrue(crossMapValues.get(0).first == 1);
        assertTrue(crossMapValues.get(0).second == 0);
        
        crossMapValues = (ArrayList<Pair<Integer, Integer>>)crossMapFields.get(3);
        assertTrue(crossMapValues.get(0).first == 1);
        assertTrue(crossMapValues.get(0).second == 1);
        
        //check that removed fields is null
        List<Pair<Integer, Integer>> crossRemovedFields = crossProjectionMap.getRemovedFields();
        assertTrue(crossRemovedFields == null);
        
        //check that added fields is null
        List<Integer> crossAddedFields = crossProjectionMap.getAddedFields();
        assertTrue(crossAddedFields == null);
    }
    
    @Test
    public void testQueryUnionWithSchema(){
        String query = "c = union (load 'a' as (url, hitcount)), (load 'b' as (url, rank));";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check union projection map
        LOUnion union = (LOUnion)lp.getSuccessors(loada).get(0);
        ProjectionMap unionProjectionMap = union.getProjectionMap();
        assertTrue(unionProjectionMap != null);
        
        MultiMap<Integer, Pair<Integer, Integer>> unionMapFields = unionProjectionMap.getMappedFields();
        assertTrue(unionMapFields != null);

        List<Pair<Integer, Integer>> unionMapValues = (ArrayList<Pair<Integer, Integer>>)unionMapFields.get(0);
        assertTrue(unionMapValues.get(0).first == 0);
        assertTrue(unionMapValues.get(0).second == 0);
        
        assertTrue(unionMapValues.get(1).first == 1);
        assertTrue(unionMapValues.get(1).second == 0);
        
        unionMapValues = (ArrayList<Pair<Integer, Integer>>)unionMapFields.get(1);
        assertTrue(unionMapValues.get(0).first == 0);
        assertTrue(unionMapValues.get(0).second == 1);
        
        assertTrue(unionMapValues.get(1).first == 1);
        assertTrue(unionMapValues.get(1).second == 1);
        
        //check that removed fields is null
        List<Pair<Integer, Integer>> unionRemovedFields = unionProjectionMap.getRemovedFields();
        assertTrue(unionRemovedFields == null);
        
        //check that added fields is null
        List<Integer> unionAddedFields = unionProjectionMap.getAddedFields();
        assertTrue(unionAddedFields == null);

    }
    
    @Test
    public void testQueryFRJoinWithSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b' as (url, rank)) by $0 using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check cross projection map
        LOFRJoin frjoin = (LOFRJoin)lp.getSuccessors(loada).get(0);
        ProjectionMap frjoinProjectionMap = frjoin.getProjectionMap();
        assertTrue(frjoinProjectionMap != null);
        
        MultiMap<Integer, Pair<Integer, Integer>> frjoinMapFields = frjoinProjectionMap.getMappedFields();
        assertTrue(frjoinMapFields != null);

        List<Pair<Integer, Integer>> frjoinMapValues = (ArrayList<Pair<Integer, Integer>>)frjoinMapFields.get(0);
        assertTrue(frjoinMapValues.get(0).first == 0);
        assertTrue(frjoinMapValues.get(0).second == 0);
        
        frjoinMapValues = (ArrayList<Pair<Integer, Integer>>)frjoinMapFields.get(1);
        assertTrue(frjoinMapValues.get(0).first == 0);
        assertTrue(frjoinMapValues.get(0).second == 1);
        
        frjoinMapValues = (ArrayList<Pair<Integer, Integer>>)frjoinMapFields.get(2);
        assertTrue(frjoinMapValues.get(0).first == 1);
        assertTrue(frjoinMapValues.get(0).second == 0);
        
        frjoinMapValues = (ArrayList<Pair<Integer, Integer>>)frjoinMapFields.get(3);
        assertTrue(frjoinMapValues.get(0).first == 1);
        assertTrue(frjoinMapValues.get(0).second == 1);
        
        //check that removed fields is null
        List<Pair<Integer, Integer>> frjoinRemovedFields = frjoinProjectionMap.getRemovedFields();
        assertTrue(frjoinRemovedFields == null);
        
        //check that added fields is null
        List<Integer> frjoinAddedFields = frjoinProjectionMap.getAddedFields();
        assertTrue(frjoinAddedFields == null);

    }

    @Test
    public void testQueryJoinWithSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b' as (url, rank)) by $0;";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> mapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 0);
        assertTrue(mapValues.get(1).first == 1);
        assertTrue(mapValues.get(1).second == 0);
        
        //check that removed fields is not null
        List<Pair<Integer, Integer>> cogroupRemovedFields = cogroupProjectionMap.getRemovedFields();
        assertTrue(cogroupRemovedFields != null);
        
        Pair<Integer, Integer> removedFields = cogroupRemovedFields.get(0);
        assertTrue(removedFields.first == 0);
        assertTrue(removedFields.second == 1);

        removedFields = cogroupRemovedFields.get(1);
        assertTrue(removedFields.first == 1);
        assertTrue(removedFields.second == 1);

        //check that cogroup added fields contain [1, 2]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 2);
        assertTrue(cogroupAddedFields.get(0) == 1);
        assertTrue(cogroupAddedFields.get(1) == 2);
        
        //check that the foreach projection map has non-null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> foreachMapFields = foreachProjectionMap.getMappedFields(); 
        assertTrue(foreachMapFields != null);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(1);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);

        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(2);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 2);

        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(3);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 2);

        
        //check that removed fields has all the group column from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachRemovedFields.size() == 1);
        Pair<Integer, Integer> removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 0);
        
        //check that added fields is null
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields == null);
    }

    @Test
    public void testQueryCrossWithMixedSchema(){
        String query = "c = cross (load 'a' as (url, hitcount)), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check cross projection map
        LOCross cross = (LOCross)lp.getSuccessors(loada).get(0);
        ProjectionMap crossProjectionMap = cross.getProjectionMap();
        assertTrue(crossProjectionMap == null);
    }
    
    @Test
    public void testQueryUnionWithMixedSchema(){
        String query = "c = union (load 'a' as (url, hitcount)), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check union projection map
        LOUnion union = (LOUnion)lp.getSuccessors(loada).get(0);
        ProjectionMap unionProjectionMap = union.getProjectionMap();
        assertTrue(unionProjectionMap == null);
    }
    
    @Test
    public void testQueryFRJoinWithMixedSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b') by $0 using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check cross projection map
        LOFRJoin frjoin = (LOFRJoin)lp.getSuccessors(loada).get(0);
        ProjectionMap frjoinProjectionMap = frjoin.getProjectionMap();
        assertTrue(frjoinProjectionMap == null);
        
    }
    
    @Test
    public void testQueryJoinWithMixedSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b') by $0;";
        LogicalPlan lp = planTester.buildPlan(query);

        //check that the loads' projection map is null
        LOLoad loada = (LOLoad) lp.getRoots().get(0);
        ProjectionMap loadaProjectionMap = loada.getProjectionMap();
        assertTrue(loadaProjectionMap == null);
        
        LOLoad loadb = (LOLoad) lp.getRoots().get(1);
        ProjectionMap loadbProjectionMap = loadb.getProjectionMap();
        assertTrue(loadbProjectionMap == null);
        
        //check cogroup projection map
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(loada).get(0);
        ProjectionMap cogroupProjectionMap = cogroup.getProjectionMap();
        assertTrue(cogroupProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> cogroupMapFields = cogroupProjectionMap.getMappedFields(); 
        assertTrue(cogroupMapFields != null);
        
        List<Pair<Integer, Integer>> mapValues = (ArrayList<Pair<Integer, Integer>>)cogroupMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 0);
        assertTrue(mapValues.get(1).first == 1);
        assertTrue(mapValues.get(1).second == 0);
        
        //check that removed fields is not null
        List<Pair<Integer, Integer>> cogroupRemovedFields = cogroupProjectionMap.getRemovedFields();
        assertTrue(cogroupRemovedFields.size() == 1);
        
        Pair<Integer, Integer> removedFields = cogroupRemovedFields.get(0);
        assertTrue(removedFields.first == 0);
        assertTrue(removedFields.second == 1);

        //check that cogroup added fields contain [1, 2]
        List<Integer> cogroupAddedFields = cogroupProjectionMap.getAddedFields();
        assertTrue(cogroupAddedFields.size() == 2);
        assertTrue(cogroupAddedFields.get(0) == 1);
        assertTrue(cogroupAddedFields.get(1) == 2);
        
        //check that the foreach projection map has non-null mappedFields
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
        assertTrue(foreachProjectionMap.changes() == true);
        
        MultiMap<Integer, Pair<Integer, Integer>> foreachMapFields = foreachProjectionMap.getMappedFields(); 
        assertTrue(foreachMapFields.size() == 3);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(0);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);
        
        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(1);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 1);

        mapValues = (ArrayList<Pair<Integer, Integer>>)foreachMapFields.get(2);
        assertTrue(mapValues.get(0).first == 0);
        assertTrue(mapValues.get(0).second == 2);

        
        //check that removed fields has all the group column from the input cogroup
        List<Pair<Integer, Integer>> foreachRemovedFields = foreachProjectionMap.getRemovedFields();
        assertTrue(foreachRemovedFields.size() == 1);
        Pair<Integer, Integer> removedField = foreachRemovedFields.get(0);
        assertTrue(removedField.first == 0);
        assertTrue(removedField.second == 0);
        
        //check that added fields is null
        List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
        assertTrue(foreachAddedFields == null);
    }
    
}
