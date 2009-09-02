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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.Test;

import junit.framework.TestCase;

public class TestRelevantFields extends TestCase {

    private static final String simpleEchoStreamingCommand;
    static {
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            simpleEchoStreamingCommand = "perl -ne 'print \\\"$_\\\"'";
        else
            simpleEchoStreamingCommand = "perl -ne 'print \"$_\"'";
    }
    
    LogicalPlanTester planTester = new LogicalPlanTester();
    @Test
    public void testQueryForeach1() {
        String query = "foreach (load 'a') generate $1,$2;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields0.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields0.getFields().size() == 1);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 1);
        RequiredFields foreachRelevantFields1 = foreach.getRelevantInputs(0, 1).get(0);
        assertTrue(foreachRelevantFields1.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields1.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields1.getFields().size() == 1);
        assertTrue(foreachRelevantFields1.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields1.getFields().get(0).second == 2);
    }

    @Test
    public void testQueryForeach2() {
        String query = "foreach (load 'a') generate $1,$2+$3;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields0.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 1);
        RequiredFields foreachRelevantFields1 = foreach.getRelevantInputs(0, 1).get(0);
        assertTrue(foreachRelevantFields1.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields1.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields1.getFields().size() == 2);
        assertTrue(foreachRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 2)));
        assertTrue(foreachRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 3)));
    }
    
    @Test
    public void testQueryForeach3() {
        String query = "foreach (load 'a') generate $1,CONCAT($2,$3);";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields0.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 1);
        RequiredFields foreachRelevantFields1 = foreach.getRelevantInputs(0, 1).get(0);
        assertTrue(foreachRelevantFields1.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields1.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields1.getFields().size() == 2);
        assertTrue(foreachRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 2)));
        assertTrue(foreachRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 3)));
    }
    
    @Test
    public void testQueryCogroup1() {
        String query = "foreach (cogroup (load 'a') by $1, (load 'b') by $1) generate org.apache.pig.builtin.AVG($1) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields cogroupRelevantFields0 = cogroup.getRelevantInputs(0, 0).get(0);
        assertTrue(cogroupRelevantFields0.getNeedAllFields()==false);
        assertTrue(cogroupRelevantFields0.getNeedNoFields()==false);
        assertTrue(cogroupRelevantFields0.getFields().size()==1);
        assertTrue(cogroupRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 1)));
        RequiredFields cogroupRelevantFields1 = cogroup.getRelevantInputs(0, 0).get(1);
        assertTrue(cogroupRelevantFields1.getFields().size()==1);
        assertTrue(cogroupRelevantFields1.getFields().contains(new Pair<Integer, Integer>(1, 1)));
        
        RequiredFields cogroupRelevantFields10 = cogroup.getRelevantInputs(0, 1).get(0);
        assertTrue(cogroupRelevantFields10.needNoFields() == false);
        assertTrue(cogroupRelevantFields10.needAllFields() == true);
        assertTrue(cogroupRelevantFields10.getFields() == null);
        
        RequiredFields cogroupRelevantFields11 = cogroup.getRelevantInputs(0, 1).get(1);
        assertTrue(cogroupRelevantFields11 == null);
        
        RequiredFields cogroupRelevantFields20 = cogroup.getRelevantInputs(0, 2).get(0);
        assertTrue(cogroupRelevantFields20 == null);
        
        RequiredFields cogroupRelevantFields21 = cogroup.getRelevantInputs(0, 2).get(1);
        assertTrue(cogroupRelevantFields21.needNoFields() == false);
        assertTrue(cogroupRelevantFields21.needAllFields() == true);
        assertTrue(cogroupRelevantFields21.getFields() == null);
    }

    @Test
    public void testQueryGroupAll() throws Exception {
        String query = "foreach (group (load 'a') ALL) generate $1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields cogroupRelevantFields0 = cogroup.getRelevantInputs(0, 0).get(0);
        assertTrue(cogroupRelevantFields0.getNeedAllFields()==false);
        assertTrue(cogroupRelevantFields0.getNeedNoFields()==true);
        assertTrue(cogroupRelevantFields0.getFields()==null);
        
        RequiredFields cogroupRelevantFields1 = cogroup.getRelevantInputs(0, 1).get(0);
        assertTrue(cogroupRelevantFields1.getNeedAllFields()==true);
        assertTrue(cogroupRelevantFields1.getNeedNoFields()==false);
        assertTrue(cogroupRelevantFields1.getFields()==null);
    }
    
    @Test
    public void testQueryGroup2() {
        String query = "foreach (group (load 'a') by $1) generate group, '1' ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields cogroupRelevantFields0 = cogroup.getRelevantInputs(0, 0).get(0);
        assertTrue(cogroupRelevantFields0.getNeedAllFields()==false);
        assertTrue(cogroupRelevantFields0.getNeedNoFields()==false);
        assertTrue(cogroupRelevantFields0.getFields().size()==1);
        assertTrue(cogroupRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 1)));
        
        RequiredFields cogroupRelevantFields1 = cogroup.getRelevantInputs(0, 1).get(0);
        assertTrue(cogroupRelevantFields1.getNeedAllFields()==true);
        assertTrue(cogroupRelevantFields1.getNeedNoFields()==false);
        assertTrue(cogroupRelevantFields1.getFields()==null);
    }

    @Test
    public void testQueryCogroup2() {
        String query = "foreach (cogroup (load 'a') by ($1), (load 'b') by ($1)) generate $1.$1, $2.$1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields cogroupRelevantFields01 = cogroup.getRelevantInputs(0, 0).get(0);
        assertTrue(cogroupRelevantFields01.getFields().size() == 1);
        assertTrue(cogroupRelevantFields01.getNeedAllFields() == false);
        assertTrue(cogroupRelevantFields01.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields01.getFields().contains(new Pair<Integer, Integer>(0, 1)));
        RequiredFields cogroupRelevantFields02 = cogroup.getRelevantInputs(0, 0).get(1);
        assertTrue(cogroupRelevantFields02.getFields().size() == 1);
        assertTrue(cogroupRelevantFields02.getNeedAllFields() == false);
        assertTrue(cogroupRelevantFields02.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields02.getFields().contains(new Pair<Integer, Integer>(1, 1)));
        
        RequiredFields cogroupRelevantFields1 = cogroup.getRelevantInputs(0, 1).get(0);
        assertTrue(cogroupRelevantFields1.getNeedAllFields() == true);
        assertTrue(cogroupRelevantFields1.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields1.getFields() == null);
        
        RequiredFields cogroupRelevantFields2 = cogroup.getRelevantInputs(0, 2).get(1);
        assertTrue(cogroupRelevantFields2.getNeedAllFields() == true);
        assertTrue(cogroupRelevantFields2.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields2.getFields() == null);
    }
    
    @Test
    public void testQueryGroup3() {
        String query = "foreach (group (load 'a') by ($6, $7)) generate flatten(group) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields cogroupRelevantFields0 = cogroup.getRelevantInputs(0, 0).get(0);
        assertTrue(cogroupRelevantFields0.getNeedAllFields() == false);
        assertTrue(cogroupRelevantFields0.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields0.getFields().size() == 2);
        assertTrue(cogroupRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 6)));
        assertTrue(cogroupRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 7)));
        
        RequiredFields cogroupRelevantFields1 = cogroup.getRelevantInputs(0, 1).get(0);
        assertTrue(cogroupRelevantFields1.getNeedAllFields() == true);
        assertTrue(cogroupRelevantFields1.getNeedNoFields() == false);        
    }

    public void testQueryFilterNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = filter a by $1 == '3';");
        
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        List<RequiredFields> filterRelevantFields0 = filter.getRelevantInputs(0, 0);
        assertTrue(filterRelevantFields0.get(0).getFields().size() == 1);
        assertTrue(filterRelevantFields0.get(0).getNeedAllFields()==false);
        assertTrue(filterRelevantFields0.get(0).getNeedAllFields()==false);
        assertTrue(filterRelevantFields0.get(0).getFields().contains(new Pair<Integer, Integer>(0, 0)));
    }

    @Test
    public void testQuerySplitNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("split a into b if $0 == '3', c if $1 == '3';");
        
        LOSplit split = (LOSplit)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields splitRelevantFields = split.getRelevantInputs(0, 0).get(0);
        assertTrue(splitRelevantFields.needAllFields() == false);
        assertTrue(splitRelevantFields.needNoFields() == true);
        assertTrue(splitRelevantFields.getFields() == null);

        LOSplitOutput splitb = (LOSplitOutput)lp.getSuccessors(split).get(0);
        RequiredFields splitbRelevantFields0 = splitb.getRelevantInputs(0, 0).get(0);
        assertTrue(splitbRelevantFields0.getNeedAllFields() == false);
        assertTrue(splitbRelevantFields0.getNeedNoFields() == false);
        assertTrue(splitbRelevantFields0.getFields().size() == 1);
        assertTrue(splitbRelevantFields0.getFields().get(0).first == 0);
        assertTrue(splitbRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields splitbRelevantFields1 = splitb.getRelevantInputs(0, 1).get(0);
        assertTrue(splitbRelevantFields1.getNeedAllFields() == false);
        assertTrue(splitbRelevantFields1.getNeedNoFields() == false);
        assertTrue(splitbRelevantFields1.getFields().size() == 1);
        assertTrue(splitbRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 1)));
        
        LOSplitOutput splitc = (LOSplitOutput)lp.getSuccessors(split).get(1);
        RequiredFields splitcRelevantFields0 = splitc.getRelevantInputs(0, 0).get(0);
        assertTrue(splitcRelevantFields0.getNeedAllFields() == false);
        assertTrue(splitcRelevantFields0.getNeedNoFields() == false);
        assertTrue(splitcRelevantFields0.getFields().size() == 1);
        assertTrue(splitcRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 0)));
        
        RequiredFields splitcRelevantFields = splitc.getRelevantInputs(0, 1).get(0);
        assertTrue(splitcRelevantFields.getNeedAllFields() == false);
        assertTrue(splitcRelevantFields.getNeedNoFields() == false);
        assertTrue(splitcRelevantFields.getFields().size() == 1);
        assertTrue(splitcRelevantFields.getFields().get(0).first == 0);
        assertTrue(splitcRelevantFields.getFields().get(0).second == 1);
    }

    @Test
    public void testQueryOrderByNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = order a by $1;");
        
        LOSort sort = (LOSort)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields sortRelevantFields0 = sort.getRelevantInputs(0, 0).get(0);
        assertTrue(sortRelevantFields0.getNeedAllFields() == false);
        assertTrue(sortRelevantFields0.getNeedNoFields() == false);
        assertTrue(sortRelevantFields0.getFields().size() == 1);
        assertTrue(sortRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 0)));
        
        RequiredFields sortRelevantFields1 = sort.getRelevantInputs(0, 1).get(0);
        assertTrue(sortRelevantFields1.getNeedAllFields() == false);
        assertTrue(sortRelevantFields1.getNeedNoFields() == false);
        assertTrue(sortRelevantFields1.getFields().size() == 1);
        assertTrue(sortRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 1)));
    }
    
    @Test
    public void testQueryLimitNoSchema() {
        planTester.buildPlan("a = load 'a';");
        planTester.buildPlan("b = order a by $1;");
        LogicalPlan lp = planTester.buildPlan("c = limit b 10;");
        
        LOLimit limit = (LOLimit)lp.getLeaves().get(0);
        List<RequiredFields> limitRelevantFields0 = limit.getRelevantInputs(0, 0);
        assertTrue(limitRelevantFields0.get(0).getFields().size()==1);
        assertTrue(limitRelevantFields0.get(0).getNeedAllFields()==false);
        assertTrue(limitRelevantFields0.get(0).getNeedNoFields()==false);
        assertTrue(limitRelevantFields0.get(0).getFields().get(0).first == 0);
        assertTrue(limitRelevantFields0.get(0).getFields().get(0).second == 0);
        
        List<RequiredFields> limitRelevantFields1 = limit.getRelevantInputs(0, 1);
        assertTrue(limitRelevantFields1.get(0).getFields().size()==1);
        assertTrue(limitRelevantFields1.get(0).getNeedAllFields()==false);
        assertTrue(limitRelevantFields1.get(0).getNeedNoFields()==false);
        assertTrue(limitRelevantFields1.get(0).getFields().get(0).first == 0);
        assertTrue(limitRelevantFields1.get(0).getFields().get(0).second == 1);
    }
    
    @Test
    public void testQueryDistinctNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = distinct a;");
        
        LODistinct distinct = (LODistinct)lp.getLeaves().get(0);
        List<RequiredFields> distinctRelevantFields0 = distinct.getRelevantInputs(0, 0);
        assertTrue(distinctRelevantFields0.get(0).getFields().size()==1);
        assertTrue(distinctRelevantFields0.get(0).getNeedAllFields()==false);
        assertTrue(distinctRelevantFields0.get(0).getNeedNoFields()==false);
        assertTrue(distinctRelevantFields0.get(0).getFields().get(0).first == 0);
        assertTrue(distinctRelevantFields0.get(0).getFields().get(0).second == 0);
        
        List<RequiredFields> distinctRelevantFields1 = distinct.getRelevantInputs(0, 1);
        assertTrue(distinctRelevantFields1.get(0).getFields().size()==1);
        assertTrue(distinctRelevantFields1.get(0).getNeedAllFields()==false);
        assertTrue(distinctRelevantFields1.get(0).getNeedNoFields()==false);
        assertTrue(distinctRelevantFields1.get(0).getFields().get(0).first == 0);
        assertTrue(distinctRelevantFields1.get(0).getFields().get(0).second == 1);
    }
    
    @Test
    public void testQueryStreamingNoSchema() {
        String query = "stream (load 'a') through `" + simpleEchoStreamingCommand + "`;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOStream stream = (LOStream)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields streamRelevantFields = stream.getRelevantInputs(0, 0).get(0);
        assertTrue(streamRelevantFields.getNeedAllFields() == true);
        assertTrue(streamRelevantFields.getNeedNoFields() == false);
        assertTrue(streamRelevantFields.getFields() == null);
    }

    @Test
    public void testQueryStreamingNoSchema1() {
        String query = "stream (load 'a' as (url, hitCount)) through `" + simpleEchoStreamingCommand + "` ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOStream stream = (LOStream)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields streamRelevantFields = stream.getRelevantInputs(0, 0).get(0);
        assertTrue(streamRelevantFields.getNeedAllFields() == true);
        assertTrue(streamRelevantFields.getNeedNoFields() == false);
        assertTrue(streamRelevantFields.getFields() == null);
    }
    
    @Test
    public void testQueryForeach4() throws FrontendException {
        planTester.buildPlan("A = load 'a';");
        planTester.buildPlan("B = load 'b';");
        LogicalPlan lp = planTester.buildPlan("foreach (cogroup A by ($1), B by ($1)) generate A, flatten(B.($1, $2, $3));");
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields0.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields0.getFields().size() == 1);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 1);
        RequiredFields foreachRelevantFields1 = foreach.getRelevantInputs(0, 1).get(0);
        assertTrue(foreachRelevantFields1.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields1.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields1.getFields().size() == 1);
        assertTrue(foreachRelevantFields1.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields1.getFields().get(0).second == 2);
        RequiredFields foreachRelevantFields2 = foreach.getRelevantInputs(0, 2).get(0);
        assertTrue(foreachRelevantFields2.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields2.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields2.getFields().size() == 1);
        assertTrue(foreachRelevantFields2.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields2.getFields().get(0).second == 2);
        RequiredFields foreachRelevantFields3 = foreach.getRelevantInputs(0, 3).get(0);
        assertTrue(foreachRelevantFields3.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields3.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields3.getFields().size() == 1);
        assertTrue(foreachRelevantFields3.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields3.getFields().get(0).second == 2);
        assertTrue(foreach.getRelevantInputs(0, 4) == null);
    }

    @Test
    public void testForeach5() {
        planTester.buildPlan("A = load 'a' AS (a1, a2, a3);");
        planTester.buildPlan("B = load 'b' AS (b1, b2, b3, b4);");
        planTester.buildPlan("C = cogroup A by ($1), B by ($1);");
        String query = "foreach C { " +
                "B = order B by $0; " +
                "generate FLATTEN(A), B.($1, $2, $3) ;" +
                "};" ;
        LogicalPlan lp = planTester.buildPlan(query);

        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.getNeedAllFields()==false);
        assertTrue(foreachRelevantFields0.getNeedNoFields()==false);
        assertTrue(foreachRelevantFields0.getFields().size()==1);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 1);
        
        RequiredFields foreachRelevantFields1 = foreach.getRelevantInputs(0, 1).get(0);
        assertTrue(foreachRelevantFields1.getNeedAllFields()==false);
        assertTrue(foreachRelevantFields1.getNeedNoFields()==false);
        assertTrue(foreachRelevantFields1.getFields().size()==1);
        assertTrue(foreachRelevantFields1.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields1.getFields().get(0).second == 1);
        
        RequiredFields foreachRelevantFields2 = foreach.getRelevantInputs(0, 2).get(0);
        assertTrue(foreachRelevantFields2.getNeedAllFields()==false);
        assertTrue(foreachRelevantFields2.getNeedNoFields()==false);
        assertTrue(foreachRelevantFields2.getFields().size()==1);
        assertTrue(foreachRelevantFields2.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields2.getFields().get(0).second == 1);
        
        RequiredFields foreachRelevantFields3 = foreach.getRelevantInputs(0, 3).get(0);
        assertTrue(foreachRelevantFields3.getNeedAllFields()==false);
        assertTrue(foreachRelevantFields3.getNeedNoFields()==false);
        assertTrue(foreachRelevantFields3.getFields().size()==1);
        assertTrue(foreachRelevantFields3.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields3.getFields().get(0).second == 2);
    }
    
    @Test
    public void testQueryCrossNoSchema(){
        String query = "c = cross (load 'a'), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        LOCross cross = (LOCross)lp.getLeaves().get(0);
        assertTrue(cross.getRelevantInputs(0, 0)==null);
    }
    
    @Test
    public void testQueryUnionNoSchema(){
        String query = "c = union (load 'a'), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);

        LOUnion union = (LOUnion)lp.getLeaves().get(0);
        RequiredFields unionRelevantFields0 = union.getRelevantInputs(0, 0).get(0);
        assertTrue(unionRelevantFields0.getNeedAllFields() == false);
        assertTrue(unionRelevantFields0.getNeedNoFields() == false);
        assertTrue(unionRelevantFields0.getFields().size() == 1);
        assertTrue(unionRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 0)));
        
        RequiredFields unionRelevantFields1 = union.getRelevantInputs(0, 0).get(1);
        assertTrue(unionRelevantFields1.getNeedAllFields() == false);
        assertTrue(unionRelevantFields1.getNeedNoFields() == false);
        assertTrue(unionRelevantFields1.getFields().size() == 1);
        assertTrue(unionRelevantFields1.getFields().contains(new Pair<Integer, Integer>(1, 0)));
    }

    public void testQueryFRJoinNoSchema(){
        String query = "c = join (load 'a') by $0, (load 'b') by $0 using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin frjoin = (LOJoin)lp.getLeaves().get(0);
        assertTrue(frjoin.getRelevantInputs(0, 0) == null);
    }
    
    @Test
    public void testQueryJoinNoSchema(){
        String query = "c = join (load 'a') by $0, (load 'b') by $0;";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin join = (LOJoin)lp.getLeaves().get(0);
        assertTrue(join.getRelevantInputs(0, 0) == null);
    }
    
    @Test
    public void testQueryFilterWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = filter a by $1 == '3';");
        
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        List<RequiredFields> filterRelevantFields0 = filter.getRelevantInputs(0, 0);
        assertTrue(filterRelevantFields0.get(0).getFields().size() == 1);
        assertTrue(filterRelevantFields0.get(0).getNeedAllFields()==false);
        assertTrue(filterRelevantFields0.get(0).getNeedAllFields()==false);
        assertTrue(filterRelevantFields0.get(0).getFields().contains(new Pair<Integer, Integer>(0, 0)));
    }
    
    @Test
    public void testQuerySplitWithSchema() {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        LogicalPlan lp = planTester.buildPlan("split a into b if url == '3', c if hitCount == '3';");
        
        LOSplit split = (LOSplit)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields splitRelevantFields = split.getRelevantInputs(0, 0).get(0);
        assertTrue(splitRelevantFields.needAllFields() == false);
        assertTrue(splitRelevantFields.needNoFields() == true);
        assertTrue(splitRelevantFields.getFields() == null);

        LOSplitOutput splitb = (LOSplitOutput)lp.getSuccessors(split).get(0);
        RequiredFields splitbRelevantFields = splitb.getRelevantInputs(0, 1).get(0);
        assertTrue(splitbRelevantFields.getNeedAllFields() == false);
        assertTrue(splitbRelevantFields.getNeedNoFields() == false);
        assertTrue(splitbRelevantFields.getFields().size() == 1);
        assertTrue(splitbRelevantFields.getFields().contains(new Pair<Integer, Integer>(0, 1)));
        
        LOSplitOutput splitc = (LOSplitOutput)lp.getSuccessors(split).get(0);
        RequiredFields splitcRelevantFields = splitc.getRelevantInputs(0, 1).get(0);
        assertTrue(splitcRelevantFields.getNeedAllFields() == false);
        assertTrue(splitcRelevantFields.getNeedNoFields() == false);
        assertTrue(splitcRelevantFields.getFields().size() == 1);
        assertTrue(splitcRelevantFields.getFields().contains(new Pair<Integer, Integer>(0, 1)));
    }
    
    @Test
    public void testQueryOrderByWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = order a by $1;");
        
        LOSort sort = (LOSort)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields sortRelevantFields0 = sort.getRelevantInputs(0, 0).get(0);
        assertTrue(sortRelevantFields0.getNeedAllFields() == false);
        assertTrue(sortRelevantFields0.getNeedNoFields() == false);
        assertTrue(sortRelevantFields0.getFields().size() == 1);
        assertTrue(sortRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 0)));
        
        RequiredFields sortRelevantFields1 = sort.getRelevantInputs(0, 1).get(0);
        assertTrue(sortRelevantFields1.getNeedAllFields() == false);
        assertTrue(sortRelevantFields1.getNeedNoFields() == false);
        assertTrue(sortRelevantFields1.getFields().size() == 1);
        assertTrue(sortRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 1)));
    }
    
    @Test
    public void testQueryLimitWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        planTester.buildPlan("b = order a by $1;");
        LogicalPlan lp = planTester.buildPlan("c = limit b 10;");
        
        LOLimit limit = (LOLimit)lp.getLeaves().get(0);
        List<RequiredFields> limitRelevantFields0 = limit.getRelevantInputs(0, 0);
        assertTrue(limitRelevantFields0.get(0).getFields().size()==1);
        assertTrue(limitRelevantFields0.get(0).getNeedAllFields()==false);
        assertTrue(limitRelevantFields0.get(0).getNeedNoFields()==false);
        assertTrue(limitRelevantFields0.get(0).getFields().get(0).first == 0);
        assertTrue(limitRelevantFields0.get(0).getFields().get(0).second == 0);
        
        List<RequiredFields> limitRelevantFields1 = limit.getRelevantInputs(0, 1);
        assertTrue(limitRelevantFields1.get(0).getFields().size()==1);
        assertTrue(limitRelevantFields1.get(0).getNeedAllFields()==false);
        assertTrue(limitRelevantFields1.get(0).getNeedNoFields()==false);
        assertTrue(limitRelevantFields1.get(0).getFields().get(0).first == 0);
        assertTrue(limitRelevantFields1.get(0).getFields().get(0).second == 1);
    }
    
    @Test
    public void testQueryDistinctWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = distinct a;");
        
        LODistinct distinct = (LODistinct)lp.getLeaves().get(0);
        List<RequiredFields> distinctRelevantFields0 = distinct.getRelevantInputs(0, 0);
        assertTrue(distinctRelevantFields0.get(0).getFields().size()==1);
        assertTrue(distinctRelevantFields0.get(0).getNeedAllFields()==false);
        assertTrue(distinctRelevantFields0.get(0).getNeedNoFields()==false);
        assertTrue(distinctRelevantFields0.get(0).getFields().get(0).first == 0);
        assertTrue(distinctRelevantFields0.get(0).getFields().get(0).second == 0);
        
        List<RequiredFields> distinctRelevantFields1 = distinct.getRelevantInputs(0, 1);
        assertTrue(distinctRelevantFields1.get(0).getFields().size()==1);
        assertTrue(distinctRelevantFields1.get(0).getNeedAllFields()==false);
        assertTrue(distinctRelevantFields1.get(0).getNeedNoFields()==false);
        assertTrue(distinctRelevantFields1.get(0).getFields().get(0).first == 0);
        assertTrue(distinctRelevantFields1.get(0).getFields().get(0).second == 1);
    }

    @Test
    public void testQueryStreamingWithSchema() {
        String query = "stream (load 'a') through `" + simpleEchoStreamingCommand + "` as (x, y);";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOStream stream = (LOStream)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields streamRelevantFields = stream.getRelevantInputs(0, 0).get(0);
        assertTrue(streamRelevantFields.getNeedAllFields() == true);
        assertTrue(streamRelevantFields.getNeedNoFields() == false);
        assertTrue(streamRelevantFields.getFields() == null);
    }

    @Test
    public void testQueryStreamingWithSchema1() {
        String query = "stream (load 'a' as (url, hitCount)) through `" + simpleEchoStreamingCommand + "` as (x, y);";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOStream stream = (LOStream)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields streamRelevantFields = stream.getRelevantInputs(0, 0).get(0);
        assertTrue(streamRelevantFields.getNeedAllFields() == true);
        assertTrue(streamRelevantFields.getNeedNoFields() == false);
        assertTrue(streamRelevantFields.getFields() == null);
    }
    
    @Test
    public void testQueryImplicitJoinWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        planTester.buildPlan("b = load 'b' as (url,rank);");
        planTester.buildPlan("c = cogroup a by url, b by url;");
        LogicalPlan lp = planTester.buildPlan("d = foreach c generate group,flatten(a),flatten(b);");

        //check cogroup required fields
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        assertTrue(cogroup.getRelevantInputs(0, 0).size() == 2);
        RequiredFields cogroupRelevantFields00 = cogroup.getRelevantInputs(0, 0).get(0);
        assertTrue(cogroupRelevantFields00.getNeedAllFields() == false);
        assertTrue(cogroupRelevantFields00.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields00.getFields().size() == 1);
        assertTrue(cogroupRelevantFields00.getFields().get(0).first == 0);
        assertTrue(cogroupRelevantFields00.getFields().get(0).second == 0);
        
        RequiredFields cogroupRelevantFields01 = cogroup.getRelevantInputs(0, 0).get(1);
        assertTrue(cogroupRelevantFields01.getNeedAllFields() == false);
        assertTrue(cogroupRelevantFields01.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields01.getFields().size() == 1);
        assertTrue(cogroupRelevantFields01.getFields().get(0).first == 1);
        assertTrue(cogroupRelevantFields01.getFields().get(0).second == 0);
        
        
        assertTrue(cogroup.getRelevantInputs(0, 1).size() == 2);
        RequiredFields cogroupRelevantFields1 = cogroup.getRelevantInputs(0, 1).get(0);
        assertTrue(cogroupRelevantFields1.getNeedAllFields() == true);
        assertTrue(cogroupRelevantFields1.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields1.getFields() == null);
        assertTrue(cogroup.getRelevantInputs(0, 1).get(1) == null);
        
        RequiredFields cogroupRelevantFields2 = cogroup.getRelevantInputs(0, 2).get(1);
        assertTrue(cogroupRelevantFields2.getNeedAllFields() == true);
        assertTrue(cogroupRelevantFields2.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields2.getFields() == null);
        assertTrue(cogroup.getRelevantInputs(0, 2).get(0) == null);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields0.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields0.getFields().size() == 1);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields foreachRelevantFields1 = foreach.getRelevantInputs(0, 1).get(0);
        assertTrue(foreachRelevantFields1.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields1.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields1.getFields().size() == 1);
        assertTrue(foreachRelevantFields1.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields1.getFields().get(0).second == 1);
        
        RequiredFields foreachRelevantFields2 = foreach.getRelevantInputs(0, 2).get(0);
        assertTrue(foreachRelevantFields2.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields2.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields2.getFields().size() == 1);
        assertTrue(foreachRelevantFields2.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields2.getFields().get(0).second == 1);
        
        RequiredFields foreachRelevantFields3 = foreach.getRelevantInputs(0, 3).get(0);
        assertTrue(foreachRelevantFields3.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields3.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields3.getFields().size() == 1);
        assertTrue(foreachRelevantFields3.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields3.getFields().get(0).second == 2);
        
        RequiredFields foreachRelevantFields4 = foreach.getRelevantInputs(0, 4).get(0);
        assertTrue(foreachRelevantFields4.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields4.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields4.getFields().size() == 1);
        assertTrue(foreachRelevantFields4.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields4.getFields().get(0).second == 2);
        
        assertTrue(foreach.getRelevantInputs(0, 5) == null);
    }
    
    @Test
    public void testQueryCrossWithSchema(){
        String query = "c = cross (load 'a' as (a, b, c)), (load 'b' as (d, e, f));";
        LogicalPlan lp = planTester.buildPlan(query);

        //check cross required fields
        LOCross cross = (LOCross)lp.getLeaves().get(0);
        
        RequiredFields crossRelevantFields0 = cross.getRelevantInputs(0, 0).get(0);
        assertTrue(crossRelevantFields0.getNeedAllFields()==false);
        assertTrue(crossRelevantFields0.getNeedNoFields()==false);
        assertTrue(crossRelevantFields0.getFields().get(0).first==0);
        assertTrue(crossRelevantFields0.getFields().get(0).second==0);
        
        assertTrue(cross.getRelevantInputs(0, 0).get(1)==null);
        
        RequiredFields crossRelevantFields1 = cross.getRelevantInputs(0, 1).get(0);
        assertTrue(crossRelevantFields1.getNeedAllFields()==false);
        assertTrue(crossRelevantFields1.getNeedNoFields()==false);
        assertTrue(crossRelevantFields1.getFields().get(0).first==0);
        assertTrue(crossRelevantFields1.getFields().get(0).second==1);
        
        assertTrue(cross.getRelevantInputs(0, 1).get(1)==null);
        
        RequiredFields crossRelevantFields2 = cross.getRelevantInputs(0, 2).get(0);
        assertTrue(crossRelevantFields2.getNeedAllFields()==false);
        assertTrue(crossRelevantFields2.getNeedNoFields()==false);
        assertTrue(crossRelevantFields2.getFields().get(0).first==0);
        assertTrue(crossRelevantFields2.getFields().get(0).second==2);
        
        assertTrue(cross.getRelevantInputs(0, 2).get(1)==null);
        
        RequiredFields crossRelevantFields3 = cross.getRelevantInputs(0, 3).get(1);
        assertTrue(crossRelevantFields3.getNeedAllFields()==false);
        assertTrue(crossRelevantFields3.getNeedNoFields()==false);
        assertTrue(crossRelevantFields3.getFields().get(0).first==1);
        assertTrue(crossRelevantFields3.getFields().get(0).second==0);
        
        assertTrue(cross.getRelevantInputs(0, 3).get(0)==null);
        
        RequiredFields crossRelevantFields4 = cross.getRelevantInputs(0, 4).get(1);
        assertTrue(crossRelevantFields4.getNeedAllFields()==false);
        assertTrue(crossRelevantFields4.getNeedNoFields()==false);
        assertTrue(crossRelevantFields4.getFields().get(0).first==1);
        assertTrue(crossRelevantFields4.getFields().get(0).second==1);
        
        assertTrue(cross.getRelevantInputs(0, 4).get(0)==null);
        
        RequiredFields crossRelevantFields5 = cross.getRelevantInputs(0, 5).get(1);
        assertTrue(crossRelevantFields5.getNeedAllFields()==false);
        assertTrue(crossRelevantFields5.getNeedAllFields()==false);
        assertTrue(crossRelevantFields5.getFields().get(0).first==1);
        assertTrue(crossRelevantFields5.getFields().get(0).second==2);
        
        assertTrue(cross.getRelevantInputs(0, 5).get(0)==null);
    }
    
    @Test
    public void testQueryUnionWithSchema(){
        String query = "c = union (load 'a' as (url, hitcount)), (load 'b' as (url, rank));";
        LogicalPlan lp = planTester.buildPlan(query);

        LOUnion union = (LOUnion)lp.getLeaves().get(0);
        RequiredFields unionRelevantFields0 = union.getRelevantInputs(0, 0).get(0);
        assertTrue(unionRelevantFields0.getNeedAllFields() == false);
        assertTrue(unionRelevantFields0.getNeedNoFields() == false);
        assertTrue(unionRelevantFields0.getFields().size() == 1);
        assertTrue(unionRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 0)));
        
        RequiredFields unionRelevantFields1 = union.getRelevantInputs(0, 0).get(1);
        assertTrue(unionRelevantFields1.getNeedAllFields() == false);
        assertTrue(unionRelevantFields1.getNeedNoFields() == false);
        assertTrue(unionRelevantFields1.getFields().size() == 1);
        assertTrue(unionRelevantFields1.getFields().contains(new Pair<Integer, Integer>(1, 0)));
    }

    @Test
    public void testQueryFRJoinSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b' as (url, rank)) by $0 using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin frjoin = (LOJoin)lp.getLeaves().get(0);
        RequiredFields frjoinRelevantFields0 = frjoin.getRelevantInputs(0, 0).get(0);
        assertTrue(frjoinRelevantFields0.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields0.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields0.getFields().size() == 1);
        assertTrue(frjoinRelevantFields0.getFields().get(0).first == 0);
        assertTrue(frjoinRelevantFields0.getFields().get(0).second == 0);
        
        assertTrue(frjoin.getRelevantInputs(0, 0).get(1)==null);
        
        RequiredFields frjoinRelevantFields1 = frjoin.getRelevantInputs(0, 1).get(0);
        assertTrue(frjoinRelevantFields1.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields1.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields1.getFields().size() == 1);
        assertTrue(frjoinRelevantFields1.getFields().get(0).first == 0);
        assertTrue(frjoinRelevantFields1.getFields().get(0).second == 1);
        
        assertTrue(frjoin.getRelevantInputs(0, 1).get(1)==null);
        
        RequiredFields frjoinRelevantFields2 = frjoin.getRelevantInputs(0, 2).get(1);
        assertTrue(frjoinRelevantFields2.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields2.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields2.getFields().size() == 1);
        assertTrue(frjoinRelevantFields2.getFields().get(0).first == 1);
        assertTrue(frjoinRelevantFields2.getFields().get(0).second == 0);
        
        assertTrue(frjoin.getRelevantInputs(0, 2).get(0)==null);
        
        RequiredFields frjoinRelevantFields3 = frjoin.getRelevantInputs(0, 3).get(1);
        assertTrue(frjoinRelevantFields3.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields3.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields3.getFields().size() == 1);
        assertTrue(frjoinRelevantFields3.getFields().get(0).first == 1);
        assertTrue(frjoinRelevantFields3.getFields().get(0).second == 1);
        
        assertTrue(frjoin.getRelevantInputs(0, 3).get(0)==null);
        
        assertTrue(frjoin.getRelevantInputs(0, 4)==null);
    }

    @Test
    public void testQueryJoinWithSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b' as (url, rank)) by $0;";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin join = (LOJoin)lp.getLeaves().get(0);
        RequiredFields joinRelevantFields0 = join.getRelevantInputs(0, 0).get(0);
        assertTrue(joinRelevantFields0.getNeedAllFields() == false);
        assertTrue(joinRelevantFields0.getNeedNoFields() == false);
        assertTrue(joinRelevantFields0.getFields().size() == 1);
        assertTrue(joinRelevantFields0.getFields().get(0).first == 0);
        assertTrue(joinRelevantFields0.getFields().get(0).second == 0);
        
        assertTrue(join.getRelevantInputs(0, 0).get(1)==null);
        
        RequiredFields joinRelevantFields1 = join.getRelevantInputs(0, 1).get(0);
        assertTrue(joinRelevantFields1.getNeedAllFields() == false);
        assertTrue(joinRelevantFields1.getNeedNoFields() == false);
        assertTrue(joinRelevantFields1.getFields().size() == 1);
        assertTrue(joinRelevantFields1.getFields().get(0).first == 0);
        assertTrue(joinRelevantFields1.getFields().get(0).second == 1);
        
        assertTrue(join.getRelevantInputs(0, 1).get(1)==null);
        
        RequiredFields joinRelevantFields2 = join.getRelevantInputs(0, 2).get(1);
        assertTrue(joinRelevantFields2.getNeedAllFields() == false);
        assertTrue(joinRelevantFields2.getNeedNoFields() == false);
        assertTrue(joinRelevantFields2.getFields().size() == 1);
        assertTrue(joinRelevantFields2.getFields().get(0).first == 1);
        assertTrue(joinRelevantFields2.getFields().get(0).second == 0);
        
        assertTrue(join.getRelevantInputs(0, 2).get(0)==null);
        
        RequiredFields joinRelevantFields3 = join.getRelevantInputs(0, 3).get(1);
        assertTrue(joinRelevantFields3.getNeedAllFields() == false);
        assertTrue(joinRelevantFields3.getNeedNoFields() == false);
        assertTrue(joinRelevantFields3.getFields().size() == 1);
        assertTrue(joinRelevantFields3.getFields().get(0).first == 1);
        assertTrue(joinRelevantFields3.getFields().get(0).second == 1);
        
        assertTrue(join.getRelevantInputs(0, 3).get(0)==null);
        
        assertTrue(join.getRelevantInputs(0, 4)==null);
    }
    
    @Test
    public void testQueryCrossWithMixedSchema(){
        String query = "c = cross (load 'a' as (url, hitcount)), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOCross cross = (LOCross)lp.getLeaves().get(0);
        RequiredFields crossRelevantFields0 = cross.getRelevantInputs(0, 0).get(0);
        assertTrue(crossRelevantFields0.getNeedAllFields()==false);
        assertTrue(crossRelevantFields0.getNeedNoFields()==false);
        assertTrue(crossRelevantFields0.getFields().get(0).first==0);
        assertTrue(crossRelevantFields0.getFields().get(0).second==0);
        
        RequiredFields crossRelevantFields1 = cross.getRelevantInputs(0, 1).get(0);
        assertTrue(crossRelevantFields1.getNeedAllFields()==false);
        assertTrue(crossRelevantFields1.getNeedNoFields()==false);
        assertTrue(crossRelevantFields1.getFields().get(0).first==0);
        assertTrue(crossRelevantFields1.getFields().get(0).second==1);
        
        assertTrue(cross.getRelevantInputs(0, 2)==null);
    }


    @Test
    public void testQueryUnionWithMixedSchema(){
        String query = "c = union (load 'a' as (url, hitcount)), (load 'b');";
        LogicalPlan lp = planTester.buildPlan(query);
        LOUnion union = (LOUnion)lp.getLeaves().get(0);
        
        RequiredFields unionRelevantFields0 = union.getRelevantInputs(0, 0).get(0);
        assertTrue(unionRelevantFields0.getNeedAllFields() == false);
        assertTrue(unionRelevantFields0.getNeedNoFields() == false);
        assertTrue(unionRelevantFields0.getFields().size() == 1);
        assertTrue(unionRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 0)));
        
        RequiredFields unionRelevantFields1 = union.getRelevantInputs(0, 0).get(1);
        assertTrue(unionRelevantFields1.getNeedAllFields() == false);
        assertTrue(unionRelevantFields1.getNeedNoFields() == false);
        assertTrue(unionRelevantFields1.getFields().size() == 1);
        assertTrue(unionRelevantFields1.getFields().contains(new Pair<Integer, Integer>(1, 0)));
    }
    
    @Test
    public void testQueryFRJoinWithMixedSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b') by $0 using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin frjoin = (LOJoin)lp.getLeaves().get(0);
        RequiredFields frjoinRelevantFields0 = frjoin.getRelevantInputs(0, 0).get(0);
        assertTrue(frjoinRelevantFields0.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields0.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields0.getFields().size() == 1);
        assertTrue(frjoinRelevantFields0.getFields().get(0).first == 0);
        assertTrue(frjoinRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields frjoinRelevantFields1 = frjoin.getRelevantInputs(0, 1).get(0);
        assertTrue(frjoinRelevantFields1.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields1.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields1.getFields().size() == 1);
        assertTrue(frjoinRelevantFields1.getFields().get(0).first == 0);
        assertTrue(frjoinRelevantFields1.getFields().get(0).second == 1);
        
        assertTrue(frjoin.getRelevantInputs(0, 2)==null);
    }
    
    @Test
    public void testQueryJoinWithMixedSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by $0, (load 'b') by $0;";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin join = (LOJoin)lp.getLeaves().get(0);
        RequiredFields joinRelevantFields0 = join.getRelevantInputs(0, 0).get(0);
        assertTrue(joinRelevantFields0.getNeedAllFields() == false);
        assertTrue(joinRelevantFields0.getNeedNoFields() == false);
        assertTrue(joinRelevantFields0.getFields().size() == 1);
        assertTrue(joinRelevantFields0.getFields().get(0).first == 0);
        assertTrue(joinRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields joinRelevantFields1 = join.getRelevantInputs(0, 1).get(0);
        assertTrue(joinRelevantFields1.getNeedAllFields() == false);
        assertTrue(joinRelevantFields1.getNeedNoFields() == false);
        assertTrue(joinRelevantFields1.getFields().size() == 1);
        assertTrue(joinRelevantFields1.getFields().get(0).first == 0);
        assertTrue(joinRelevantFields1.getFields().get(0).second == 1);
        
        assertTrue(join.getRelevantInputs(0, 2)==null);
    }
    
    @Test
    public void testQueryFilterWithStarNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = filter a by COUNT(*) == 3;");
        
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        RequiredFields filterRelevantFields0 = filter.getRelevantInputs(0, 0).get(0);
        assertTrue(filterRelevantFields0.getNeedAllFields() == false);
        assertTrue(filterRelevantFields0.getNeedNoFields() == false);
        assertTrue(filterRelevantFields0.getFields().size() == 1);
        assertTrue(filterRelevantFields0.getFields().get(0).first == 0);
        assertTrue(filterRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields filterRelevantFields1 = filter.getRelevantInputs(0, 1).get(0);
        assertTrue(filterRelevantFields1.getNeedAllFields() == false);
        assertTrue(filterRelevantFields1.getNeedNoFields() == false);
        assertTrue(filterRelevantFields1.getFields().size() == 1);
        assertTrue(filterRelevantFields1.getFields().get(0).first == 0);
        assertTrue(filterRelevantFields1.getFields().get(0).second == 1);
    }
    
    @Test
    public void testQueryOrderByStarNoSchema() {
        planTester.buildPlan("a = load 'a';");
        LogicalPlan lp = planTester.buildPlan("b = order a by *;");
        
        LOSort sort = (LOSort)lp.getLeaves().get(0);
        RequiredFields sortRelevantFields0 = sort.getRelevantInputs(0, 0).get(0);
        assertTrue(sortRelevantFields0.getNeedAllFields() == false);
        assertTrue(sortRelevantFields0.getNeedNoFields() == false);
        assertTrue(sortRelevantFields0.getFields().size() == 1);
        assertTrue(sortRelevantFields0.getFields().get(0).first == 0);
        assertTrue(sortRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields sortRelevantFields1 = sort.getRelevantInputs(0, 1).get(0);
        assertTrue(sortRelevantFields1.getNeedAllFields() == false);
        assertTrue(sortRelevantFields1.getNeedNoFields() == false);
        assertTrue(sortRelevantFields1.getFields().size() == 1);
        assertTrue(sortRelevantFields1.getFields().get(0).first == 0);
        assertTrue(sortRelevantFields1.getFields().get(0).second == 1);
    }
    
    @Test
    public void testQueryGroupByStarNoSchema() throws Exception {
        String query = "foreach (group (load 'a') by *) generate $1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields cogroupRelevantFields0 = cogroup.getRelevantInputs(0, 0).get(0);
        assertTrue(cogroupRelevantFields0.getNeedAllFields() == true);
        assertTrue(cogroupRelevantFields0.getNeedNoFields() == false);
        
        RequiredFields cogroupRelevantFields1 = cogroup.getRelevantInputs(0, 1).get(0);
        assertTrue(cogroupRelevantFields1.getNeedAllFields() == true);
        assertTrue(cogroupRelevantFields1.getNeedNoFields() == false);
        
        LOForEach foreach = (LOForEach)lp.getSuccessors(cogroup).get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.needNoFields() == false);
        assertTrue(foreachRelevantFields0.needAllFields() == false);
        assertTrue(foreachRelevantFields0.getFields().size() == 1);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 1);
        
        assertTrue(foreach.getRelevantInputs(0, 1) == null);
    }
    
    @Test
    public void testQueryFRJoinOnStarNoSchema(){
        String query = "c = join (load 'a') by *, (load 'b') by * using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin frjoin = (LOJoin)lp.getLeaves().get(0);
        assertTrue(frjoin.getRelevantInputs(0, 0) == null);
    }
    
    @Test
    public void testQueryJoinOnStarNoSchema(){
        String query = "c = join (load 'a') by *, (load 'b') by *;";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin join = (LOJoin)lp.getLeaves().get(0);
        assertTrue(join.getRelevantInputs(0, 0) == null);
    }
    
    @Test
    public void testQueryFilterStarWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = filter a by COUNT(*) == 3;");
        
        LOFilter filter = (LOFilter)lp.getLeaves().get(0);
        RequiredFields filterRelevantFields0 = filter.getRelevantInputs(0, 0).get(0);
        assertTrue(filterRelevantFields0.getNeedAllFields() == false);
        assertTrue(filterRelevantFields0.getNeedNoFields() == false);
        assertTrue(filterRelevantFields0.getFields().size() == 1);
        assertTrue(filterRelevantFields0.getFields().get(0).first == 0);
        assertTrue(filterRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields filterRelevantFields1 = filter.getRelevantInputs(0, 1).get(0);
        assertTrue(filterRelevantFields1.getNeedAllFields() == false);
        assertTrue(filterRelevantFields1.getNeedNoFields() == false);
        assertTrue(filterRelevantFields1.getFields().size() == 1);
        assertTrue(filterRelevantFields1.getFields().get(0).first == 0);
        assertTrue(filterRelevantFields1.getFields().get(0).second == 1);
    }
    
    @Test
    public void testQuerySplitWithStarSchema() {
        planTester.buildPlan("a = load 'a' as (url, hitCount);");
        LogicalPlan lp = planTester.buildPlan("split a into b if url == '3', c if COUNT(*) == '3';");
        
        LOSplit split = (LOSplit)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields splitRelevantFields = split.getRelevantInputs(0, 0).get(0);
        assertTrue(splitRelevantFields.needAllFields() == false);
        assertTrue(splitRelevantFields.needNoFields() == true);
        assertTrue(splitRelevantFields.getFields() == null);

        LOSplitOutput splitb = (LOSplitOutput)lp.getSuccessors(split).get(0);
        RequiredFields splitbRelevantFields0 = splitb.getRelevantInputs(0, 0).get(0);
        assertTrue(splitbRelevantFields0.getNeedAllFields() == false);
        assertTrue(splitbRelevantFields0.getNeedNoFields() == false);
        assertTrue(splitbRelevantFields0.getFields().size() == 1);
        assertTrue(splitbRelevantFields0.getFields().get(0).first == 0);
        assertTrue(splitbRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields splitbRelevantFields1 = splitb.getRelevantInputs(0, 1).get(0);
        assertTrue(splitbRelevantFields1.getNeedAllFields() == false);
        assertTrue(splitbRelevantFields1.getNeedNoFields() == false);
        assertTrue(splitbRelevantFields1.getFields().size() == 1);
        assertTrue(splitbRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 1)));
        
        LOSplitOutput splitc = (LOSplitOutput)lp.getSuccessors(split).get(1);
        RequiredFields splitcRelevantFields0 = splitc.getRelevantInputs(0, 0).get(0);
        assertTrue(splitcRelevantFields0.getNeedAllFields() == false);
        assertTrue(splitcRelevantFields0.getNeedNoFields() == false);
        assertTrue(splitcRelevantFields0.getFields().size() == 1);
        assertTrue(splitcRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 0)));
        
        RequiredFields splitcRelevantFields1 = splitc.getRelevantInputs(0, 1).get(0);
        assertTrue(splitcRelevantFields1.getNeedAllFields() == false);
        assertTrue(splitcRelevantFields1.getNeedNoFields() == false);
        assertTrue(splitcRelevantFields1.getFields().size() == 1);
        assertTrue(splitcRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 1)));
    }
    
    @Test
    public void testQueryOrderByStarWithSchema() {
        planTester.buildPlan("a = load 'a' as (url,hitCount);");
        LogicalPlan lp = planTester.buildPlan("b = order a by *;");
        
        LOSort sort = (LOSort)lp.getLeaves().get(0);
        RequiredFields sortRelevantFields0 = sort.getRelevantInputs(0, 0).get(0);
        assertTrue(sortRelevantFields0.getNeedAllFields() == false);
        assertTrue(sortRelevantFields0.getNeedNoFields() == false);
        assertTrue(sortRelevantFields0.getFields().size() == 1);
        assertTrue(sortRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 0)));
        
        RequiredFields sortRelevantFields1 = sort.getRelevantInputs(0, 1).get(0);
        assertTrue(sortRelevantFields1.getNeedAllFields() == false);
        assertTrue(sortRelevantFields1.getNeedNoFields() == false);
        assertTrue(sortRelevantFields1.getFields().size() == 1);
        assertTrue(sortRelevantFields1.getFields().contains(new Pair<Integer, Integer>(0, 1)));
    }
    
    @Test
    public void testQueryGroupByStarWithSchema() throws Exception {
        String query = "foreach (group (load 'a' as (url, hitCount)) by *) generate $1 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOCogroup cogroup = (LOCogroup)lp.getSuccessors(lp.getRoots().get(0)).get(0);
        RequiredFields cogroupRelevantFields0 = cogroup.getRelevantInputs(0, 0).get(0);
        assertTrue(cogroupRelevantFields0.getNeedAllFields() == false);
        assertTrue(cogroupRelevantFields0.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields0.getFields().size() == 2);
        assertTrue(cogroupRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 0)));
        assertTrue(cogroupRelevantFields0.getFields().contains(new Pair<Integer, Integer>(0, 1)));
        
        RequiredFields cogroupRelevantFields1 = cogroup.getRelevantInputs(0, 1).get(0);
        assertTrue(cogroupRelevantFields1.getNeedAllFields() == true);
        assertTrue(cogroupRelevantFields1.getNeedNoFields() == false);
        assertTrue(cogroupRelevantFields1.getFields() == null);
        
        assertTrue(cogroup.getRelevantInputs(0, 2) == null);
        
        LOForEach foreach = (LOForEach)lp.getSuccessors(cogroup).get(0);
        RequiredFields forEachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(forEachRelevantFields0.getNeedAllFields() == false);
        assertTrue(forEachRelevantFields0.getNeedNoFields() == false);
        assertTrue(forEachRelevantFields0.getFields().size() == 1);
        assertTrue(forEachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(forEachRelevantFields0.getFields().get(0).second == 1);
        
        assertTrue(foreach.getRelevantInputs(0, 1) == null);
    }
    
    @Test
    public void testQueryFRJoinOnStarWithSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by *, (load 'b' as (url, rank)) by * using \"replicated\";";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin frjoin = (LOJoin)lp.getLeaves().get(0);
        RequiredFields frjoinRelevantFields0 = frjoin.getRelevantInputs(0, 0).get(0);
        assertTrue(frjoinRelevantFields0.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields0.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields0.getFields().size() == 1);
        assertTrue(frjoinRelevantFields0.getFields().get(0).first == 0);
        assertTrue(frjoinRelevantFields0.getFields().get(0).second == 0);
        
        assertTrue(frjoin.getRelevantInputs(0, 0).get(1)==null);
        
        RequiredFields frjoinRelevantFields1 = frjoin.getRelevantInputs(0, 1).get(0);
        assertTrue(frjoinRelevantFields1.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields1.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields1.getFields().size() == 1);
        assertTrue(frjoinRelevantFields1.getFields().get(0).first == 0);
        assertTrue(frjoinRelevantFields1.getFields().get(0).second == 1);
        
        assertTrue(frjoin.getRelevantInputs(0, 1).get(1)==null);
        
        RequiredFields frjoinRelevantFields2 = frjoin.getRelevantInputs(0, 2).get(1);
        assertTrue(frjoinRelevantFields2.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields2.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields2.getFields().size() == 1);
        assertTrue(frjoinRelevantFields2.getFields().get(0).first == 1);
        assertTrue(frjoinRelevantFields2.getFields().get(0).second == 0);
        
        assertTrue(frjoin.getRelevantInputs(0, 2).get(0)==null);
        
        RequiredFields frjoinRelevantFields3 = frjoin.getRelevantInputs(0, 3).get(1);
        assertTrue(frjoinRelevantFields3.getNeedAllFields() == false);
        assertTrue(frjoinRelevantFields3.getNeedNoFields() == false);
        assertTrue(frjoinRelevantFields3.getFields().size() == 1);
        assertTrue(frjoinRelevantFields3.getFields().get(0).first == 1);
        assertTrue(frjoinRelevantFields3.getFields().get(0).second == 1);
        
        assertTrue(frjoin.getRelevantInputs(0, 3).get(0)==null);
        
        assertTrue(frjoin.getRelevantInputs(0, 4)==null);
    }

    @Test
    public void testQueryJoinOnStarWithSchema(){
        String query = "c = join (load 'a' as (url, hitcount)) by *, (load 'b' as (url, rank)) by *;";
        LogicalPlan lp = planTester.buildPlan(query);

        LOJoin join = (LOJoin)lp.getLeaves().get(0);
        RequiredFields joinRelevantFields0 = join.getRelevantInputs(0, 0).get(0);
        assertTrue(joinRelevantFields0.getNeedAllFields() == false);
        assertTrue(joinRelevantFields0.getNeedNoFields() == false);
        assertTrue(joinRelevantFields0.getFields().size() == 1);
        assertTrue(joinRelevantFields0.getFields().get(0).first == 0);
        assertTrue(joinRelevantFields0.getFields().get(0).second == 0);
        
        assertTrue(join.getRelevantInputs(0, 0).get(1)==null);
        
        RequiredFields joinRelevantFields1 = join.getRelevantInputs(0, 1).get(0);
        assertTrue(joinRelevantFields1.getNeedAllFields() == false);
        assertTrue(joinRelevantFields1.getNeedNoFields() == false);
        assertTrue(joinRelevantFields1.getFields().size() == 1);
        assertTrue(joinRelevantFields1.getFields().get(0).first == 0);
        assertTrue(joinRelevantFields1.getFields().get(0).second == 1);
        
        assertTrue(join.getRelevantInputs(0, 1).get(1)==null);
        
        RequiredFields joinRelevantFields2 = join.getRelevantInputs(0, 2).get(1);
        assertTrue(joinRelevantFields2.getNeedAllFields() == false);
        assertTrue(joinRelevantFields2.getNeedNoFields() == false);
        assertTrue(joinRelevantFields2.getFields().size() == 1);
        assertTrue(joinRelevantFields2.getFields().get(0).first == 1);
        assertTrue(joinRelevantFields2.getFields().get(0).second == 0);
        
        assertTrue(join.getRelevantInputs(0, 2).get(0)==null);
        
        RequiredFields joinRelevantFields3 = join.getRelevantInputs(0, 3).get(1);
        assertTrue(joinRelevantFields3.getNeedAllFields() == false);
        assertTrue(joinRelevantFields3.getNeedNoFields() == false);
        assertTrue(joinRelevantFields3.getFields().size() == 1);
        assertTrue(joinRelevantFields3.getFields().get(0).first == 1);
        assertTrue(joinRelevantFields3.getFields().get(0).second == 1);
        
        assertTrue(join.getRelevantInputs(0, 3).get(0)==null);
        
        assertTrue(join.getRelevantInputs(0, 4)==null);
    }
    
    @Test
    public void testQueryForeachGenerateStarNoSchema() {
        String query = "foreach (load 'a') generate * ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        assertTrue(foreach.getRelevantInputs(0, 0) == null);
    }
    
    @Test
    public void testQueryForeachGenerateCountStarNoSchema() {
        String query = "foreach (load 'a') generate COUNT(*) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields.getNeedAllFields() == true);
        assertTrue(foreachRelevantFields.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields.getFields() == null);
    }
    
    @Test
    public void testQueryForeachGenerateStarNoSchema1() {
        String query = "foreach (load 'a') generate *, COUNT(*) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        assertTrue(foreach.getRelevantInputs(0, 0) == null);
        assertTrue(foreach.getRelevantInputs(0, 1) == null);
    }
    
    @Test
    public void testQueryForeachGenerateStarNoSchema2() {
        String query = "foreach (load 'a') generate *, $0 ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        assertTrue(foreach.getRelevantInputs(0, 0) == null);
        assertTrue(foreach.getRelevantInputs(0, 1) == null);
    }
    
    @Test
    public void testQueryForeachGenerateStarWithSchema() {
        String query = "foreach (load 'a' as (url, hitCount)) generate * ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields0.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields0.getFields().size() == 1);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields foreachRelevantFields1 = foreach.getRelevantInputs(0, 1).get(0);
        assertTrue(foreachRelevantFields1.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields1.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields1.getFields().size() == 1);
        assertTrue(foreachRelevantFields1.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields1.getFields().get(0).second == 1);
        
        assertTrue(foreach.getRelevantInputs(0, 2)==null);
    }

    @Test
    public void testQueryForeachGenerateCountStarWithSchema() {
        String query = "foreach (load 'a' as (url, hitCount)) generate COUNT(*) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields.getNeedAllFields() == true);
        assertTrue(foreachRelevantFields.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields.getFields() == null);
    }

    @Test
    public void testQueryForeachGenerateStarWithSchema1() {
        String query = "foreach (load 'a' as (url, hitCount)) generate *, COUNT(*) ;";
        LogicalPlan lp = planTester.buildPlan(query);
        
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields0.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields0.getFields().size() == 1);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields foreachRelevantFields1 = foreach.getRelevantInputs(0, 1).get(0);
        assertTrue(foreachRelevantFields1.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields1.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields1.getFields().size() == 1);
        assertTrue(foreachRelevantFields1.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields1.getFields().get(0).second == 1);
        
        RequiredFields foreachRelevantFields2 = foreach.getRelevantInputs(0, 2).get(0);
        assertTrue(foreachRelevantFields2.getNeedAllFields() == true);
        assertTrue(foreachRelevantFields2.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields2.getFields() == null);
    }

    @Test
    public void testQueryForeachGenerateStarWithSchema2() {
        String query = "foreach (load 'a' as (url, hitCount)) generate *, url ;";
        LogicalPlan lp = planTester.buildPlan(query);

        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        RequiredFields foreachRelevantFields0 = foreach.getRelevantInputs(0, 0).get(0);
        assertTrue(foreachRelevantFields0.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields0.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields0.getFields().size() == 1);
        assertTrue(foreachRelevantFields0.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields0.getFields().get(0).second == 0);
        
        RequiredFields foreachRelevantFields1 = foreach.getRelevantInputs(0, 1).get(0);
        assertTrue(foreachRelevantFields1.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields1.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields1.getFields().size() == 1);
        assertTrue(foreachRelevantFields1.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields1.getFields().get(0).second == 1);
        
        RequiredFields foreachRelevantFields2 = foreach.getRelevantInputs(0, 2).get(0);
        assertTrue(foreachRelevantFields2.getNeedAllFields() == false);
        assertTrue(foreachRelevantFields2.getNeedNoFields() == false);
        assertTrue(foreachRelevantFields2.getFields().size() == 1);
        assertTrue(foreachRelevantFields2.getFields().get(0).first == 0);
        assertTrue(foreachRelevantFields2.getFields().get(0).second == 0);
    }
}


