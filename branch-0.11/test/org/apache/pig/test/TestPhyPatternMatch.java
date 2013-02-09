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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PatternNode;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PatternPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.junit.Before;
import org.junit.Test;


public class TestPhyPatternMatch {
  
    int opKeyNum = 0;
    
    @Before
    public void setUp() throws Exception {
        opKeyNum = 0;
    }
    
    
    @Test
    public void testSingleNodePattern() throws PlanException{
        
        //create pattern 
        PatternPlan ptPlan = new PatternPlan();
        PatternNode ptFilNode = new PatternNode(ptPlan);
        ptFilNode.setClassName(POFilter.class);
        ptPlan.add(ptFilNode);
                
        //create plan with single ForEach node
        PhysicalPlan pplan = new PhysicalPlan();
        POForEach fe = new POForEach(getNewOpKey()); 
        pplan.add(fe);

        // verify that match is false
        boolean matched = ptPlan.match(pplan);
        assertFalse("plan not matched", matched);

        
        //add a filter to the plan (fe -> fil)
        POFilter fil = new POFilter(getNewOpKey()); 
        pplan.add(fil);
        pplan.connect(fe, fil);
        
        //verify that pattern matches
        matched = ptPlan.match(pplan);
        assertTrue("plan matched", matched);
        assertEquals(" class matched ", ptFilNode.getMatch(), fil);
        
        //test leaf/source settings in pattern node
        ptFilNode.setSourceNode(true);
        assertFalse("plan matched", ptPlan.match(pplan));
        
        ptFilNode.setSourceNode(false);
        ptFilNode.setLeafNode(true);
        assertTrue("plan matched", ptPlan.match(pplan));
        
        
        
    }

    @Test
    public void testTwoNodePattern() throws PlanException{
        //create pattern (foreach -> filter)
        Class<?>[] nodes = {POForEach.class, POFilter.class};
        PatternPlan ptPlan = PatternPlan.create(nodes);
        PatternNode ptFilNode = (PatternNode) ptPlan.getSinks().get(0);
        PatternNode ptFENode = (PatternNode) ptPlan.getSources().get(0);

        
        //create plan with single Filter node
        PhysicalPlan pplan = new PhysicalPlan();
        POFilter fil = new POFilter(getNewOpKey()); 
        pplan.add(fil);

        // verify that match is false
        assertFalse("plan not matched", ptPlan.match(pplan));
        
        //verify that there is no match in the pattern nodes
        assertEquals("null match", ptFilNode.getMatch(), null);
        assertEquals("null match", ptFENode.getMatch(), null);

        //add a foreach to the plan (fe -> fil)
        POForEach fe = new POForEach(getNewOpKey()); 
        pplan.add(fe);
        pplan.connect(fe, fil);

        // verify that match is true
        assertTrue("plan matched", ptPlan.match(pplan));

        // set leaf and source properties and try again
        ptFilNode.setLeafNode(true);
        ptFENode.setSourceNode(true);
        assertTrue("plan matched", ptPlan.match(pplan));        
        
        //add a store to the plan to make it (fe -> fil -> store)
        POStore store = new POStore(getNewOpKey());
        pplan.add(store);
        pplan.connect(fil, store);
        
        // match should fail because filter pt node leaf property is set
        assertFalse("plan matched", ptPlan.match(pplan));
        //reset patter filter node leaf property
        ptFilNode.setLeafNode(false);
        
        
        // verify that match is true
        assertTrue("plan matched", ptPlan.match(pplan));
        assertEquals("filter pt node match", ptFilNode.getMatch(), fil);
        assertEquals("foreach pt node match", ptFENode.getMatch(), fe);
               
        
        //add a store to the plan to make it (fe -> fe -> fil -> store)
        POForEach fe2 = new POForEach(getNewOpKey());
        pplan.add(fe2);
        pplan.connect(fe2, fe);
        
        // match fails because fe pattern node is set to be source
        assertFalse("plan matched", ptPlan.match(pplan));
        ptFENode.setSourceNode(false);
        
        // verify that match is true
        assertTrue("plan matched", ptPlan.match(pplan));
        assertEquals("filter pt node match", ptFilNode.getMatch(), fil);
        assertEquals("foreach pt node match", ptFENode.getMatch(), fe);
        
        //create new plan (fil -> fe)
        PhysicalPlan pplan2 = new PhysicalPlan();
        POFilter fil2 = new POFilter(getNewOpKey()); 
        pplan.add(fil2);
        POForEach fe21 = new POForEach(getNewOpKey()); 
        pplan.add(fe21);
        pplan.connect(fil2, fe21);

        //verify that plan does not match
        assertFalse("plan not matched", ptPlan.match(pplan2));
        assertEquals("null match", ptFilNode.getMatch(), null);
        assertEquals("null match", ptFENode.getMatch(), null);     
        
    }
    
    
    @Test
    public void testThreeNodePatternLinear() throws PlanException{
        
        //create pattern (fil -> FE -> store) 
        PatternPlan ptPlan = new PatternPlan();
        PatternNode ptFilNode = createPtNode(ptPlan, POFilter.class);
        PatternNode ptFENode = createPtNode(ptPlan, POForEach.class);
        PatternNode ptStNode = createPtNode(ptPlan, POStore.class);

        ptPlan.connect(ptFilNode, ptFENode);
        ptPlan.connect(ptFENode, ptStNode);
        
        //create plan fil -> fil -> fe -> store
        PhysicalPlan pplan = new PhysicalPlan();
        POFilter fil = new POFilter(getNewOpKey()); 
        pplan.add(fil);
        assertFalse("plan not matched", ptPlan.match(pplan));
        assertEquals("null match", ptFilNode.getMatch(), null);
        assertEquals("null match", ptFENode.getMatch(), null);
        assertEquals("null match", ptStNode.getMatch(), null);
        
        POForEach fe = new POForEach(getNewOpKey()); 
        pplan.add(fe);
        pplan.connect(fil, fe);
        assertFalse("plan not matched", ptPlan.match(pplan));
        
        POFilter fil2 = new POFilter(getNewOpKey()); 
        pplan.add(fil2);
        pplan.connect(fil2, fil);
        assertFalse("plan not matched", ptPlan.match(pplan));
        
        POStore store = new POStore(getNewOpKey()); 
        pplan.add(store);
        pplan.connect(fe, store);
        assertTrue("plan matched", ptPlan.match(pplan));
        
        assertEquals("test match node", ptFilNode.getMatch(), fil);
        assertEquals("test match node", ptFENode.getMatch(), fe);
        assertEquals("test match node", ptStNode.getMatch(), store);
        
        
        
    }
    
    
    @Test
    public void testThreeNodePatternTwoParents() throws PlanException, ExecException{
        //create pattern fe   fil
        //                \   /
        //                frJoin
        PatternPlan ptPlan = new PatternPlan();
        PatternNode ptFilNode = createPtNode(ptPlan, POFilter.class);
        PatternNode ptFENode = createPtNode(ptPlan, POForEach.class);
        PatternNode ptJoinNode = createPtNode(ptPlan, POFRJoin.class);

        ptPlan.connect(ptFilNode, ptJoinNode);
        ptPlan.connect(ptFENode, ptJoinNode);
        
        //create plan 
        PhysicalPlan pplan = new PhysicalPlan();
        POFilter fil = new POFilter(getNewOpKey()); 
        pplan.add(fil);
        assertFalse("plan not matched", ptPlan.match(pplan));
        
        POForEach fe = new POForEach(getNewOpKey()); 
        pplan.add(fe);
        assertFalse("plan not matched", ptPlan.match(pplan));
        
        POFRJoin join = new POFRJoin(getNewOpKey(), 0, null,
                new ArrayList<List<PhysicalPlan>>(), null, null, 0, false,null); 
        pplan.add(join);
        pplan.connect(fil, join);
        pplan.connect(fe, join);
        assertTrue("plan matched", ptPlan.match(pplan));
        assertEquals("test match node", ptFilNode.getMatch(), fil);
        assertEquals("test match node", ptFENode.getMatch(), fe);
        assertEquals("test match node", ptJoinNode.getMatch(), join);
        
    }
    
    
    private PatternNode createPtNode(PatternPlan ptPlan, Class<?> ptClass) {
        PatternNode ptNode = new PatternNode(ptPlan);
        ptNode.setClassName(ptClass);
        ptPlan.add(ptNode);
        return ptNode;
        
    }


    private OperatorKey getNewOpKey() {
        return new OperatorKey("", ++opKeyNum);
    }
    
    
}
