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

import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPlanGeneration extends junit.framework.TestCase {
    
    PigContext pc;
    
    @Override
    @BeforeClass
    public void setUp() throws ExecException {
        pc = new PigContext(ExecType.LOCAL, new Properties());
        pc.connect();
    }
    
    @Test
    public void testStoreAlias() throws Exception  {
        String query = "A = load 'data' as (a0, a1);" +
            "B = filter A by a0 > 1;" +
            "store B into 'output';";
        
        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        assert(loStore.getAlias().equals("B"));
        
        PhysicalPlan pp = Util.buildPhysicalPlanFromNewLP(lp, pc);
        POStore poStore = (POStore)pp.getLeaves().get(0);
        assert(poStore.getAlias().equals("B"));
        
        MROperPlan mrp = Util.buildMRPlanWithOptimizer(pp, pc);
        MapReduceOper mrOper = mrp.getLeaves().get(0);
        poStore = (POStore)mrOper.mapPlan.getLeaves().get(0);
        assert(poStore.getAlias().equals("B"));
    }
}
