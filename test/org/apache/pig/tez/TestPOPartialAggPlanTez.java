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
package org.apache.pig.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerNode;
import org.apache.pig.test.TestPOPartialAggPlan;
import org.junit.Test;

public class TestPOPartialAggPlanTez extends TestPOPartialAggPlan {
    @Test
    public void testNoMapAggProp() throws Exception{
        //test with pig.exec.mapPartAgg not set
        String query = getGByQuery();

        TezPlanContainer tezPlanContainer = TezUtil.buildTezPlanContainer(query, pc);
        assertEquals(tezPlanContainer.size(), 1);

        assertNull("POPartialAgg should be absent",findPOPartialAgg(tezPlanContainer));
    }

    @Test
    public void testMapAggPropFalse() throws Exception{
        //test with pig.exec.mapPartAgg set to false
        String query = getGByQuery();
        pc.getProperties().setProperty(PigConfiguration.PIG_EXEC_MAP_PARTAGG, "false");
        TezPlanContainer tezPlanContainer = TezUtil.buildTezPlanContainer(query, pc);
        assertEquals(tezPlanContainer.size(), 1);

        assertNull("POPartialAgg should be absent", findPOPartialAgg(tezPlanContainer));
    }

    @Test
    public void testMapAggPropTrue() throws Exception{
        //test with pig.exec.mapPartAgg to true
        String query = getGByQuery();
        pc.getProperties().setProperty(PigConfiguration.PIG_EXEC_MAP_PARTAGG, "true");
        TezPlanContainer tezPlanContainer = TezUtil.buildTezPlanContainer(query, pc);
        assertEquals(tezPlanContainer.size(), 1);

        assertNotNull("POPartialAgg should be present",findPOPartialAgg(tezPlanContainer));

    }

    @Test
    public void testMapAggNoAggFunc() throws Exception{
        //no agg func, so there should not be a POPartial
        String query = "l = load 'x' as (a,b,c);" +
                "g = group l by a;" +
                "f = foreach g generate group;";
        pc.getProperties().setProperty(PigConfiguration.PIG_EXEC_MAP_PARTAGG, "true");
        TezPlanContainer tezPlanContainer = TezUtil.buildTezPlanContainer(query, pc);
        assertEquals(tezPlanContainer.size(), 1);

        assertNull("POPartialAgg should be absent",findPOPartialAgg(tezPlanContainer));
    }

    @Test
    public void testMapAggNotCombinable() throws Exception{
        //not combinable, so there should not be a POPartial
        String query = "l = load 'x' as (a,b,c);" +
                "g = group l by a;" +
                "f = foreach g generate group, COUNT(l.b), l.b;";
        pc.getProperties().setProperty(PigConfiguration.PIG_EXEC_MAP_PARTAGG, "true");
        TezPlanContainer tezPlanContainer = TezUtil.buildTezPlanContainer(query, pc);
        assertEquals(tezPlanContainer.size(), 1);

        assertNull("POPartialAgg should be absent", findPOPartialAgg(tezPlanContainer));
    }

    private PhysicalOperator findPOPartialAgg(TezPlanContainer tezPlanContainer) {
        for (TezPlanContainerNode node : tezPlanContainer) {
            TezOperPlan tezPlan = node.getTezOperPlan();
            for (TezOperator tezOper : tezPlan) {
                PhysicalOperator partialAgg = findPOPartialAgg(tezOper.plan);
                if (partialAgg != null) {
                    return partialAgg;
                }
            }
        }
        return null;
    }
}
