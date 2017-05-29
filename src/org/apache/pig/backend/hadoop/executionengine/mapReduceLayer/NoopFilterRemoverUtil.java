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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.impl.plan.PlanException;

import java.util.List;

public class NoopFilterRemoverUtil {
    private static Log log = LogFactory.getLog(NoopFilterRemoverUtil.class);

    public static void removeFilter(POFilter filter, PhysicalPlan plan) {
        if (plan.size() > 1) {
            try {
                List<PhysicalOperator> fInputs = filter.getInputs();
                List<PhysicalOperator> sucs = plan.getSuccessors(filter);

                plan.removeAndReconnect(filter);
                if(sucs!=null && sucs.size()!=0){
                    for (PhysicalOperator suc : sucs) {
                        suc.setInputs(fInputs);
                    }
                }
            } catch (PlanException pe) {
                log.info("Couldn't remove a filter in optimizer: "+pe.getMessage());
            }
        }
    }
}
