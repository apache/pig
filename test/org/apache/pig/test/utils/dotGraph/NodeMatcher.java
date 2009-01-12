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

package org.apache.pig.test.utils.dotGraph;

import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.Operator;

import java.util.Map;

/***
 * This is a common interface for graph vertex mapping logic.
 * Though I call it Matcher so that people don't get confused
 * with mapper in MapReduce.
 */

public interface NodeMatcher<E extends Operator,
                             P extends OperatorPlan<E>> {

    /***
     * This method does matching between vertices in two
     * given plans.
     *
     * @param plan1
     * @param plan2
     * @param messages
     * @return The output map: plan1Key -> plan2Key
     */
    Map<OperatorKey, OperatorKey> match(P plan1,
                                        P plan2,
                                        StringBuilder messages) ;
}
