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

import java.util.*;

/***
 * This matcher allows matching of different keys but both
 * key sets have to be in the same increasing order
 *
 * Example:
 * Plan1:  0-Load 1-Distinct 2-Split 3-SplitOutput
 *         4-SplitOutput 5-Union
 *
 * Plan2: 10-Load 14-Distinct 15-Split 100-SplitOutput
 *        110-SplitOutput 9999-Union
 *
 *
 * Note: All the keys have to be in the same scope.
 *
 */
public class IncreasingKeyMatcher implements
                    NodeMatcher<Operator,OperatorPlan<Operator>> {

    public Map<OperatorKey, OperatorKey> match(OperatorPlan plan1,
                                               OperatorPlan plan2,
                                               StringBuilder messages) {

        List<OperatorKey> keyList1 = getSortedKeyList(plan1) ;
        List<OperatorKey> keyList2 = getSortedKeyList(plan2) ;

        // This matching logic only works when both plans have
        // the same number of operators
        if (keyList1.size() != keyList2.size()) {
            messages.append("Two plans have different size") ;
            return null ;
        }

        // Populating the output map
        Map<OperatorKey, OperatorKey> outputMap
                                = new HashMap<OperatorKey, OperatorKey>() ;
        for(int i=0; i< keyList1.size() ; i++) {
            outputMap.put(keyList1.get(i), keyList2.get(i)) ;
        }

        return outputMap ;
    }


    ////////// Helper ///////////
    private List<OperatorKey> getSortedKeyList(OperatorPlan plan) {

        List<OperatorKey> keyList
                        = new ArrayList<OperatorKey>(plan.getKeys().keySet()) ;
        Collections.sort(keyList); 
        return keyList ;
    }

}
