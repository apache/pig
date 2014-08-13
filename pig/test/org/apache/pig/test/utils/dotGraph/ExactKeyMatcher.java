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
import java.util.Iterator;
import java.util.HashMap;

/***
 * This matcher only does exact key matching
 */
public class ExactKeyMatcher implements NodeMatcher<Operator,
                                                    OperatorPlan<Operator>> {

    public Map<OperatorKey, OperatorKey> match(OperatorPlan<Operator> plan1,
                                               OperatorPlan<Operator> plan2,
                                               StringBuilder messages) {
        // Find plan1.OperatorSet - plan2.OperatorSet
        int diff1 = diffKeys(plan1, plan2, messages, "plan2") ;

        // Find plan2.OperatorSet - plan1.OperatorSet
        int diff2 = diffKeys(plan2, plan1, messages, "plan1") ;

        // If there is a problem, just finish here
        if ( (diff1 != 0) || (diff2 != 0) ) {
            return null ;
        }

        // if no problem, we just return exact matching
        Iterator<Operator> iter = plan1.getKeys().values().iterator() ;
        Map<OperatorKey, OperatorKey> outputMap
                                = new HashMap<OperatorKey, OperatorKey>() ;
        while(iter.hasNext()) {
            Operator op = iter.next() ;
            outputMap.put(op.getOperatorKey(), op.getOperatorKey()) ;
        }

        return outputMap;
    }

    /***
     * Report plan1.OperatorSet - plan2.OperatorSet
     *
     * @param plan1
     * @param plan2
     * @param messages where the report messages go. null if no messages needed
     * @param plan2Name the name that is used to refer to plan2 in messages
     * @return
     */

    private int diffKeys(OperatorPlan<Operator> plan1,
                         OperatorPlan<Operator> plan2,
                         StringBuilder messages,
                         String plan2Name) {
        int count = 0 ;

        // prepare
        Map<OperatorKey, Operator> keyList = plan1.getKeys() ;
        Iterator<OperatorKey> iter = keyList.keySet().iterator() ;

        // go through the list of vertices of the first plan
        while(iter.hasNext()) {

            OperatorKey key = iter.next() ;

            // if the same key doesn't exist in the second plan
            // we've got a problem
            if (plan2.getOperator(key) == null) {
                Operator op1 = plan1.getOperator(key) ;
                if (messages != null) {
                    messages.append(op1.getClass().getSimpleName()) ;
                    appendOpKey(op1.getOperatorKey(), messages) ;
                    messages.append(" doesn't exist") ;
                    if (plan2Name != null) {
                        messages.append(" in ") ;
                        messages.append(plan2Name) ;
                        messages.append("\n") ;
                    }
                }
                // increment diff counter
                count++ ;
            }
        }

        return count ;
    }

    ////////////// String Formatting Helpers //////////////

    protected void appendOpKey(OperatorKey operatorKey, StringBuilder sb) {
        sb.append("(") ;
        sb.append(operatorKey.toString()) ;
        sb.append(")") ;
    }

}
