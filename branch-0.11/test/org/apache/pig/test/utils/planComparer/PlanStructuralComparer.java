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

package org.apache.pig.test.utils.planComparer;

import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.test.utils.dotGraph.NodeMatcher;
import org.apache.pig.test.utils.dotGraph.IncreasingKeyMatcher;

import java.util.*;

/***
 * This abstract class is a base for plan equality comparer
 */

public abstract class PlanStructuralComparer<E extends Operator,
                                             P extends OperatorPlan<E>> {

    // This maps operator keys from plan#1 to plan#2
    protected Map<OperatorKey, OperatorKey> plan1ToPlan2 = null ;

    // This maps operator keys from plan#2 to plan#1
    protected Map<OperatorKey, OperatorKey> plan2ToPlan1 = null ;

    // This is the default vertex matcher
    private NodeMatcher nodeMatcher = new IncreasingKeyMatcher();

    /***
     * This method does structural comparison of two plans based on:-
     *  - Graph connectivity
     *
     * The current implementation is based on simple key-based
     * vertex matching.
     *
     * @param plan1 the first plan
     * @param plan2 the second plan
     * @param messages where the error messages go
     * @return
     */
    public boolean structurallyEquals(P plan1,
                                      P plan2,
                                      StringBuilder messages) {

        // Stage 1: Match vertices
        plan1ToPlan2 = nodeMatcher.match(plan1, plan2, messages) ;

        // If we don't get all vertices matched here,
        // all following logics are useless
        if (plan1ToPlan2 == null) {
            return false ;
        }

        // initialize the inverse map
        plan2ToPlan1 = generateInverseMap(plan1ToPlan2) ;

        // Stage 2: Match outgoing edges of each vertex

        int diffCount = 0 ;
        Iterator<OperatorKey> keyIter1 = plan1.getKeys().keySet().iterator() ;

        // for each vertex, find diff for edges
        while(keyIter1.hasNext()) {
            OperatorKey opKey = keyIter1.next() ;
            E op1 = plan1.getOperator(opKey) ;
            E op2 = plan2.getOperator(plan1ToPlan2.get(opKey)) ;
            diffCount += diffOutgoingEdges(op1, op2, plan1, plan2,
                                           messages, "plan1", "plan2") ;
        }

        return diffCount==0 ;
    }

    /***
     * Same as above in case just want to compare but
     * don't want to know the error messages
     * @param plan1 the first plan
     * @param plan2 the second plan
     * @return
     */
    public boolean structurallyEquals(P plan1,
                                      P plan2) {
        return structurallyEquals(plan1, plan2, null) ;
    }


    /***
     * This allows different implementation of node matcher
     * @param matcher
     */
    public void setNodeMatcher(NodeMatcher matcher) {
        nodeMatcher = matcher ;
    }


    /***
     * Generate inverse map of the given map
     * @param map
     * @return
     */
    private Map<OperatorKey, OperatorKey> generateInverseMap(
                                           Map<OperatorKey, OperatorKey> map) {
        Map<OperatorKey, OperatorKey> inverseMap
                                 = new HashMap<OperatorKey, OperatorKey>() ;
        Iterator<OperatorKey> iter = map.keySet().iterator() ;
        while(iter.hasNext()) {
            OperatorKey key = iter.next() ;
            inverseMap.put(map.get(key) ,key) ;
        }
        return inverseMap ;
    }


    /***
     * Report operator1.edges - operator2.edges  (Non-commutative)
     *
     * @param operator1
     * @param operator2
     * @param plan1
     * @param plan2
     * @param messages
     * @param plan1Name
     * @param plan2Name
     * @return count(operator1.edges - operator2.edges)
     */
    private int diffOutgoingEdges(E operator1,
                                  E operator2,
                                  P plan1,
                                  P plan2,
                                  StringBuilder messages,
                                  String plan1Name,
                                  String plan2Name) {
        int diffCount = 0 ;

        // Prepare
        List<E> list1 = plan1.getSuccessors(operator1) ;
        List<E> list2 = plan2.getSuccessors(operator2) ;

        // If both of them are leaves, we're done
        if ((list1==null) && (list2==null)) {
            return 0 ;
        }

        // if only operator2 is a leaf
        else if ((list1!=null) && (list2==null)) {
            // record every edge from list1 as missing from plan2
            for(E op: list1) {
                if (messages != null) {
                    appendMissingEdgeMessage(operator1.getOperatorKey(),
                                             op.getOperatorKey(),
                                             messages,
                                             plan2Name);
                }
                diffCount++ ;
            }
            return diffCount ;
        }

        // if only operator1 is a leaf
        else if ((list1==null) && (list2!=null)) {
            for(E op: list2) {
                if (messages != null) {
                    appendMissingEdgeMessage(operator2.getOperatorKey(),
                                             op.getOperatorKey(),
                                             messages,
                                             plan1Name);
                }
                diffCount++ ;
            }
            return diffCount ;
        }

        // Real calculations

         // Matching map
        Map<OperatorKey, Boolean> edgeMap2
                                    = new HashMap<OperatorKey, Boolean>() ;
        // put all the outgoing edges from plan2 in buckets
        for(E op: list2) {
            edgeMap2.put(op.getOperatorKey(), true) ;
        }

        // iterate through outgoing edges from plan1 to check

        for(E op: list1) {
            if (edgeMap2.get(plan1ToPlan2.get(op.getOperatorKey())) == null) {
                if (messages != null) {
                    appendMissingEdgeMessage(operator1.getOperatorKey(),
                                             op.getOperatorKey(),
                                             messages,
                                             plan2Name) ;
                }
                diffCount++ ;
            }
        }


        // Matching map
        Map<OperatorKey, Boolean> edgeMap1
                                    = new HashMap<OperatorKey, Boolean>() ;


        // put all the outgoing edges from plan1 in buckets
        for(E op: list1) {
            edgeMap1.put(op.getOperatorKey(), true) ;
        }

        // iterate through outgoing edges from plan2 to check

        for(E op: list2) {
            if (edgeMap1.get(plan2ToPlan1.get(op.getOperatorKey())) == null) {
                if (messages != null) {
                    appendMissingEdgeMessage(operator2.getOperatorKey(),
                                             op.getOperatorKey(),
                                             messages,
                                             plan1Name) ;
                }
                diffCount++ ;
            }
        }

        return diffCount ;
    }

    ////////////// String Formatting Helpers //////////////

    protected void appendOpKey(OperatorKey operatorKey, StringBuilder sb) {
        sb.append("(") ;
        sb.append(operatorKey.toString()) ;
        sb.append(")") ;
    }

    private void appendMissingEdgeMessage(OperatorKey fromKey,
                                          OperatorKey toKey,
                                          StringBuilder messages,
                                          String planName) {
        messages.append("Edge ") ;
        appendOpKey(fromKey, messages) ;
        messages.append(" -> ") ;
        appendOpKey(toKey, messages) ;
        messages.append(" doesn't exist") ;
        if (planName != null) {
            messages.append(" in ") ;
            messages.append(planName) ;
        }
        messages.append("\n") ;
    }
}