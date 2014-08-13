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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.newplan.BaseOperatorPlan;


/**
 * Used for finding/representing a pattern in the plan
 * This class represents the pattern
 * Finds only a single matching pattern
 * This is finding a sub-graph( represented by pattern) in the graph(plan)
 */
public class PatternPlan extends BaseOperatorPlan{

    // this is used to keep track of any nodes whose match is to be reset
    // when we backtrack after finding mismatch
    ArrayList<PatternNode> ptNodesVisited = new ArrayList<PatternNode>();
    OperatorPlan currentPlan = null;

    /**
     * Return true if the given plan has nodes that match the pattern 
     * represented by this class
     * If a match is found, the PatterNodes in the plan will return non 
     * null node for getMatch(). 
     * @param inpPlan - input plan to match
     * @return true if match is found
     */
    public boolean match(OperatorPlan<? extends Operator<?>> inpPlan){
        reset();

        PatternPlan pattern = this;
        currentPlan = inpPlan;

        if(pattern.size() == 0){
            return true;
        }

        PatternNode ptNode = (PatternNode) pattern.getSinks().get(0);
        //try matching the pattern with the plan, starting with ptNode
        Iterator it = currentPlan.iterator();
        while(it.hasNext()){
            Operator<?> plOp = (Operator<?>) it.next();
            if(match(ptNode, plOp)){
                if(this.size() != ptNodesVisited.size()){
                    //BUG
                    throw new RuntimeException("invalid size of pattern nodes visited");
                }

                return true;
            }

        }
        return false;
    }

    /**
     * Reset the matching information if the pattern has been used to find 
     * a match
     */
    void reset(){
        Iterator<org.apache.pig.newplan.Operator> iter =  this.getOperators();
        while(iter.hasNext()){
            PatternNode ptNode = (PatternNode) iter.next();
            ptNode.setMatch(null);
        }
        ptNodesVisited.clear(); 

    }

    /**
     * Check if the pattern node ptNode matches given Operator plOp
     * @param ptNode
     * @param plOp
     * @return
     */
    // to suppress warnings from currentPlan.getPredecessors and 
    // getSuccessors. 
    @SuppressWarnings("unchecked") 
    private boolean match(PatternNode ptNode, Operator plOp) {
        if(ptNode.getMatch() != null && ptNode.getMatch() == plOp){
            return true;
        }

        Class<?> ptClass = ptNode.getClassName();
        Class<?> plClass = plOp.getClass();
        //        if(!ptClass.getClass().isInstance(plOp)){
        //            return false;
        //        }
        if(ptClass != plClass){
            return false;
        }

        if(ptNode.isLeafNode()){
            //pattern node requires matching plan node to be a sink/leaf
            if(currentPlan.getSuccessors(plOp) != null 
                    && currentPlan.getSuccessors(plOp).size() > 0){
                return false;
            }
        }

        if(ptNode.isSourceNode()){
            //pattern node requires matching plan node to be a source/root
            if(currentPlan.getPredecessors(plOp) != null 
                    && currentPlan.getPredecessors(plOp).size() > 0){
                return false;
            }
        }
        //set this as match for now, it also indicates that this node is expected
        // to match this operator while traversing other nodes
        ptNode.setMatch(plOp);
        int ptNodesVisitedIdx = ptNodesVisited.size();
        ptNodesVisited.add(ptNode);

        //try matching predecessors of this pattern node with the plan predecessors
        List<org.apache.pig.newplan.Operator> ptPreds = this.getPredecessors(ptNode);
        List<Operator<?>> plPreds = currentPlan.getPredecessors(plOp);
        if(! match(ptPreds, plPreds)){
            resetNewlyMatchedPtNodes(ptNodesVisitedIdx);
            return false;
        }

        //try matching successors of this pattern node with the plan successors
        List<org.apache.pig.newplan.Operator> ptSuccs = this.getSuccessors(ptNode);
        List<Operator<?>> plSuccs = currentPlan.getSuccessors(plOp);
        if(! match(ptSuccs, plSuccs)){
            resetNewlyMatchedPtNodes(ptNodesVisitedIdx);
            return false;
        }

        return true;
    }

    private void resetNewlyMatchedPtNodes(int ptNodesVisitedIdx) {
        for(int i=ptNodesVisited.size() - 1; i >= ptNodesVisitedIdx; i--){
            ptNodesVisited.get(i).setMatch(null);
            ptNodesVisited.remove(i);
        }
    }


    /**
     * try matching list of pattern nodes with list of plan nodes . these are
     * either predecessors or successors of a matching node
     * if pattern nodes is a ordered subset of plan nodes, return true
     * @param ptList list of pattern nodes
     * @param plList list of plan nodes
     * @return true if matched
     */
    private boolean match(List<org.apache.pig.newplan.Operator> ptList,
            List<Operator<?>> plList) {

        if(ptList == null || ptList.size() == 0){
            return true;
        }
        if(plList == null){
            return false;
        }

        // pattern list has to be smaller than plan list , as it is going
        // to be a subset
        if(ptList.size() > plList.size()){
            return false;
        }

        int plStart = 0;
        int ptIdx = 0;

        // while there are sufficient nodes in list to be matched with list
        // in pattern
        while( (plList.size() - plStart) >=  ptList.size()){
            // try matching the plan list with pattern list starting from
            // plStart
            for(int i = plStart; i < plList.size(); i++){
                Operator<?> plNode = plList.get(i);
                PatternNode ptNode = (PatternNode) ptList.get(ptIdx);
                if(ptNode.getMatch() != null){
                    if(plNode != ptNode.getMatch()){
                        // an already matched node has to match same node again
                        // if not start comparing again from next plan position
                        ptIdx = 0;
                        plStart++;
                        break;                        
                    }else{
                        //already matched
                        ptIdx++;
                        if(ptIdx == ptList.size()){
                            //matched the patter nodes list
                            return true;
                        }
                    }
                }
                else if(!match(ptNode, plNode)){
                    //not matched, start comparing again from next plan position
                    ptIdx = 0;
                    plStart++;
                    break;
                }else{
                    //matched
                    ptIdx++;
                    if(ptIdx == ptList.size()){
                        //matched the patter nodes list
                        return true;
                    }
                }
            }
        }

        return false;

    }

    /**
     * This function can be used to create a new PatternPlan if the pattern
     * nodes have at most one parent/child, and they are connected to each other.
     * The PatternNode corresponding to the i'th class in classList will be
     * the predecessor of the one corresponding to i+1'th class.
     * @param classList
     * @return new PatterPlan corresponding to classList
     */
    public static PatternPlan create(Class<?>[] classList) {
        PatternPlan ptPlan = new PatternPlan();
        PatternNode prevNode = null;
        for(Class<?> ptClass : classList){
            PatternNode ptNode = new PatternNode(ptPlan);
            ptNode.setClassName(ptClass);
            ptPlan.add(ptNode);
            if(prevNode != null){
                ptPlan.connect(prevNode, ptNode);
            }
            prevNode = ptNode;
        }
        return ptPlan;
    }

}
