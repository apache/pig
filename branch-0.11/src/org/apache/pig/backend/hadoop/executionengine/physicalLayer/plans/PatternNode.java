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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;


/**
 * Used for finding/representing a pattern in the plan
 * This class represents a node in the pattern
 */
public class PatternNode extends Operator{


    public PatternNode(OperatorPlan p) {
        super("pattern", p);
    }


    private static final long serialVersionUID = 1L;

    /**
     * Does this node have to be the leaf node
     */
    private boolean isLeafNode = false;

    /**
     * Does this node have to be the source node
     */
    private boolean isSourceNode = false;

//    /**
//     * Is this node optional
//     */
//    private boolean isOptional;

    /**
     * The class this node in plan should be an instance of
     */
    private Class<?> className;



    /**
     * The node in plan that this pattern node matched 
     */
    private Object match;


    /**
     * @return the isLeafNode
     */
    public boolean isLeafNode() {
        return isLeafNode;
    }


    /**
     * Set isLeafNode to true if the node must be a source
     * @param isLeafNode 
     */
    public void setLeafNode(boolean isLeafNode) {
        this.isLeafNode = isLeafNode;
    }


    /**
     * @return the isSourceNode
     */
    public boolean isSourceNode() {
        return isSourceNode;
    }


    /**
     * Set isSourceNode to true if the node must be a source
     * @param isSourceNode 
     */
    public void setSourceNode(boolean isSourceNode) {
        this.isSourceNode = isSourceNode;
    }

    /**
     * @return the className
     */
    public Class<?> getClassName() {
        return className;
    }


    /**
     * @param className the className to set
     */
    public void setClassName(Class<?> className) {
        this.className = className;
    }


    /**
     * @return the match
     */
    public Object getMatch() {
        return match;
    }


    /**
     * @param match the match to set
     */
    public void setMatch(Object match) {
        this.match = match;
    }


    @Override
    public void accept(org.apache.pig.newplan.PlanVisitor v)
            throws FrontendException {
        //should not be called
        throw new RuntimeException("function not implemented");
    }


    @Override
    public boolean isEqual(Operator operator) throws FrontendException {
        //dummy
        return false;
    }

    @Override
    public String toString(){
        String str ="";
        if(className != null){
            str = className.toString();
        }
        return str;
    }




}
