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

package org.apache.pig.impl.plan.optimizer;

import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Parent for all Logical operators.
 */
public class RuleOperator extends Operator<RulePlanVisitor> {
    private static final long serialVersionUID = 2L;

    private static Log log = LogFactory.getLog(RuleOperator.class);
    
    public enum NodeType {
        ANY_NODE,
        SIMPLE_NODE,
        MULTI_NODE,
        COMMON_NODE;
    }

    private Class mNodeClass;
    private NodeType mNodeType = NodeType.SIMPLE_NODE;
    
    /**
     * @param clazz
     *            Class type of this node, e.g.: LOFilter.class
     * @param k
     *            Operator key to assign to this node.
     */
    public RuleOperator(Class clazz, OperatorKey k) {
        super(k);
        mNodeClass = clazz;
    }

    /**
     * @param clazz
     *            Class type of this node, e.g.: LOFilter.class
     * @param nodeType
     *            Node type of this node             
     * @param k
     *            Operator key to assign to this node.
     */
    public RuleOperator(Class clazz, NodeType nodeType, OperatorKey k) {
        super(k);
        mNodeType = nodeType;
        mNodeClass = clazz;
    }
    
    /**
     * Set the node type of this rule operator.
     * 
     * @param type 
     *            Node type to set this operator to.
     */
    final public void setNodeType(NodeType type) {
        mNodeType = type;
    }

    /**
     * Get the node type of this operator.
     */
    public NodeType getNodeType() {
        return mNodeType;
    }

    /**
     * Get the node class of this operator.
     */
    public Class getNodeClass() {
        return mNodeClass;
    }

    @Override
    public String toString() {
        return name();
    }

    /**
     * Visit this node with the provided visitor. This should only be called by
     * the visitor class itself, never directly.
     * 
     * @param v
     *            Visitor to visit with.
     * @throws VisitException
     *             if the visitor has a problem.
     */
    public void visit(RulePlanVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    /**
     * @see org.apache.pig.impl.plan.Operator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        RuleOperator ruleOpClone = (RuleOperator)super.clone();
        return ruleOpClone;
    }

    @Override
    public String name() {
        StringBuffer msg = new StringBuffer();
        msg.append("(Name: " + mNodeClass.getSimpleName() + " Node Type: " + mNodeType + "[" + this.mKey + "]" + ")");
        return msg.toString();
    }

}
