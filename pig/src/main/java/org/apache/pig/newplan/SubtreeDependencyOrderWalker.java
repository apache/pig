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

package org.apache.pig.newplan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;

public class SubtreeDependencyOrderWalker extends DependencyOrderWalker {
    private Operator startNode;
    
    public SubtreeDependencyOrderWalker(OperatorPlan plan) {
        super(plan);            
    }
    
    public SubtreeDependencyOrderWalker(OperatorPlan plan, Operator startNode) {
        super(plan);            
        this.startNode = startNode;
    }
    
    public void walk(PlanVisitor visitor) throws FrontendException {          
        List<Operator> fifo = new ArrayList<Operator>();
        Set<Operator> seen = new HashSet<Operator>();

        // get all predecessors of startNode
        doAllPredecessors(startNode, seen, fifo);           

        for (Operator op: fifo) {
            op.accept(visitor);
        }
    }
}
