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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class DepthFirstMemoryWalker extends DepthFirstWalker {
    
    private int level = 0;
    private int startingLevel = 0;
    private Stack<String> prefixStack;
    private String currentPrefix = "";
    
    public DepthFirstMemoryWalker(OperatorPlan plan, int startingLevel) {
        super(plan);
        level = startingLevel;
        this.startingLevel = startingLevel;
        prefixStack = new Stack<String>();
    }

    @Override
    public PlanWalker spawnChildWalker(OperatorPlan plan) {
        return new DepthFirstMemoryWalker(plan, level);
    }

    /**
     * Begin traversing the graph.
     * @param visitor Visitor this walker is being used by.
     * @throws IOException if an error is encountered while walking.
     */
    @Override
    public void walk(PlanVisitor visitor) throws IOException {
        List<Operator> roots = plan.getSources();
        Set<Operator> seen = new HashSet<Operator>();

        depthFirst(null, roots, seen, visitor);
    }
    
    public String getPrefix() {
        return currentPrefix;
    }

    private void depthFirst(Operator node,
                            Collection<Operator> successors,
                            Set<Operator> seen,
                            PlanVisitor visitor) throws IOException {
        if (successors == null) return;
        
        StringBuilder strb = new StringBuilder(); 
        for(int i = 0; i < startingLevel; i++ ) {
            strb.append("|\t");
        }
        if( ((level-1) - startingLevel ) >= 0 )
            strb.append("\t");
        for(int i = 0; i < ((level-1) - startingLevel ); i++ ) {
            strb.append("|\t");
        }
        strb.append( "|\n" );
        for(int i = 0; i < startingLevel; i++ ) {
            strb.append("|\t");
        }
        if( ((level-1) - startingLevel ) >= 0 )
            strb.append("\t");
        for(int i = 0; i < ((level-1) - startingLevel ); i++ ) {
            strb.append("|\t");
        }
        strb.append("|---");
        currentPrefix = strb.toString();

        for (Operator suc : successors) {
            if (seen.add(suc)) {
                suc.accept(visitor);
                Collection<Operator> newSuccessors = plan.getSuccessors(suc);
                level++;
                prefixStack.push(currentPrefix);
                depthFirst(suc, newSuccessors, seen, visitor);
                level--;
                currentPrefix = prefixStack.pop();
            }
        }
    }
}