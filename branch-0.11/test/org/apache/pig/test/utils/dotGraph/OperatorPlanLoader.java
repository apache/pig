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

import org.apache.pig.impl.plan.*;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.HashMap;

public abstract class OperatorPlanLoader<E extends Operator,
                                         P extends OperatorPlan<E>> {

    /***
     * This method is used for loading an operator plan encoded in Dot format
     * @param dotContent the dot content
     * @param clazz the plan type to be created
     * @return
     */
    public P load(String dotContent, Class<P> clazz) {
        DotGraphReader dotReader = new DotGraphReader() ;
        DotGraph graph = dotReader.load(dotContent) ;
        return constructPlan(graph, clazz) ;
    }

    /***
     * Convenient method for loading directly from file
     * @param file
     * @param clazz
     * @return
     */
    public P loadFromFile(String file, Class<P> clazz) {
        DotGraphReader dotReader = new DotGraphReader() ;
        DotGraph graph = dotReader.loadFromFile(file) ;
        return constructPlan(graph, clazz) ;
    }

    public PlanAndGraphEntry loadFromFileWithGraph(String file,
                                                        Class<P> clazz) {
        DotGraphReader dotReader = new DotGraphReader() ;
        DotGraph graph = dotReader.loadFromFile(file) ;
        P plan = constructPlan(graph, clazz) ;
        return new PlanAndGraphEntry(plan, graph) ;
    }

    public class PlanAndGraphEntry {
        public PlanAndGraphEntry(P plan, DotGraph dotGraph) {
            this.plan = plan ;
            this.dotGraph = dotGraph ;
        }
        public P plan ;
        public DotGraph dotGraph ;
    }

    /***
     * This method has be overridden to instantiate the correct vertex type
     *
     * @param node
     * @param plan
     * @return
     */
    protected abstract E createOperator(DotNode node, P plan) ;

    //////////////// Helpers //////////////////

    /***
     * Construct the plan based on the given Dot graph
     * 
     * @param graph
     * @return
     */

    P constructPlan(DotGraph graph, Class<P> clazz) {

        P plan ;
        Map<String, E> nameMap = new HashMap<String, E>() ;

        try {
            plan = clazz.newInstance() ;
        }
        catch (IllegalAccessException iae) {
            throw new AssertionError("Cannot instantiate a plan") ;
        }
        catch (InstantiationException ie) {
            throw new AssertionError("Cannot instantiate a plan") ;
        }

        for(DotNode node: graph.nodes) {
            E op = createOperator(node, plan) ;
            nameMap.put(node.name, op) ;
            plan.add(op);
        }

        for(DotEdge edge: graph.edges) {
            E fromOp = nameMap.get(edge.fromNode)  ;
            E toOp = nameMap.get(edge.toNode)  ;
            try {
                plan.connect(fromOp, toOp);
            }
            catch (PlanException pe) {
                throw new RuntimeException("Invalid Dot file") ;
            }
        }

        return plan ;
    }


    /***
     * Helper for retrieving operator key from encoded attributes.
     * By default, it will look for "key" in attributes.
     * If no key is found, an arbitrary one will be generated.
     * @param attributes
     * @return
     */
    protected OperatorKey getKey(Map<String,String> attributes) {
        String key = attributes.get("key") ;
        if (key != null) {
            return new OperatorKey("scope", Long.parseLong(key)) ;
        }
        else {
            long newId = NodeIdGenerator.getGenerator().getNextNodeId("scope") ;
            return new OperatorKey("scope", newId) ;
        }
    }

}
