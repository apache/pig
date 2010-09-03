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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.optimizer.ScalarFinder;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

/**
 * Dependency walker that walks logical plan, it picks the leafs based 
 * on scalar-alias dependency order
 */
public class DependencyOrderWalkerLPScalar extends DependencyOrderWalker {
    
    public DependencyOrderWalkerLPScalar(OperatorPlan plan) {
        super(plan);
    }


    @Override
    protected List<Operator> getSinks() throws FrontendException{
        //find list of scalars and the LogicalRelationalOperator that has them
        ScalarFinder sFinder = new ScalarFinder(plan);
        sFinder.visit();
        HashMap<UserFuncExpression, LogicalRelationalOperator> scalars =
            sFinder.getScalarLOMap();
        
        //create a plan with only the sinks from original plan
        // that maps dependency between sink nodes based on 
        // scalar dependency
        LogicalPlan sinksPlan = new LogicalPlan();
        for(Map.Entry<UserFuncExpression, LogicalRelationalOperator> e : scalars.entrySet()){
            List<LogicalRelationalOperator> scalarSinks =
                new ArrayList<LogicalRelationalOperator>();
            getSinks(e.getValue(), scalarSinks);
            
            List<LogicalRelationalOperator> sourceSinks =
                new ArrayList<LogicalRelationalOperator>();
            getSinks(e.getKey().getImplicitReferencedOperator(), sourceSinks);
            
            for(LogicalRelationalOperator scalarSink : scalarSinks){
                for(LogicalRelationalOperator sourceSink : sourceSinks ){
                    //if the link already exists, don't add again
                    if(sinksPlan.getSuccessors(sourceSink) != null
                            && sinksPlan.getSuccessors(sourceSink).contains(scalarSink)
                    ){
                        continue;
                    }
                    // add the relationship - scalarSink depends on sourceSink
                    sinksPlan.add(sourceSink);
                    sinksPlan.add(scalarSink);
                    sinksPlan.connect(sourceSink, scalarSink);
                }
            }
            
        }
        
        //list of sink nodes ordered by the scalar dependency order
        ArrayList<Operator> orderedSinkNodes =
            new ArrayList<Operator>();

        //keep track of sink nodes that have been added so far
        Set<Operator> sinkNodesAdded = new HashSet<Operator>();
        
        
        //use the plan to get sink operators out first
        List<Operator> sources;
        do{
            sources = new ArrayList<Operator>(sinksPlan.getSources());
            for(Operator source : sources){
                // add the current sources to list
                // and remove them from plan to get new sources
                orderedSinkNodes.add(source);
                sinkNodesAdded.add(source);
                if(sinksPlan.getSuccessors(source) != null){
                    //disconnect before removing
                    List<Operator>succs = new ArrayList<Operator>(sinksPlan.getSuccessors(source));
                    for(Operator succ : succs){
                        sinksPlan.disconnect(source, succ);
                    }
                }
                sinksPlan.remove(source);
            }
        }
        while(sources.size() > 0);

        //add remaining sink nodes from original plan
        List<Operator> allSinks = plan.getSinks();
        for(Operator sink : allSinks){
            if(!sinkNodesAdded.contains(sink)){
                orderedSinkNodes.add(sink);
            }
        }
        
        return orderedSinkNodes;
    }


    /**
     * get all sinks that are successor of LogicalRelationalOperator lop
     * and add them to scalarSinks
     * @param lop LogicalRelationalOperator
     * @param scalarSinks
     */
    private void getSinks(LogicalRelationalOperator lop, 
            List<LogicalRelationalOperator> scalarSinks) {
        
        List<Operator> succs = lop.getPlan().getSuccessors(lop);
        if(succs == null){
            // no successors, this is a sink
            scalarSinks.add(lop);
            return;
        }
        for(Operator op : succs){
            getSinks((LogicalRelationalOperator)op, scalarSinks);
        }
        
    }

    
}
