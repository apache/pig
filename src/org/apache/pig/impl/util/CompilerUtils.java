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

package org.apache.pig.impl.util;

import java.util.ArrayList;
import java.util.List;


import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.builtin.IsEmpty;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;

/* 
 * A class to add util functions that gets used by LogToPhyTranslator and MRCompiler
 * 
 */
public class CompilerUtils {

    public static void addEmptyBagOuterJoin(PhysicalPlan fePlan, Schema inputSchema) throws PlanException {
        // we currently have POProject[bag] as the only operator in the plan
        // If the bag is an empty bag, we should replace
        // it with a bag with one tuple with null fields so that when we flatten
        // we do not drop records (flatten will drop records if the bag is left
        // as an empty bag) and actually project nulls for the fields in 
        // the empty bag
        
        // So we need to get to the following state:
        // POProject[Bag]
        //         \     
        //    POUserFunc["IsEmpty()"] Const[Bag](bag with null fields)   
        //                        \      |    POProject[Bag]             
        //                         \     |    /
        //                          POBinCond
        POProject relationProject = (POProject) fePlan.getRoots().get(0);
        try {
            
            // condition of the bincond
            POProject relationProjectForIsEmpty = relationProject.clone();
            fePlan.add(relationProjectForIsEmpty);
            String scope = relationProject.getOperatorKey().scope;
            FuncSpec isEmptySpec = new FuncSpec(IsEmpty.class.getName());
            Object f = PigContext.instantiateFuncFromSpec(isEmptySpec);
            POUserFunc isEmpty = new POUserFunc(new OperatorKey(scope, NodeIdGenerator.getGenerator().
                        getNextNodeId(scope)), -1, null, isEmptySpec, (EvalFunc) f);
            isEmpty.setResultType(DataType.BOOLEAN);
            fePlan.add(isEmpty);
            fePlan.connect(relationProjectForIsEmpty, isEmpty);
            
            // lhs of bincond (const bag with null fields)
            ConstantExpression ce = new ConstantExpression(new OperatorKey(scope,
                    NodeIdGenerator.getGenerator().getNextNodeId(scope)));
            // the following should give a tuple with the
            // required number of nulls
            Tuple t = TupleFactory.getInstance().newTuple(inputSchema.size());
            for(int i = 0; i < inputSchema.size(); i++) {
                t.set(i, null);
            }
            List<Tuple> bagContents = new ArrayList<Tuple>(1);
            bagContents.add(t);
            DataBag bg = new NonSpillableDataBag(bagContents);
            ce.setValue(bg);
            ce.setResultType(DataType.BAG);
            //this operator doesn't have any predecessors
            fePlan.add(ce);
            
            //rhs of bincond is the original project
            // let's set up the bincond now
            POBinCond bincond = new POBinCond(new OperatorKey(scope,
                    NodeIdGenerator.getGenerator().getNextNodeId(scope)));
            bincond.setCond(isEmpty);
            bincond.setLhs(ce);
            bincond.setRhs(relationProject);
            bincond.setResultType(DataType.BAG);
            fePlan.add(bincond);

            fePlan.connect(isEmpty, bincond);
            fePlan.connect(ce, bincond);
            fePlan.connect(relationProject, bincond);

        } catch (Exception e) {
            throw new PlanException("Error setting up outerjoin", e);
        }
    	
    }

}
