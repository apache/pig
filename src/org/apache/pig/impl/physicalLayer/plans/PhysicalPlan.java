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
package org.apache.pig.impl.physicalLayer.plans;

import java.io.OutputStream;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;

/**
 * 
 * The base class for all types of physical plans. 
 * This extends the Operator Plan.
 *
 * @param <E>
 */
public class PhysicalPlan<E extends PhysicalOperator> extends OperatorPlan<E> {

    public PhysicalPlan() {
        super();
    }
    
    public void attachInput(Tuple t){
        List<E> roots = getRoots();
        for (E operator : roots)
            operator.attachInput(t);
    }
    
    /**
     * Write a visual representation of the Physical Plan
     * into the given output stream
     * @param out : OutputStream to which the visual representation is written
     */
    public void explain(OutputStream out){
        //Use a plan visitor and output the current physical
        //plan into out
    }
    
    @Override
    public void connect(E from, E to)
            throws PlanException {
        super.connect(from, to);
        to.setInputs(getPredecessors(to));
    }
}
