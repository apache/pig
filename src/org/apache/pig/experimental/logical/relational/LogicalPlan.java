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

package org.apache.pig.experimental.logical.relational;

import org.apache.pig.experimental.plan.BaseOperatorPlan;
import org.apache.pig.experimental.plan.OperatorPlan;

/**
 * LogicalPlan is the logical view of relational operations Pig will execute 
 * for a given script.  Note that it contains only relational operations.
 * All expressions will be contained in LogicalExpressionPlans inside
 * each relational operator.
 */
public class LogicalPlan extends BaseOperatorPlan {
    
    /**
     * Equality is checked by calling equals on every leaf in the plan.  This
     * assumes that plans are always connected graphs.  It is somewhat 
     * inefficient since every leaf will test equality all the way to 
     * every root.  But it is only intended for use in testing, so that
     * should be ok.  Checking predecessors (as opposed to successors) was
     * chosen because splits (which have multiple successors) do not depend
     * on order of outputs for correctness, whereas joins (with multiple
     * predecessors) do.  That is, reversing the outputs of split in the
     * graph has no correctness implications, whereas reversing the inputs
     * of join can.  This method of doing equals will detect predecessors
     * in different orders but not successors in different orders.
     */
    @Override
    public boolean isEqual(OperatorPlan other) {
        if (other == null || !(other instanceof LogicalPlan)) {
            return false;
        }
        
        return super.isEqual(other);   
    }
    
}
