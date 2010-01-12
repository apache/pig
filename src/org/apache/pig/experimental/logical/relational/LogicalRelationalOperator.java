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

import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;

/**
 * Logical representation of relational operators.  Relational operators have
 * a schema.
 */
abstract public class LogicalRelationalOperator extends Operator {
    
    protected LogicalSchema schema;
    int requestedParallelism;

    /**
     * 
     * @param name of this operator
     * @param plan this operator is in
     */
    public LogicalRelationalOperator(String name, OperatorPlan plan) {
        this(name, plan, -1);
    }
    
    /**
     * 
     * @param name of this operator
     * @param plan this operator is in
     * @param rp requested parallelism
     */
    public LogicalRelationalOperator(String name,
                                     OperatorPlan plan,
                                     int rp) {
        super(name, plan);
        requestedParallelism = rp;
    }
    

    public int getRequestedParallelisam() {
        return requestedParallelism;
    } 

    /**
     * Get the schema for the output of this relational operator.
     * @return the schema
     */
    abstract public LogicalSchema getSchema();

}
