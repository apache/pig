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
package org.apache.pig.impl.logicalLayer.optimizer;

import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalPlan;

/**
 * {@link Optimizer} is a simple {@link LogicalPlan} optimizer.
 * 
 * It <em>visits</em> every node in the <code>LogicalPlan</code> and then
 * optimizes the <code>LogicalPlan</code>.
 */
public abstract class Optimizer extends LOVisitor {
    /**
     * Optimize the given {@link LogicalPlan} if feasible and return status.
     * 
     * @param root root of the {@link LogicalPlan} to optimize
     * @return <code>true</code> if optimization was feasible and was effected,
     *         <code>false</code> otherwise.
     */
    abstract public boolean optimize(LogicalPlan root);
}
