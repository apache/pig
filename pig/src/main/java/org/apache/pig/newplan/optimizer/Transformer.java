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

package org.apache.pig.newplan.optimizer;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;

public abstract class Transformer {

    /**
     * check if the transform should be done.  If this is being called then
     * the pattern matches, but there may be other criteria that must be met
     * as well.
     * @param matched the sub-set of the plan that matches the pattern. This 
     *        subset has the same graph as the pattern, but the operators
     *        point to the same objects as the plan to be matched.
     * @return true if the transform should be done.
     * @throws Transformer
     */
    public abstract boolean check(OperatorPlan matched) throws FrontendException;

    /**
     * Transform the tree
     * @param matched the sub-set of the plan that matches the pattern. This 
     *        subset has the same graph as the pattern, but the operators
     *        point to the same objects as the plan to be matched.
     * @throws Transformer
     */
    public abstract void transform(OperatorPlan matched) throws FrontendException;
    
    /**
     * Report what parts of the tree were transformed.  This is so that 
     * listeners can know which part of the tree to visit and modify
     * schemas, annotations, etc.  So any nodes that were removed need
     * will not be in this plan, only nodes that were added or moved.
     * @return OperatorPlan that describes just the changed nodes.
     */
    public abstract OperatorPlan reportChanges();

}
