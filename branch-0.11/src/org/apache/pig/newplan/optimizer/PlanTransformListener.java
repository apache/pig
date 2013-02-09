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

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;

/**
 * An interface to describe listeners that are notified when a plan is
 * modified.  The purpose of these listeners is to make modifications to
 * annotations on the plan after the plan is modified.  For example, if there
 * is a plan that has ... -&gt; Join -&gt; Filter -&gt; ... which is transformed
 * by pushing the filter before the join, then the input schema of the filter
 * will mostly likely change.  A schema listener can be used to make these
 * changes.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface PlanTransformListener {
    /**
     * Notification that a plan has been transformed.  The listener is free in
     * this method to make changes to the annotations on the plan now that it
     * has been transformed.
     * @param fp  the full plan that has been transformed
     * @param tp  a plan containing only the operators that have been transformed
     * @throws FrontendException 
     */
    public void transformed(OperatorPlan fp, OperatorPlan tp) throws FrontendException;

}
