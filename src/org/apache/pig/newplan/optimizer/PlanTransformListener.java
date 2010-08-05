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

/**
 * A listener class that patches up plans after they have been transformed.
 */
public interface PlanTransformListener {
    /**
     * the listener that is notified after a plan is transformed
     * @param fp  the full plan that has been transformed
     * @param tp  a plan containing only the operators that have been transformed
     * @throws FrontendException 
     */
    public void transformed(OperatorPlan fp, OperatorPlan tp) throws FrontendException;

}
