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

package org.apache.pig.newplan.logical.optimizer;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.optimizer.PlanTransformListener;

/**
 * A PlanTransformListener for the logical optimizer that will patch up schemas
 * after a plan has been transformed.
 *
 */
public class SchemaPatcher implements PlanTransformListener {

    /**
     * @throws FrontendException 
     * @link org.apache.pig.newplan.optimizer.PlanTransformListener#transformed(org.apache.pig.newplan.OperatorPlan, org.apache.pig.newplan.OperatorPlan)
     */
    @Override
    public void transformed(OperatorPlan fp, OperatorPlan tp) throws FrontendException {
        // Walk the transformed plan and clean out the schemas and call
        // getSchema again on each node.  This will cause each node
        // to regenerate its schema from its parent.
        
        SchemaResetter schemaResetter = new SchemaResetter(tp);
        schemaResetter.visit();
    }

}
