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

package org.apache.pig.experimental.logical.optimizer;

import java.io.IOException;

import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.plan.DependencyOrderWalker;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.optimizer.PlanTransformListener;

/**
 * A PlanTransformListener for the logical optimizer that will patch up schemas
 * after a plan has been transformed.
 *
 */
public class SchemaPatcher implements PlanTransformListener {

    /**
     * @throws IOException 
     * @link org.apache.pig.experimental.plan.optimizer.PlanTransformListener#transformed(org.apache.pig.experimental.plan.OperatorPlan, org.apache.pig.experimental.plan.OperatorPlan)
     */
    @Override
    public void transformed(OperatorPlan fp, OperatorPlan tp) throws IOException {
        // Walk the transformed plan and clean out the schemas and call
        // getSchema again on each node.  This will cause each node
        // to regenerate its schema from its parent.
        
        SchemaVisitor sv = new SchemaVisitor(tp);
        sv.visit();
    }
    
    private static class SchemaVisitor extends AllSameVisitor {

        public SchemaVisitor(OperatorPlan plan) {
            super(plan, new DependencyOrderWalker(plan));
        }

        @Override
        protected void execute(LogicalRelationalOperator op) throws IOException {
            op.resetSchema();
            
            // can not call get schema at this point, because projections have not been
            // adjusted. So only clean it up 
            // op.getSchema();
        }
        
    }

}
