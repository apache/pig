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
import java.util.List;

import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.plan.DepthFirstWalker;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.optimizer.PlanTransformListener;

/**
 * A PlanTransformListener that will patch up references in projections.
 *
 */
public class ProjectionPatcher implements PlanTransformListener {

    /**
     * @link org.apache.pig.experimental.plan.optimizer.PlanTransformListener#transformed(org.apache.pig.experimental.plan.OperatorPlan, org.apache.pig.experimental.plan.OperatorPlan)
     */
    @Override
    public void transformed(OperatorPlan fp, OperatorPlan tp)
            throws IOException {
        ProjectionFinder pf = new ProjectionFinder(tp);
        pf.visit();
    }
    
    private static class ProjectionRewriter extends LogicalExpressionVisitor {

        private LogicalRelationalOperator currentOp;
        
        ProjectionRewriter(OperatorPlan p, LogicalRelationalOperator cop) {
            super(p, new DepthFirstWalker(p));
            currentOp = cop;
        }
        
        @Override
        public void visitProject(ProjectExpression p) throws IOException {
            // Get the uid for this projection.  It must match the uid of the 
            // value it is projecting.
            long myUid = p.getUid();
            
            // Find the operator this projection references
            LogicalRelationalOperator pred = p.findReferent(currentOp);
            
            // Get the schema for this operator and search it for the matching uid
            int match = -1;
            LogicalSchema schema = pred.getSchema();
            List<LogicalSchema.LogicalFieldSchema> fields = schema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).uid == myUid) {
                    match = i;
                    break;
                }
            }
            if (match == -1) {
                throw new IOException("Couldn't find matching uid for project");
            }
            p.setColNum(match);
        }
        
    }
    
    private static class ProjectionFinder extends AllExpressionVisitor {

        public ProjectionFinder(OperatorPlan plan) {
            super(plan, new DepthFirstWalker(plan));
        }

        @Override
        protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) {
            return new ProjectionRewriter(expr, currentOp);
        }
        
    }

}
