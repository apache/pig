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

import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.optimizer.PlanTransformListener;

/**
 * A PlanTransformListener that will patch up references in projections.
 *
 */
public class ProjectionPatcher implements PlanTransformListener {

    /**
     * @link org.apache.pig.newplan.optimizer.PlanTransformListener#transformed(org.apache.pig.newplan.OperatorPlan, org.apache.pig.newplan.OperatorPlan)
     */
    @Override
    public void transformed(OperatorPlan fp, OperatorPlan tp)
            throws FrontendException {
        ProjectionFinder pf = new ProjectionFinder(tp);
        pf.visit();
    }
    
    private static class ProjectionRewriter extends LogicalExpressionVisitor {

        ProjectionRewriter(OperatorPlan p, LogicalRelationalOperator cop) throws FrontendException {
            super(p, new DepthFirstWalker(p));
        }
        
        @Override
        public void visit(ProjectExpression p) throws FrontendException {
            // if project is a project-star or range, ie it could not be expanded
            // then its not possible to determine the matching input columns
            // before runtime
            if (p.isRangeOrStarProject()) {
                return;
            }
            
            // Get the uid for this projection.  It must match the uid of the 
            // value it is projecting.
            long myUid = p.getFieldSchema().uid;
            
            // Find the operator this projection references
            LogicalRelationalOperator pred = p.findReferent();
            
            if (p.getAttachedRelationalOp() instanceof LOGenerate && p.getPlan().getSuccessors(p)==null) {
                // No need to adjust
                return;
            }
            else {
                // Get the schema for this operator and search it for the matching uid
                int match = -1;
                LogicalSchema schema = pred.getSchema();
                if (schema==null)
                    return;
                List<LogicalSchema.LogicalFieldSchema> fields = schema.getFields();
                for (int i = 0; i < fields.size(); i++) {
                    if (fields.get(i).uid == myUid) {
                        match = i;
                        break;
                    }
                }
                if (match == -1) {
                    throw new FrontendException("Couldn't find matching uid " + match + " for project "+p, 2229);
                }
                p.setColNum(match);
            }
        }        
    }
    
    public static class ProjectionFinder extends AllExpressionVisitor {

        public ProjectionFinder(OperatorPlan plan) throws FrontendException {
            super(plan, new DependencyOrderWalker(plan));
        }

        @Override
        protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) throws FrontendException {
            return new ProjectionRewriter(expr, currentOp);
        }
        
    }
}
