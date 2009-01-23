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
package org.apache.pig.impl.logicalLayer;

import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;

import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

/**
 * A visitor to walk operators that contain a nested plan and translate project( * )
 * operators to a list of projection operators, i.e., 
 * project( * ) -> project(0), project(1), ... project(n-2), project(n-1)
 */
public class ProjectStarTranslator extends
        LOVisitor {

    public ProjectStarTranslator(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
    }

    /**
     * 
     * @param cg
     *            the logical cogroup operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOCogroup cg) throws VisitorException {
        //get the attributes of cogroup that are modified during the trnalsation
        
        MultiMap<LogicalOperator, LogicalPlan> mapGByPlans = cg.getGroupByPlans();

        for(LogicalOperator op: cg.getInputs()) {
            ArrayList<LogicalPlan> newGByPlans = new ArrayList<LogicalPlan>();
            for(LogicalPlan lp: mapGByPlans.get(op)) {
                if (checkPlanForProjectStar(lp)) {
                    ArrayList<LogicalPlan> translatedPlans = translateProjectStarInPlan(lp);
                    for(int j = 0; j < translatedPlans.size(); ++j) {
                        newGByPlans.add(translatedPlans.get(j));
                    }
                } else {
                    newGByPlans.add(lp);
                }
            }
            mapGByPlans.removeKey(op);
            mapGByPlans.put(op, newGByPlans);
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOFRJoin)
     */
    @Override
    protected void visit(LOFRJoin frj) throws VisitorException {
        //get the attributes of LOFRJoin that are modified during the translation
        
        MultiMap<LogicalOperator, LogicalPlan> joinColPlans = frj.getJoinColPlans();

        for(LogicalOperator op: frj.getInputs()) {
            ArrayList<LogicalPlan> newPlansAfterTranslation = new ArrayList<LogicalPlan>();
            for(LogicalPlan lp: joinColPlans.get(op)) {
                if (checkPlanForProjectStar(lp)) {
                    ArrayList<LogicalPlan> translatedPlans = translateProjectStarInPlan(lp);
                    for(int j = 0; j < translatedPlans.size(); ++j) {
                        newPlansAfterTranslation.add(translatedPlans.get(j));
                    }
                } else {
                    newPlansAfterTranslation.add(lp);
                }
            }
            joinColPlans.removeKey(op);
            joinColPlans.put(op, newPlansAfterTranslation);
        }
    }

    /**
     * 
     * @param forEach
     *            the logical foreach operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOForEach forEach) throws VisitorException {
        //get the attributes of foreach that are modified during the trnalsation

        super.visit(forEach);

        //List of inner plans
        ArrayList<LogicalPlan> foreachPlans = forEach.getForEachPlans();
        ArrayList<LogicalPlan> newForeachPlans = new ArrayList<LogicalPlan>();
        
        //the flatten list
        List<Boolean> flattenList = forEach.getFlatten();
        ArrayList<Boolean> newFlattenList = new ArrayList<Boolean>();
        
        //user specified schemas in the as clause
        List<Schema> userDefinedSchemaList = forEach.getUserDefinedSchema();
        ArrayList<Schema> newUserDefinedSchemaList = new ArrayList<Schema>();

        for(int i = 0; i < foreachPlans.size(); ++i) {
            LogicalPlan lp = foreachPlans.get(i);
            if(checkPlanForProjectStar(lp)) {
                ArrayList<LogicalPlan> translatedPlans = translateProjectStarInPlan(lp);
                Schema s = userDefinedSchemaList.get(i);
                for(int j = 0; j < translatedPlans.size(); ++j) {
                    LogicalPlan translatedPlan = translatedPlans.get(j);
                    newForeachPlans.add(translatedPlan);
                    newFlattenList.add(flattenList.get(i));
                    if(null != s) {
                        try {
                            if(j < s.size()) {
                                newUserDefinedSchemaList.add(new Schema(s.getField(j)));
                            } else {
                                newUserDefinedSchemaList.add(null);
                            }
                        } catch (FrontendException fee) {
                            throw new VisitorException(fee.getMessage(), fee);
                        }
                    } else {
                        newUserDefinedSchemaList.add(null);
                    }
                }
            } else {
                newForeachPlans.add(lp);
                newFlattenList.add(flattenList.get(i));
                if(null != userDefinedSchemaList) {
                    newUserDefinedSchemaList.add(userDefinedSchemaList.get(i));
                } else {
                    newUserDefinedSchemaList.add(null);
                }
            }
        }
        forEach.setForEachPlans(newForeachPlans);
        forEach.setFlatten(newFlattenList);
        forEach.setUserDefinedSchema(newUserDefinedSchemaList);

    }

    /**
     * 
     * @param s
     *            the logical sort operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOSort s) throws VisitorException {
        //get the attributes of sort that are modified during the trnalsation

        //List of inner plans
        List<LogicalPlan> sortPlans = s.getSortColPlans();
        ArrayList<LogicalPlan> newSortPlans = new ArrayList<LogicalPlan>();

        //sort order
        List<Boolean> sortOrder = s.getAscendingCols();
        ArrayList<Boolean> newSortOrder = new ArrayList<Boolean>();
        
        for(int i = 0; i < sortPlans.size(); ++i) {
            LogicalPlan lp = sortPlans.get(i);
            if(checkPlanForProjectStar(lp)) {
                ArrayList<LogicalPlan> translatedPlans = translateProjectStarInPlan(lp);
                for(int j = 0; j < translatedPlans.size(); ++j) {
                    newSortPlans.add(translatedPlans.get(j));
                    newSortOrder.add(sortOrder.get(i));
                }
            } else {
                newSortPlans.add(lp);
                newSortOrder.add(sortOrder.get(i));
            }
        }
        s.setSortColPlans(newSortPlans);
        s.setAscendingCols(newSortOrder);
    }

    private boolean checkPlanForProjectStar(LogicalPlan lp) {
        List<LogicalOperator> leaves = lp.getLeaves();

        for(LogicalOperator op: leaves) {
            if(op instanceof LOProject) {
                if(((LOProject) op).isStar() && ((LOProject)op).getType() != DataType.BAG) {
                    return true;
                }
            }
        }

        return false;
    }

    private LOProject getProjectStarFromPlan(LogicalPlan lp) {
        List<LogicalOperator> leaves = lp.getLeaves();

        for(LogicalOperator op: leaves) {
            if(op instanceof LOProject) {
                if(((LOProject) op).isStar()) {
                    return (LOProject)op;
                }
            }
        }

        return null;
    }

    private ArrayList<LogicalPlan> translateProjectStarInPlan(LogicalPlan lp) throws VisitorException {
        //translate the project( * ) into a list of projections
        LOProject projectStar = getProjectStarFromPlan(lp);
        LogicalOperator projectInput = projectStar.getExpression();
        ArrayList<LogicalPlan> translatedPlans = new ArrayList<LogicalPlan>();
        Schema s = null;
        try {
            if(!(projectInput instanceof ExpressionOperator)) {
                s = projectInput.getSchema();
            } else {
                Schema.FieldSchema fs = ((ExpressionOperator)projectInput).getFieldSchema();
                if(null != fs) {
                    s = fs.schema;
                }
            }
        } catch (FrontendException fee) {
            throw new VisitorException(fee.getMessage(), fee);
        }
        if (null != s) {
            for(int i = 0; i < s.size(); ++i) {
                LogicalPlan replicatedPlan = replicatePlan(lp);
                replaceProjectStar(replicatedPlan, projectStar, i);
                translatedPlans.add(replicatedPlan);
            }
        } else {
            translatedPlans.add(replicatePlan(lp));
        }
        return translatedPlans;
    }

    private LogicalPlan replicatePlan(LogicalPlan lp) throws VisitorException {
        LogicalPlan replicatedPlan = new LogicalPlan();

        for(LogicalOperator root: lp.getRoots()) {
            replicatedPlan.add(root);
            addSuccessors(lp, replicatedPlan, root);
        }

        return replicatedPlan;
    }

    private void addSuccessors(LogicalPlan lp, LogicalPlan replicatedPlan, LogicalOperator root) throws VisitorException {
        List<LogicalOperator> successors = lp.getSuccessors(root);
        if(null == successors) return;
        for(LogicalOperator succ: successors) {
            replicatedPlan.add(succ);
            try {
                replicatedPlan.connect(root, succ);
            } catch (PlanException pe) {
                throw new VisitorException(pe.getMessage(), pe);
            }
            addSuccessors(lp, replicatedPlan, succ);
        }
    }

    private void replaceProjectStar(LogicalPlan lp, LOProject projectStar, int column) throws VisitorException {
        String scope = projectStar.getOperatorKey().getScope();
        LogicalOperator projectInput = projectStar.getExpression();
        LogicalPlan projectPlan = projectStar.getPlan();
        LOProject replacementProject = new LOProject(projectPlan, OperatorKey.genOpKey(scope), projectInput, column); 
        try {
            lp.replace(projectStar, replacementProject);
        } catch (PlanException pe) {
            throw new VisitorException(pe.getMessage(), pe);
        }
    }

}
