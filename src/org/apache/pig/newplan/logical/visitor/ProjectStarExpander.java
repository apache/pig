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
package org.apache.pig.newplan.logical.visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCube;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

import com.google.common.primitives.Booleans;

/**
 * A visitor to walk operators that contain a nested plan and translate project( * )
 * operators to a list of projection operators, i.e., 
 * project( * ) -> project(0), project(1), ... project(n-2), project(n-1)
 * If input schema is null, project(*) is not expanded.
 * It also expands project range ( eg $1 .. $5). It won't expand project-range-to-end
 * (eg $3 ..) if the input schema is null.
 * 
 */
public class ProjectStarExpander extends LogicalRelationalNodesVisitor{

    public ProjectStarExpander(OperatorPlan plan)
    throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOSort sort) throws FrontendException{
        List<LogicalExpressionPlan> expPlans = sort.getSortColPlans();
        List<Boolean> ascOrder = sort.getAscendingCols();

        // new expressionplans and sort order list after star expansion
        List<LogicalExpressionPlan> newExpPlans =
            new ArrayList<LogicalExpressionPlan>();
        List<Boolean> newAscOrder =  new ArrayList<Boolean>();

        if(expPlans.size() != ascOrder.size()){
            throw new AssertionError("Size of expPlans and ascorder should be same");
        }            
        
        for(int i=0; i < expPlans.size(); i++){
            //expand the plan
            LogicalExpressionPlan ithExpPlan = expPlans.get(i);
            List<LogicalExpressionPlan> expandedPlans = expandPlan(ithExpPlan, 0);
            newExpPlans.addAll(expandedPlans);

            //add corresponding isAsc flags
            Boolean isAsc = ascOrder.get(i);
            for(int j=0; j < expandedPlans.size(); j++){
                newAscOrder.add(isAsc);
            }
        }

        //check if there is a project-star-to-end followed by another sort plan
        // in the expanded plans (can happen if there is no input schema)
        for(int i=0; i < newExpPlans.size(); i++){
            ProjectExpression proj = getProjectStar(newExpPlans.get(i));
            if(proj != null && 
                    proj.isRangeProject() && proj.getEndCol() == -1 &&
                    i != newExpPlans.size() -1
            ){
                //because of order by sampler logic limitation, this is not
                //supported right now
                String msg = "Project-range to end (eg. x..)" +
                " is supported in order-by only as last sort column";
                throw new FrontendException(
                        msg,
                        1128,
                        PigException.INPUT
                );
            }
        }
        
        sort.setSortColPlans(newExpPlans);
        sort.setAscendingCols(newAscOrder);
    }

    /**
     * Expand plan into multiple plans if the plan contains a project star,
     * if there is no project star the returned list contains the plan argument.
     * @param plan
     * @return
     * @throws FrontendException
     */
    private List<LogicalExpressionPlan> expandPlan(LogicalExpressionPlan plan, int inputNum)
    throws FrontendException {
        List<LogicalExpressionPlan> expandedPlans;
        ProjectExpression projStar = getProjectStar(plan);
        if(projStar != null){
            // expand the plan into multiple plans
            return expandPlan(plan, projStar, inputNum);
        }else{
            //no project star to expand
            expandedPlans = new ArrayList<LogicalExpressionPlan>();
            expandedPlans.add(plan);
        }
        return expandedPlans;
    }

    @Override
    public void visit(LOCogroup cg) throws FrontendException{
        
        MultiMap<Integer, LogicalExpressionPlan> inpExprPlans = 
            cg.getExpressionPlans();
 
        //modify the plans if they have project-star
        expandPlans(inpExprPlans);
        
        
        //do some validations -
        List<Operator> inputs = cg.getInputs((LogicalPlan) cg.getPlan());

        // check if after translation none of group by plans in a cogroup
        // have a project(*) - if they still do it's because the input
        // for the project(*) did not have a schema - in this case, we should
        // error out since we could have different number/types of 
        // cogroup keys
        if(inputs.size() > 1) { // only for cogroups
            for(int i=0; i<inputs.size(); i++)
                for(LogicalExpressionPlan lp: inpExprPlans.get(i)) {
                    if(getProjectStar(lp) != null) {
                        String msg = "Cogroup/Group by '*' or 'x..' " +
                        "(range of columns to the end) " +
                        "is only allowed if the input has a schema";
                        throw new VisitorException( cg,
                                msg,
                                1123,
                                PigException.INPUT
                        );
                    }
                }
        }
        // check if after translation all group by plans have same arity
        int arity = inpExprPlans.get(0).size();
        for(int i=1; i<inputs.size(); i++){
            if(arity != inpExprPlans.get(i).size()) {
                String msg = "The arity of cogroup/group by columns " +
                "do not match";
                throw new VisitorException(cg,
                        msg,
                        1122,
                        PigException.INPUT
                );
            }
        }

    }

    @Override
    public void visit(LOCube cu) throws FrontendException {

	MultiMap<Integer, LogicalExpressionPlan> inpExprPlans = cu.getExpressionPlans();

	// modify the plans if they have project-star
	expandPlans(inpExprPlans);

    }

    @Override
    public void visit(LOJoin join) throws FrontendException{
        expandPlans(join.getExpressionPlans());
    }

    @Override
    public void visit(LOForEach foreach) throws FrontendException{
        //in case of LOForeach , expand when inner plan has a single project-star
        // and its input LOInnerLoad also is a project-star
        // then Reset the input number in project expressions
        
        LogicalPlan innerPlan = foreach.getInnerPlan();
        
        //visit the inner plan first
        PlanWalker newWalker = currentWalker.spawnChildWalker(innerPlan);
        pushWalker(newWalker);
        currentWalker.walk(this);
        popWalker();
        
        //get the LOGenerate
        List<Operator> feOutputs = innerPlan.getSinks();
        LOGenerate gen = null;
        for( Operator op  : feOutputs){
            if(op instanceof LOGenerate){
                if(gen != null){
                    String msg = "Expected single LOGenerate output in innerplan of foreach";
                    throw new VisitorException(foreach,
                            msg,
                            2266,
                            PigException.BUG
                    );
                }
                gen = (LOGenerate) op;
            }
        }
        
        //work on the generate plan, flatten and user schema
        List<LogicalExpressionPlan> expPlans = gen.getOutputPlans();
        List<LogicalExpressionPlan> newExpPlans = new ArrayList<LogicalExpressionPlan>();
        
        List<Operator> loGenPreds = innerPlan.getPredecessors(gen);
        
        if(loGenPreds == null){
            // there are no LOInnerLoads , must be working on just constants
            // no project-star expansion to be done
            return;
        }
        
        List<LogicalSchema> userSchema = gen.getUserDefinedSchema();
        List<LogicalSchema> newUserSchema = null;
        if(userSchema != null){
            newUserSchema = new ArrayList<LogicalSchema>();
        }
        
        boolean[] flattens = gen.getFlattenFlags();
        List<Boolean> newFlattens = new ArrayList<Boolean>(flattens.length);

        //get mapping of LOGenerate predecessor current position to object
        Map<Integer, LogicalRelationalOperator> oldPos2Rel =
            new HashMap<Integer, LogicalRelationalOperator>();
        
        for(int i=0; i<loGenPreds.size(); i++){
            oldPos2Rel.put(i, (LogicalRelationalOperator) loGenPreds.get(i));
        }
        
        //get schema of predecessor, project-star expansion needs a schema
        LogicalRelationalOperator pred =
            (LogicalRelationalOperator) foreach.getPlan().getPredecessors(foreach).get(0);
        LogicalSchema inpSch = pred.getSchema();
 
        //store mapping between the projection in inner plans of
        // of LOGenerate to the input relation object
        Map<ProjectExpression, LogicalRelationalOperator> proj2InpRel =
            new HashMap<ProjectExpression, LogicalRelationalOperator>();
        
        
        for(int i=0; i<expPlans.size(); i++){
            LogicalExpressionPlan expPlan = expPlans.get(i);
            ProjectExpression projStar = getProjectLonelyStar(expPlan, oldPos2Rel);

            boolean foundExpandableProject = false;
            if(projStar != null){              
                //there is a project-star to be expanded

                LogicalSchema userStarSch = null;
                if(userSchema != null && userSchema.get(i) != null){
                    userStarSch = userSchema.get(i);
                }


                //the range values are set in the project in LOInnerLoad
                ProjectExpression loInnerProj = ((LOInnerLoad)oldPos2Rel.get(projStar.getInputNum())).getProjection();

                int firstProjCol = 0;
                int lastProjCol = 0;
                
                if(loInnerProj.isRangeProject()){
                    loInnerProj.setColumnNumberFromAlias();
                    firstProjCol = loInnerProj.getStartCol();
                    lastProjCol = loInnerProj.getEndCol();
                }

                
                boolean isProjectToEnd = loInnerProj.isProjectStar() || 
                    (loInnerProj.isRangeProject() && lastProjCol == -1); 
                
                //can't expand if there is no input schema, and this is
                // as project star or project-range-to-end
                if( !(inpSch == null && isProjectToEnd) ){
                    
                    foundExpandableProject = true;

                    if(isProjectToEnd)
                        lastProjCol = inpSch.size() - 1;

                    //replacing the existing project star with new ones
                    expPlan.remove(projStar);

                    //remove the LOInnerLoad with star
                    LOInnerLoad oldLOInnerLoad = (LOInnerLoad)oldPos2Rel.get(projStar.getInputNum());
                    innerPlan.disconnect(oldLOInnerLoad, gen);
                    innerPlan.remove(oldLOInnerLoad);


                    //generate new exp plan, inner load for each field in schema
                    for(int j = firstProjCol; j <= lastProjCol; j++){

                        //add new LOInnerLoad
                        LOInnerLoad newInLoad = new LOInnerLoad(innerPlan, foreach, j);
                        innerPlan.add(newInLoad);
                        innerPlan.connect(newInLoad, gen);


                        // new expression plan and proj
                        LogicalExpressionPlan newExpPlan = new LogicalExpressionPlan();
                        newExpPlans.add(newExpPlan);

                        ProjectExpression newProj =
                            new ProjectExpression(newExpPlan, -2, -1, gen);

                        proj2InpRel.put(newProj, newInLoad);

                        newFlattens.add(flattens[i]);
                        if(newUserSchema != null ){
                            //index into user specified schema
                            int schIdx = j - firstProjCol;
                            if(userStarSch != null 
                                    && userStarSch.getFields().size() > schIdx
                                    && userStarSch.getField(schIdx) != null){

                                //if the project-star field has user specified schema, use the
                                // j'th field for this column
                                LogicalSchema sch = new LogicalSchema();
                                sch.addField(new LogicalFieldSchema(userStarSch.getField(schIdx)));
                                newUserSchema.add(sch);
                            }
                            else{
                                newUserSchema.add(null);
                            }
                        }
                    }
                }
            }

            if(!foundExpandableProject){ //no project-star that could be expanded

                //get all projects in here 
                FindProjects findProjs = new FindProjects(expPlan);
                findProjs.visit();
                List<ProjectExpression> projs = findProjs.getProjs();

                //create a mapping of project expression to their inputs
                for(ProjectExpression proj : projs){
                    proj2InpRel.put(proj, oldPos2Rel.get(proj.getInputNum()));
                }

                newExpPlans.add(expPlan);

                newFlattens.add(flattens[i]);
                if(newUserSchema != null)
                    newUserSchema.add(userSchema.get(i));

            }
        }

        //get mapping of LoGenerate input relation to current position
        Map<LogicalRelationalOperator, Integer> rel2pos = new HashMap<LogicalRelationalOperator, Integer>();
        List<Operator> newGenPreds = innerPlan.getPredecessors(gen);
        int numNewGenPreds = 0;
        if(newGenPreds != null)
            numNewGenPreds = newGenPreds.size();
            
        for(int i=0; i<numNewGenPreds; i++){
            rel2pos.put((LogicalRelationalOperator) newGenPreds.get(i),i);
        }
        
        //correct the input num for projects
        for(Entry<ProjectExpression, LogicalRelationalOperator> projAndInp : proj2InpRel.entrySet()){
           ProjectExpression proj = projAndInp.getKey();
           LogicalRelationalOperator rel = projAndInp.getValue();
           proj.setInputNum(rel2pos.get(rel));
        }
        
        // set the new lists
        gen.setOutputPlans(newExpPlans);
        gen.setFlattenFlags(Booleans.toArray(newFlattens));
        gen.setUserDefinedSchema(newUserSchema);
        
        gen.resetSchema();
        foreach.resetSchema();
        
    }
    
    
    static class FindProjects extends LogicalExpressionVisitor{

        private List<ProjectExpression> projs = new ArrayList<ProjectExpression>();

        protected FindProjects(LogicalExpressionPlan plan)
                throws FrontendException {
            super(plan, new DepthFirstWalker(plan));
        }
        
        @Override
        public void visit(ProjectExpression proj){
           projs.add(proj);
        }

        public List<ProjectExpression> getProjs(){
            return projs;
        }
    }

    /**
     * Find project-star in foreach statement. The LOInnerLoad corresponding
     * to the project-star also needs to have a project-star
     * @param expPlan - expression plan
     * @param oldPos2Rel - inner relational plan of foreach
     * @return ProjectExpression that satisfies the conditions
     * @throws FrontendException 
     */
    private ProjectExpression getProjectLonelyStar(LogicalExpressionPlan expPlan,
            Map<Integer, LogicalRelationalOperator> oldPos2Rel) throws FrontendException {

        //the expression plan should have just a single project
        if(expPlan.size() == 0 || expPlan.size() > 1){
            return null;
        }

        Operator outputOp = expPlan.getOperators().next();
        if(outputOp instanceof ProjectExpression){
            ProjectExpression proj = (ProjectExpression)outputOp;
            //check if ProjectExpression is projectStar
            if(proj.isProjectStar()){
                //now check if its input is a LOInnerLoad and it is projectStar 
                // or range project
                LogicalRelationalOperator inputRel = oldPos2Rel.get(proj.getInputNum());
                if(! (inputRel  instanceof LOInnerLoad)){
                    return null;
                }

                ProjectExpression innerProj = ((LOInnerLoad) inputRel).getProjection(); 
                if( innerProj.isRangeOrStarProject()){
                    return proj;
                }
            }
        }
        return null;
    }


    private void expandPlans(
            MultiMap<Integer, LogicalExpressionPlan> inpExprPlans)
    throws FrontendException {
 
        //for each input relation, expand any logical plan that has project-star
        for(int i=0; i< inpExprPlans.size() ; i++){
            List<LogicalExpressionPlan> plans = inpExprPlans.get(i);
            List<LogicalExpressionPlan> newPlans =
                new ArrayList<LogicalExpressionPlan>();
            for(LogicalExpressionPlan plan : plans){
                newPlans.addAll(expandPlan(plan, i));
            }
            inpExprPlans.removeKey(i);
            inpExprPlans.put(i, newPlans);
        }
   
    }

    /**
     * expand this plan containing project star to multiple plans 
     * each projecting a single column
     * @param expPlan
     * @param proj
     * @return
     * @throws FrontendException 
     */
    private List<LogicalExpressionPlan> expandPlan(
            LogicalExpressionPlan expPlan, ProjectExpression proj, int inputNum)
            throws FrontendException {
        
        Pair<Integer, Integer> startAndEndProjs =
            ProjectStarExpanderUtil.getProjectStartEndCols(expPlan, proj);  
        List<LogicalExpressionPlan> newPlans = new ArrayList<LogicalExpressionPlan>();

        if(startAndEndProjs == null){
            // can't expand this project
            newPlans.add(expPlan);
            return newPlans;
        }

        //expand from firstProjCol to lastProjCol 
        int firstProjCol = startAndEndProjs.first;
        int lastProjCol = startAndEndProjs.second;

        LogicalRelationalOperator relOp = proj.getAttachedRelationalOp();
        for(int i = firstProjCol; i <= lastProjCol; i++){
            newPlans.add(createExpPlanWithProj(relOp, inputNum, i));
        }

        return newPlans;

    }

    /**
     * Create new logical plan with a project that is attached to LogicalRelation
     * attachRel and projects i'th column from input
     * @param attachRel 
     * @param inputNum
     * @param colNum 
     * @return
     */
    private LogicalExpressionPlan createExpPlanWithProj(
            LogicalRelationalOperator attachRel, 
            int inputNum, int colNum) {
        LogicalExpressionPlan newExpPlan = new LogicalExpressionPlan();
        ProjectExpression newProj = 
            new ProjectExpression(newExpPlan, inputNum, colNum, attachRel);
        newExpPlan.add(newProj);
        return newExpPlan;
    }

    /**
     * if LogicalExpressionPlan argument has a project star output then
     * return it, otherwise return null
     * @param expPlan
     * @return
     * @throws FrontendException 
     */
    private ProjectExpression getProjectStar(LogicalExpressionPlan expPlan)
    throws FrontendException {
        List<Operator> outputs = expPlan.getSources();
        ProjectExpression projStar = null;
        for(Operator outputOp : outputs){
            if(outputOp instanceof ProjectExpression){
                ProjectExpression proj = (ProjectExpression)outputOp;
                if(proj.isRangeOrStarProject()){
                    if(outputs.size() > 1){
                        String msg = "More than one operator in an expression plan" +
                        " containing project star(*)/project-range (..)";
                        throw new VisitorException(proj,
                                msg,
                                2264,
                                PigException.BUG
                        );
                    }
                    projStar = proj;
                }
            }
        }
        return projStar;
    }



}
