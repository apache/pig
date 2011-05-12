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
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

/**
 * Expand project-star or project-range when used as udf argument.
 * This is different from {@link ProjectStarExpander} because in those 
 * cases, the project star gets expanded as new {@link LogicalExpressionPlan}.
 * In case of project-star or project-range within udf, it should get expanded
 * only as multiple inputs to this udf, no addtional {@link LogicalExpressionPlan}s
 * are created.
 * The expansion happens only if input schema is not null
 */
public class ProjStarInUdfExpander extends AllExpressionVisitor {

    public ProjStarInUdfExpander(OperatorPlan plan) throws FrontendException {
        super( plan, new DependencyOrderWalker( plan ) );
    }

    @Override
    protected LogicalExpressionVisitor getVisitor(final LogicalExpressionPlan exprPlan)
    throws FrontendException {
        //This handles the expansion udf in all operators other than foreach
        return new ProjExpanderForNonForeach(exprPlan);
    }  


    /* 
     * LOForeach needs special handling because LOInnerLoad's inner ProjectExpression
     * is the one that gets expanded
     */
    @Override
    public void visit(LOForEach foreach) throws FrontendException{
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

        List<Operator> loGenPreds = innerPlan.getPredecessors(gen);

        if(loGenPreds == null){
            // there are no LOInnerLoads , must be working on just constants
            // no project-star expansion to be done
            return;
        }

        //get mapping of LOGenerate predecessor current position to object
        Map<Integer, LogicalRelationalOperator> oldPos2Rel =
            new HashMap<Integer, LogicalRelationalOperator>();
        
        for(int i=0; i<loGenPreds.size(); i++){
            oldPos2Rel.put(i, (LogicalRelationalOperator) loGenPreds.get(i));
        }

        //store mapping between the projection in inner plans of
        // of LOGenerate to the input relation object
        Map<ProjectExpression, LogicalRelationalOperator> proj2InpRel =
            new HashMap<ProjectExpression, LogicalRelationalOperator>();
        
        List<LOInnerLoad> expandedInLoads = new ArrayList<LOInnerLoad>();
        
        //visit each expression plan, and expand the projects in the udf
        for( OperatorPlan plan : gen.getOutputPlans()){
            ProjExpanderForForeach projExpander = new ProjExpanderForForeach(
                    plan,
                    gen,
                    oldPos2Rel,
                    proj2InpRel,
                    foreach,
                    expandedInLoads
            );
            projExpander.visit();
        }
        
        //remove the LOInnerLoads that have been expanded
        for(LOInnerLoad inLoad : expandedInLoads){
            innerPlan.disconnect(inLoad, gen);
            innerPlan.remove(inLoad);
        }
        
        //reset the input relation position in the projects
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
    }

    @Override
    public void visit(LOGenerate gen) throws FrontendException{
        
    }
}

class ProjExpanderForForeach extends LogicalExpressionVisitor{

    private LOGenerate loGen;
    private LogicalPlan innerRelPlan;
    private Map<Integer, LogicalRelationalOperator> oldPos2Rel;
    private Map<ProjectExpression, LogicalRelationalOperator> proj2InpRel;
    private LOForEach foreach;
    private List<LOInnerLoad> expandedInLoads;

    protected ProjExpanderForForeach(
            OperatorPlan p, 
            LOGenerate loGen, 
            Map<Integer, LogicalRelationalOperator> oldPos2Rel,
            Map<ProjectExpression, LogicalRelationalOperator> proj2InpRel,
            LOForEach foreach,
            List<LOInnerLoad> expandedInLoads
    )
    throws FrontendException {
        super(p, new ReverseDependencyOrderWalker(p));
        this.loGen = loGen;
        this.innerRelPlan = (LogicalPlan) loGen.getPlan();
        this.oldPos2Rel = oldPos2Rel;
        this.proj2InpRel = proj2InpRel;
        this.foreach = foreach;
        this.expandedInLoads = expandedInLoads;
        
    }
    
    @Override
    public void visit(UserFuncExpression func) throws FrontendException{
        if(plan.getSuccessors(func) == null){
            // no args for the udf, so nothing to do
            return;
        }
        List<Operator> inputs = new ArrayList<Operator>(plan.getSuccessors(func));

        // expandedProjectStars will be removed from plan
        List<Operator> expandedProjectStars = new ArrayList<Operator>();

        // new projects to be added to the plan
        List<Operator> newExpandedProjects =  new ArrayList<Operator>();

        //new set of inputs 
        List<Operator> newInputs = new ArrayList<Operator>();
        

        
        for(Operator inp  : inputs){
            if(inp instanceof ProjectExpression && ((ProjectExpression)inp).isRangeOrStarProject() 
                        && oldPos2Rel.get(((ProjectExpression)inp).getInputNum()) instanceof LOInnerLoad
            ){
                //under foreach the ProjectExpression is always a project-star, 
                // need to check if the input relation is a LOInnerLoad 
                // containing project-star/range
                LOInnerLoad inLoad = 
                    (LOInnerLoad)oldPos2Rel.get(((ProjectExpression)inp).getInputNum());
                
                ProjectExpression innerProj = inLoad.getProjection();
                if(!innerProj.isRangeOrStarProject()){
                    newInputs.add(inp);
                    continue;
                }
                
                //try expanding the project-star/range
                List<Operator> expandedOps = expandProjectStar(innerProj);
                if(expandedOps != null){
                    expandedProjectStars.add(inp);//to remove
                    expandedInLoads.add(inLoad);//to remove
                    newInputs.addAll(expandedOps); 
                    newExpandedProjects.addAll(expandedOps);//to add
                }else {
                    newInputs.add(inp);
                }
            }else{
                newInputs.add(inp);
            }
        }
        
        //make changes to the plan if there is a project that was expanded
        if(expandedProjectStars.size() > 0){

            //disconnect old inputs
            for(Operator inp : inputs){
                plan.disconnect(func, inp);
            }

            //remove expanded projects
            for(Operator op : expandedProjectStars){
                plan.remove(op);
                proj2InpRel.remove(op);
            }

            //add new projects
            for(Operator op : newExpandedProjects){
                plan.add(op);
            }

            //connect new inputs
            for(Operator newInp : newInputs){
                plan.connect(func, newInp);
            }
        }
        
        
    }
    
    @Override
    public void visit(ProjectExpression proj){
        //add project to LOInnerLoad mapping so that the input number can be
        //corrected later
        proj2InpRel.put(proj, oldPos2Rel.get(proj.getInputNum()));
    }

    private List<Operator> expandProjectStar(ProjectExpression proj)
    throws FrontendException {
        Pair<Integer, Integer> firstLastCols =
            ProjectStarExpanderUtil.getProjectStartEndCols((LogicalExpressionPlan)plan, proj);

        if(firstLastCols == null){
            //no expansion happening now
            return null;
        }
        
        //expand from firstProjCol to lastProjCol 
        int firstProjCol = firstLastCols.first;
        int lastProjCol = firstLastCols.second;


        List<Operator> newProjects = new ArrayList<Operator>();
        for(int i = firstProjCol; i <= lastProjCol; i++){
            LOInnerLoad newILoad = new LOInnerLoad(innerRelPlan, foreach, i);
            innerRelPlan.add(newILoad);
            innerRelPlan.connect(newILoad, loGen);
            ProjectExpression newProj = new ProjectExpression(plan, -2, -1, loGen) ;
            proj2InpRel.put(newProj, newILoad);
            newProjects.add(newProj);
        } 

        return newProjects;
        
    }
}

class ProjExpanderForNonForeach extends LogicalExpressionVisitor{

    protected ProjExpanderForNonForeach(OperatorPlan p)
    throws FrontendException {
        super(p, new ReverseDependencyOrderWalker(p));
    }

    @Override
    public void visit(UserFuncExpression func) throws FrontendException {
        if(plan.getSuccessors(func) == null){
            //udf without args, nothing to do 
            return;
        }
        List<Operator> inputs = new ArrayList<Operator>(plan.getSuccessors(func));

        // expandedProjectStars will be removed from plan
        List<Operator> expandedProjectStars = new ArrayList<Operator>();

        // new projects to be added to the plan
        List<Operator> newExpandedProjects =  new ArrayList<Operator>();

        //new set of inputs 
        List<Operator> newInputs = new ArrayList<Operator>();

        for(Operator inp  : inputs){
            if(inp instanceof ProjectExpression && ((ProjectExpression)inp).isRangeOrStarProject() ){
                //try expanding the project-star/range
                List<Operator> expandedOps = expandProjectStar((ProjectExpression)inp);
                if(expandedOps != null){
                    expandedProjectStars.add(inp);//to remove
                    newInputs.addAll(expandedOps); 
                    newExpandedProjects.addAll(expandedOps);//to add
                }else {
                    newInputs.add(inp);
                }
            }else{
                newInputs.add(inp);
            }
        }


        //make changes to the plan if there is a project that was expanded
        if(expandedProjectStars.size() > 0){

            //disconnect old inputs
            for(Operator inp : inputs){
                plan.disconnect(func, inp);
            }

            //remove expanded projects
            for(Operator op : expandedProjectStars){
                plan.remove(op);
            }

            //add new projects
            for(Operator op : newExpandedProjects){
                plan.add(op);
            }

            //connect new inputs
            for(Operator newInp : newInputs){
                plan.connect(func, newInp);
            }
        }

    }

    private List<Operator> expandProjectStar(
            ProjectExpression proj) throws FrontendException {

        Pair<Integer, Integer> firstLastCols =
            ProjectStarExpanderUtil.getProjectStartEndCols((LogicalExpressionPlan)plan, proj);


        if(firstLastCols == null){
            //no expansion happening now
            return null;
        }
        //expand from firstProjCol to lastProjCol 
        int firstProjCol = firstLastCols.first;
        int lastProjCol = firstLastCols.second;


        List<Operator> newProjects = new ArrayList<Operator>();
        LogicalRelationalOperator relOp = proj.getAttachedRelationalOp();
        for(int i = firstProjCol; i <= lastProjCol; i++){
            newProjects.add(new ProjectExpression(plan, proj.getInputNum(), i, relOp));
        } 

        return newProjects;
    }


};