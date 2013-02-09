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

import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

/**
 * Util function(s) for project-(star/range) expansion
 */
public class ProjectStarExpanderUtil{

    /**
     * If the argument project is a project-star or project-range that
     * can be expanded, find the position of first and last columns 
     * it should project  
     * @param expPlan
     * @param proj
     * @return pair that has the first and last columns that need to be projected 
     * @throws FrontendException
     */
    static Pair<Integer, Integer> getProjectStartEndCols(
            LogicalExpressionPlan expPlan, ProjectExpression proj)
            throws FrontendException {
        
        // get the input schema first
        
        LogicalRelationalOperator relOp = proj.getAttachedRelationalOp();

        // list of inputs of attached relation
        List<Operator> inputRels = relOp.getPlan().getPredecessors(relOp);

        //the relation that is input to this project 
        LogicalRelationalOperator inputRel =
            (LogicalRelationalOperator) inputRels.get(proj.getInputNum());

        LogicalSchema inputSchema = inputRel.getSchema();
        
        
        if(inputSchema == null && 
                (proj.isProjectStar() || (proj.isRangeProject() && proj.getEndCol() == -1))
        ){
            // can't expand if input schema is null and it is a project-star
            // or project-range-until-end
            return null;
        }

        //find first and last column in input schema to be projected
        int firstProjCol;
        int lastProjCol;

        //the range values are set in the project in LOInnerLoad
        if(proj.isRangeProject()){
            proj.setColumnNumberFromAlias();
            firstProjCol = proj.getStartCol();
            
            if(proj.getEndCol() >= 0)
                lastProjCol = proj.getEndCol();
            else
                lastProjCol = inputSchema.size() - 1;
        }else{
            //project-star
            firstProjCol = 0;
            lastProjCol = inputSchema.size() - 1;
        }
        return new Pair<Integer, Integer>(firstProjCol, lastProjCol);

    }



}
