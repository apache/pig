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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to track the top-level projection operators in a plan.
 * If there is a $1.$0 then only $1 is tracked 
 */
public class TopLevelProjectFinder extends
        LOVisitor {
	
	List<LOProject> mProjectList = new ArrayList<LOProject>();

    public TopLevelProjectFinder(LogicalPlan plan) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
    }


    
    /* (non-Javadoc)
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LODistinct)
     */
    @Override
    protected void visit(LODistinct dt) throws VisitorException {
    }



    /* (non-Javadoc)
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOFilter)
     */
    @Override
    protected void visit(LOFilter filter) throws VisitorException {
    }



    /* (non-Javadoc)
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOForEach)
     */
    @Override
    protected void visit(LOForEach forEach) throws VisitorException {
    }



    /* (non-Javadoc)
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOSort)
     */
    @Override
    protected void visit(LOSort s) throws VisitorException {
    }



    /* (non-Javadoc)
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOProject)
     */
    @Override
    protected void visit(LOProject project) throws VisitorException {
        //If the project is a root then add it to the list
    	List<LogicalOperator> projectPreds = this.getPlan().getPredecessors(project);
    	if(projectPreds == null) {
        	//check if the project's predecessor is null then add it to the list
    		mProjectList.add(project);
    	} /*else if (!(projectPreds.get(0) instanceof LOProject)) {
    		//check if the project's predecessor is not a project then add it to the list
    		mProjectList.add(project);
    	}*/
    }
    
    public List<LOProject> getProjectList() {
    	return mProjectList;
    }
    
    public Set<LOProject> getProjectSet() {
    	return new HashSet<LOProject>(mProjectList);
    }
    
    public Set<LOProject> getProjectStarSet() {
    	Set<LOProject> projectStarSet = new HashSet<LOProject>();
    	
    	for(LOProject project: getProjectSet()) {
    		if(project.isStar() && (this.getPlan().getPredecessors(project) == null)) {
    			projectStarSet.add(project);
    		}
    	}
    	
    	return (projectStarSet.size() == 0? null : projectStarSet);
    }
}
