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

package org.apache.pig.impl.logicalLayer.optimizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LOFRJoin;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.optimizer.OptimizerException;

/**
 * A visitor to discover if any schema has been specified for a file being
 * loaded.  If so, a projection will be injected into the plan to cast the
 * data being loaded to the appropriate types.  The optimizer can then come
 * along and move those casts as far down as possible, or in some cases remove
 * them altogether.  This visitor does not handle finding the schemas for the 
 * file, that has already been done as part of parsing.
 *
 */
public class OpLimitOptimizer extends LogicalTransformer {

    private static final Log log = LogFactory.getLog(OpLimitOptimizer.class);
    private ExecType mode = ExecType.MAPREDUCE;

    public OpLimitOptimizer(LogicalPlan plan) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
    }

    public OpLimitOptimizer(LogicalPlan plan, ExecType mode) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
        this.mode = mode;
    }

    @Override
    public boolean check(List<LogicalOperator> nodes) throws OptimizerException {
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        
        try {
            LogicalOperator lo = nodes.get(0);
            if (lo == null || !(lo instanceof LOLimit)) {
                int errCode = 2005;
                String msg = "Expected " + LOLimit.class.getSimpleName() + ", got " + lo.getClass().getSimpleName();
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }
        } catch (Exception e) {
            int errCode = 2049;
            String msg = "Error while performing checks to optimize limit operator.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }

        return true;
    }

    @Override
    public void transform(List<LogicalOperator> nodes) throws OptimizerException {        
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        try {
            LogicalOperator lo = nodes.get(0);
            if (lo == null || !(lo instanceof LOLimit)) {
                int errCode = 2005;
                String msg = "Expected " + LOLimit.class.getSimpleName() + ", got " + lo.getClass().getSimpleName();
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }

            LOLimit limit = (LOLimit)lo;
            
            processNode(limit);
        } catch (OptimizerException oe) {
            throw oe;
        } catch (Exception e) {
            int errCode = 2050;
            String msg = "Internal error. Unable to optimize limit operator.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
    }
    
    // We recursively optimize a LOLimit, until one of the following conditions occurs:
    //   1. LOLimit can not move up
    //   2. LOLimit merged into another LOSort or another LOLimit
    // If we duplicate a LOLimit, then we leave the old LOLimit unmoved, 
    //    and recursively optimize the new LOLimit
    public void processNode(LOLimit limit) throws OptimizerException
    {
    	try {
            List<LogicalOperator> predecessors = mPlan.getPredecessors(limit);
            if (predecessors.size()!=1) {
            	int errCode = 2008;
            	String msg = "Limit cannot have more than one input. Found " + predecessors.size() + " inputs.";
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }
            LogicalOperator predecessor = predecessors.get(0);
            
            // Limit cannot be pushed up
            if (predecessor instanceof LOCogroup || predecessor instanceof LOFilter ||
            		predecessor instanceof LOLoad || predecessor instanceof LOSplit ||
            		predecessor instanceof LOSplitOutput || predecessor instanceof LODistinct || predecessor instanceof LOFRJoin)
            {
            	return;
            }
            // Limit can be pushed in front of ForEach if it does not have a flatten
            else if (predecessor instanceof LOForEach)
            {
            	LOForEach loForEach = (LOForEach)predecessor;
            	List<Boolean> mFlatten = loForEach.getFlatten();
            	boolean hasFlatten = false;
            	for (Boolean b:mFlatten)
            		if (b.equals(true)) hasFlatten = true;
            	
            	// We can safely move LOLimit up
            	if (!hasFlatten)
            	{
            		// Get operator before LOFilter
            		LogicalOperator prepredecessor = mPlan.getPredecessors(predecessor).get(0);
            		if (prepredecessor!=null)
            		{
            				try {
            					removeFromChain(limit, null);
            					insertBetween(prepredecessor, limit, predecessor, null);
            					
            				} catch (Exception e) {
            				    int errCode = 2009;
            				    String msg = "Can not move LOLimit up";
            					throw new OptimizerException(msg, errCode, PigException.BUG, e);
            				}
            		}
            		else
            		{
            		    int errCode = 2010;
            		    String msg = "LOFilter should have one input";
            			throw new OptimizerException(msg, errCode, PigException.BUG);
            		}
                    // we can move LOLimit even further, recursively optimize LOLimit
                    processNode(limit);
            	}
            }
            // Limit can be duplicated, and the new instance pushed in front of an operator for the following operators 
            // (that is, if you have X->limit, you can transform that to limit->X->limit):
            else if (predecessor instanceof LOCross || predecessor instanceof LOUnion)
            {
            	LOLimit newLimit = null;
            	List<LogicalOperator> nodesToProcess = new ArrayList<LogicalOperator>();
            	for (LogicalOperator prepredecessor:mPlan.getPredecessors(predecessor))
            		nodesToProcess.add(prepredecessor);
            	for (LogicalOperator prepredecessor:nodesToProcess)
            	{
            		try {
            			newLimit = (LOLimit)limit.duplicate();
            			insertBetween(prepredecessor, newLimit, predecessor, null);
            		} catch (Exception e) {
            		    int errCode = 2011;
            		    String msg = "Can not insert LOLimit clone";
            			throw new OptimizerException(msg, errCode, PigException.BUG, e);
            		}
            		// we can move the new LOLimit even further, recursively optimize LOLimit
            		processNode(newLimit);
            	}
            }
            // Limit can be merged into LOSort, result a "limited sort"
            else if (predecessor instanceof LOSort)
            {
                if(mode == ExecType.LOCAL) {
                    //We don't need this optimisation to happen in the local mode.
                    //so we do nothing here.
                } else {
                    LOSort sort = (LOSort)predecessor;
                    if (sort.getLimit()==-1)
                        sort.setLimit(limit.getLimit());
                    else
                        sort.setLimit(sort.getLimit()<limit.getLimit()?sort.getLimit():limit.getLimit());
                    try {
                        removeFromChain(limit, null);
                    } catch (Exception e) {
                        int errCode = 2012;
                        String msg = "Can not remove LOLimit after LOSort";
                        throw new OptimizerException(msg, errCode, PigException.BUG, e);
                    }
                }
            }
            // Limit is merged into another LOLimit
            else if (predecessor instanceof LOLimit)
            {
            	LOLimit beforeLimit = (LOLimit)predecessor;
            	beforeLimit.setLimit(beforeLimit.getLimit()<limit.getLimit()?beforeLimit.getLimit():limit.getLimit());
            	try {
            		removeFromChain(limit, null);
            	} catch (Exception e) {
            	    int errCode = 2012;
            	    String msg = "Can not remove LOLimit after LOLimit";
            		throw new OptimizerException(msg, errCode, PigException.BUG, e);
            	}
            }
            else {
                int errCode = 2013;
                String msg = "Moving LOLimit in front of " + predecessor.getClass().getSimpleName() + " is not implemented";
            	throw new OptimizerException(msg, errCode, PigException.BUG);
            }
    	} catch (OptimizerException oe) {
    	    throw oe;
        } catch (Exception e) {
            int errCode = 2050;
            String msg = "Internal error. Unable to optimize limit operator.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
    }
}

 
