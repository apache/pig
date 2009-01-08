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
import org.apache.pig.impl.util.MultiMap;

/**
 * A visitor mechanism for navigating and operating on a tree of Logical
 * Operators. This class contains the logic to navigate the tree, but does not
 * do anything with or to the tree. In order to operate on or extract
 * information from the tree, extend this class. You only need to implement the
 * methods dealing with the logical operators you are concerned with. For
 * example, if you wish to find every LOEval in a logical plan and perform some
 * operation on it, your visitor would look like: class MyLOVisitor extends
 * LOVisitor { public void visitEval(LOEval e) { you're logic here } } Any
 * operators that you do not implement the visitX method for will then be
 * navigated through by this class.
 * 
 * *NOTE* When invoking a visitor, you should never call one of the methods in
 * this class. You should pass your visitor as an argument to visit() on the
 * object you want to visit. So: RIGHT: LOEval myEval; MyVisitor v;
 * myEval.visit(v); WRONG: LOEval myEval; MyVisitor v; v.visitEval(myEval);
 * These methods are only public to make them accessible to the LO* objects.
 */
abstract public class LOVisitor extends
        PlanVisitor<LogicalOperator, LogicalPlan> {

    public LOVisitor(LogicalPlan plan,
                     PlanWalker<LogicalOperator, LogicalPlan> walker) {
        super(plan, walker);
    }

    /**
     * @param lOp
     *            the logical operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LogicalOperator lOp)
            throws VisitorException {
        //
        // Do Nothing
        //
    }

    /**
     * @param eOp
     *            the logical expression operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(ExpressionOperator eOp)
            throws VisitorException {
        //
        // Do Nothing
        //
    }

    /**
     * @param binOp
     *            the logical binary expression operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(BinaryExpressionOperator binOp)
            throws VisitorException {
        //
        // Visit the left hand side operand followed by the right hand side
        // operand
        //

        /*
        binOp.getLhsOperand().visit(this);
        binOp.getRhsOperand().visit(this);
        */
    }

    /**
     * 
     * @param uniOp
     *            the logical unary operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(UnaryExpressionOperator uniOp) throws VisitorException {
        // Visit the operand

        /*
        uniOp.getOperand().visit(this);
        */
    }

    /**
     * 
     * @param cg
     *            the logical cogroup operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOCogroup cg) throws VisitorException {
        // Visit each of the inputs of cogroup.
        MultiMap<LogicalOperator, LogicalPlan> mapGByPlans = cg.getGroupByPlans();
        for(LogicalOperator op: cg.getInputs()) {
            for(LogicalPlan lp: mapGByPlans.get(op)) {
                if (null != lp) {
                    // TODO FIX - How do we know this should be a
                    // DependencyOrderWalker?  We should be replicating the
                    // walker the current visitor is using.
                    PlanWalker w = new DependencyOrderWalker(lp);
                    pushWalker(w);
                    w.walk(this);
                    popWalker();
                }
            }
        }
    }
    
    protected void visit(LOFRJoin frj) throws VisitorException {
        // Visit each of the inputs of cogroup.
        MultiMap<LogicalOperator, LogicalPlan> mapGByPlans = frj.getJoinColPlans();
        for(LogicalOperator op: frj.getInputs()) {
            for(LogicalPlan lp: mapGByPlans.get(op)) {
                if (null != lp) {
                    // TODO FIX - How do we know this should be a
                    // DependencyOrderWalker?  We should be replicating the
                    // walker the current visitor is using.
                    PlanWalker w = new DependencyOrderWalker(lp);
                    pushWalker(w);
                    w.walk(this);
                    popWalker();
                }
            }
        }
    }

    /**
     * 
     * @param forEach
     *            the logical foreach operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOForEach forEach) throws VisitorException {
        // Visit each of generates projection elements.
        for(LogicalPlan lp: forEach.getForEachPlans()) {
            PlanWalker w = new DependencyOrderWalker(lp);
            pushWalker(w);
            w.walk(this);
            popWalker();
        }
    }

    /**
     * 
     * @param s
     *            the logical sort operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOSort s) throws VisitorException {
        // Visit the sort function
        for(LogicalPlan lp: s.getSortColPlans()) {
            PlanWalker w = new DependencyOrderWalker(lp);
            pushWalker(w);
            w.walk(this);
            popWalker();
        }
    }

    protected void visit(LOLimit limOp) throws VisitorException {
        return;
    }
    
    protected void visit(LOStream stream) throws VisitorException {
        return;
    }

    /**
     * 
     * @param filter
     *            the logical filter operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOFilter filter) throws VisitorException {
        // Visit the condition for the filter followed by the input
        PlanWalker w = new DependencyOrderWalker(filter.getComparisonPlan());
        pushWalker(w);
        w.walk(this);
        popWalker();
    }

    /**
     * 
     * @param split
     *            the logical split operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOSplit split) throws VisitorException {
        // Visit each of split's conditions
        /*
        for(LogicalOperator logicalOp: split.getOutputs()) {
            logicalOp.visit(this);
        }
        */
    }

    /**
     * 
     * @param g
     *            the logical generate operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOGenerate g) throws VisitorException {
        // Visit the operators that are part of the foreach plan
        for(LogicalPlan lp: g.getGeneratePlans()) {
            PlanWalker w = new DependencyOrderWalker(lp);
            pushWalker(w);
            w.walk(this);
            popWalker();
        }
    }

    /**
     * Iterate over each expression that is part of the function argument list
     * 
     * @param func
     *            the user defined function
     * @throws VisitorException
     */
    protected void visit(LOUserFunc func) throws VisitorException {

    }

    /**
     * @param binCond
     *            the logical binCond operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOBinCond binCond) throws VisitorException {

    }

    /**
     * 
     * @param cast
     *            the logical cast operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOCast cast) throws VisitorException {

    }
    
    /**
     * 
     * @param regexp
     *            the logical regexp operator that has to be visited
     * @throws ParseException
     */
    protected void visit(LORegexp regexp) throws VisitorException {

    }

    protected void visit(LOLoad load) throws VisitorException{
        
        
    }
    
    protected void visit(LOStore store) throws VisitorException{
        
    }
    
    protected void visit(LOConst store) throws VisitorException{
        
    }

    protected void visit(LOUnion u) throws VisitorException {

    }

    protected void visit(LOSplitOutput sop) throws VisitorException {
        LogicalPlan lp = sop.getConditionPlan();
        if (null != lp) {
            PlanWalker w = new DependencyOrderWalker(lp);
            pushWalker(w);
            w.walk(this);
            popWalker();
        }
    }

    protected void visit(LODistinct dt) throws VisitorException {

    }

    protected void visit(LOCross cs) throws VisitorException {

    }

    protected void visit(LOProject project) throws VisitorException {
        // Visit the operand of the project as long as the sentinel is false
        
        if(!project.getSentinel()) {
            project.getExpression().visit(this);
        }
    }
    
    public void visit(LOGreaterThan op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOLesserThan op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOGreaterThanEqual op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOLesserThanEqual op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOEqual op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LONotEqual op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOAdd op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOSubtract op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOMultiply op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LODivide op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOMod op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	
	public void visit(LONegative op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOMapLookup op) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOAnd binOp) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOOr binOp) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LONot uniOp) throws VisitorException {
		// TODO Auto-generated method stub
		return;
	}

	public void visit(LOIsNull uniOp) throws VisitorException {
		return;
	}
}
