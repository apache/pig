/**
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

package org.apache.pig.newplan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.Expression;
import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;

import org.apache.pig.Expression.OpType;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.DepthFirstWalker;

/**
 * This Visitor works on the filter condition of a LOFilter which immediately 
 * follows a LOLoad that interacts with a metadata system (currently OWL) to 
 * read table data. The visitor looks for conditions on partition columns in the
 * filter condition and extracts those conditions out of the filter condition.
 * The condition on partition cols will be used to prune partitions of the table.
 *
 */
public class PColFilterExtractor extends PlanVisitor {
    
    private static final Log LOG = LogFactory.getLog(PColFilterExtractor.class);
    
	/**
	 * partition columns associated with the table
	 * present in the load on which the filter whose
	 * inner plan is being visited is applied
	 */
	private List<String> partitionCols;

	/**
	 * will contain the partition column filter conditions
	 * accumulated during the visit - the final condition will an expression
	 * built from these sub expressions connected with AND
	 */
	private ArrayList<Expression> pColConditions = new ArrayList<Expression>();

	/**
	 * flag used during visit to indicate if a partition key
	 * was seen
	 */
	private boolean sawKey;

	private boolean sawNonKeyCol;

	private enum Side { LEFT, RIGHT, NONE };
	private Side replaceSide = Side.NONE;

	private boolean filterRemovable = false;
	
    private boolean canPushDown = true;
	
	@Override
	public void visit() throws FrontendException {
		// we will visit the leaf and it will recursively walk the plan
		LogicalExpression leaf = (LogicalExpression)plan.getSources().get( 0 );
		// if the leaf is a unary operator it should be a FilterFunc in 
		// which case we don't try to extract partition filter conditions
		if(leaf instanceof BinaryExpression) {
			BinaryExpression binExpr = (BinaryExpression)leaf;
			visit( binExpr );
			replaceChild( binExpr );
			// if the entire expression is to be removed, then the above
			// replaceChild will not set sawKey to false (sawKey is set to
			// false only in replaceChild()
			if(sawKey == true) {
				//there are only conditions on partition columns in the filter
				//extract it
				pColConditions.add( getExpression( leaf ) );
				filterRemovable = true;
			}
		}
	}

	/**
	 * 
	 * @param plan logical plan corresponding the filter's comparison condition
	 * @param partitionCols list of partition columns of the table which is
	 * being loaded in the LOAD statement which is input to the filter
	 */
	public PColFilterExtractor(OperatorPlan plan,
			List<String> partitionCols) {
		// though we configure a DepthFirstWalker to be the walker, we will not
		// use it - we will visit the leaf and it will recursively walk the
		// plan
		super( plan, new DepthFirstWalker( plan ) );
		this.partitionCols = new ArrayList<String>(partitionCols);
	}

	protected void visit(ProjectExpression project) throws FrontendException {
		String fieldName = project.getFieldSchema().alias;
		if(partitionCols.contains(fieldName)) {
			sawKey = true;
			// The condition on partition column will be used to prune the
			// scan and removed from the filter condition. Hence the condition 
			// on the partition column will not be re applied when data is read,
			// so the following cases should throw error until that changes.
			List<Class<?>> opsToCheckFor = new ArrayList<Class<?>>();
			opsToCheckFor.add(RegexExpression.class);
			if(checkSuccessors(project, opsToCheckFor)) {
            LOG.warn("No partition filter push down: " +
                "You have an partition column (" 
                + fieldName + ") inside a regexp operator in the " +
                "filter condition.");
            canPushDown = false;
            return;
			} 
			opsToCheckFor.set(0, UserFuncExpression.class);
			if(checkSuccessors(project, opsToCheckFor)) {
            LOG.warn("No partition filter push down: " +
                "You have an partition column (" 
                + fieldName + ") inside a function in the " +
                "filter condition.");
            canPushDown = false;
            return;
			}
			opsToCheckFor.set(0, CastExpression.class);
			if(checkSuccessors(project, opsToCheckFor)) {
            LOG.warn("No partition filter push down: " +
                "You have an partition column (" 
                + fieldName + ") inside a cast in the " +
                "filter condition.");
            canPushDown = false;
            return;
			}
			opsToCheckFor.set(0, IsNullExpression.class);
			if(checkSuccessors(project, opsToCheckFor)) {
            LOG.warn("No partition filter push down: " +
                "You have an partition column (" 
                + fieldName + ") inside a null check operator in the " +
                "filter condition.");
            canPushDown = false;
            return;
			}
			opsToCheckFor.set(0, BinCondExpression.class);
			if(checkSuccessors(project, opsToCheckFor)) {
            LOG.warn("No partition filter push down: " +
                "You have an partition column (" 
                + fieldName + ") inside a bincond operator in the " +
                "filter condition.");
            canPushDown = false;
            return;
			}
			opsToCheckFor.set(0, AndExpression.class);
			opsToCheckFor.add(OrExpression.class);
			if(checkSuccessors(project, opsToCheckFor)) {
            LOG.warn("No partition filter push down: " +
                "You have an partition column (" + fieldName +
                " ) in a construction like: " +
                "(pcond  and ...) or (pcond and ...) " +
                "where pcond is a condition on a partition column.");
            canPushDown = false;
            return;
			}
		} else {
			sawNonKeyCol = true;
		}
	}

	private void visit(BinaryExpression binOp) throws FrontendException {
		boolean lhsSawKey = false;        
		boolean rhsSawKey = false;        
		boolean lhsSawNonKeyCol = false;        
		boolean rhsSawNonKeyCol = false;        

		sawKey = false;
		sawNonKeyCol = false;
		visit( binOp.getLhs() );
		replaceChild(binOp.getLhs());
		lhsSawKey = sawKey;
		lhsSawNonKeyCol = sawNonKeyCol;

		sawKey = false;
		sawNonKeyCol = false;
		visit( binOp.getRhs() );
		replaceChild(binOp.getRhs());
		rhsSawKey = sawKey;
		rhsSawNonKeyCol = sawNonKeyCol;

		// only in the case of an AND, we potentially split the AND to 
		// remove conditions on partition columns out of the AND. For this 
		// we set replaceSide accordingly so that when we reach a predecessor
		// we can trim the appropriate side. If both sides of the AND have 
		// conditions on partition columns, we will remove the AND completely - 
		// in this case, we will not set replaceSide, but sawKey will be 
		// true so that as we go to higher predecessor ANDs we can trim later.
		if(binOp instanceof AndExpression) {
			if(lhsSawKey && rhsSawNonKeyCol){
				replaceSide = Side.LEFT;
			}else if(rhsSawKey && lhsSawNonKeyCol){
				replaceSide = Side.RIGHT;
			}
		} else if(lhsSawKey && rhsSawNonKeyCol || rhsSawKey && lhsSawNonKeyCol){
        LOG.warn("No partition filter push down: " +
            "Use of partition column/condition with" +
            " non partition column/condition in filter expression is not " +
            "supported.");
        canPushDown = false;
		}

		sawKey = lhsSawKey || rhsSawKey;
		sawNonKeyCol = lhsSawNonKeyCol || rhsSawNonKeyCol;
	}



	/**
	 * @return the condition on partition columns extracted from filter
	 */
	public  Expression getPColCondition(){
    if(!canPushDown || pColConditions.size() == 0)
        return null;
		Expression cond =  pColConditions.get(0);
		for(int i=1; i<pColConditions.size(); i++){
			//if there is more than one condition expression
			// connect them using "AND"s
			cond = new Expression.BinaryExpression(cond, pColConditions.get(i),
                    OpType.OP_AND);
		}
		return cond;
	}

	/**
	 * @return the filterRemovable
	 */
	public boolean isFilterRemovable() {
    return canPushDown && filterRemovable;
	}

	//////// helper methods /////////////////////////
	/**
	 * check for the presence of a certain operator type in the Successors
	 * @param opToStartFrom
	 * @param opsToCheckFor operators to be checked for at each level of 
	 * Successors - the ordering in the list is the order in which the ops 
	 * will be checked.
	 * @return true if opsToCheckFor are found
	 * @throws IOException 
	 */
	private boolean checkSuccessors(Operator opToStartFrom, 
			List<Class<?>> opsToCheckFor) throws FrontendException {
		boolean done = checkSuccessorsHelper(opToStartFrom, opsToCheckFor);
		if(!done && !opsToCheckFor.isEmpty()) {
			// continue checking if there is more to check
			while(!done) {
				opToStartFrom = plan.getPredecessors(opToStartFrom).get(0);
				done = checkSuccessorsHelper(opToStartFrom, opsToCheckFor);
			}
		}
		return opsToCheckFor.isEmpty();
	}

	private boolean checkSuccessorsHelper(Operator opToStartFrom, 
			List<Class<?>> opsToCheckFor) throws FrontendException {
		List<Operator> successors = plan.getPredecessors(
				opToStartFrom);
		if(successors == null || successors.size() == 0) {
			return true; // further checking cannot be done
		}
		if(successors.size() == 1) {
			Operator suc  = successors.get(0);
			if(suc.getClass().getCanonicalName().equals(
					opsToCheckFor.get(0).getCanonicalName())) {
				// trim the list of operators to check
				opsToCheckFor.remove(0);
				if(opsToCheckFor.isEmpty()) {
					return true; //no further checks required
				}
			}
		} else {
		    logInternalErrorAndSetFlag();
		}
		return false; // more checking can be done
	}

	private void replaceChild(LogicalExpression childExpr) throws FrontendException {

		if(replaceSide == Side.NONE) {
			// the child is trimmed when the appropriate
			// flag is set to indicate that it needs to be trimmed.
			return;
		}

		// eg if replaceSide == Side.LEFT
		//    binexpop
		//   /   \ \ 
		// child (this is the childExpr argument send in)
		//  /  \
		// Lt   Rt 
		//
		// gets converted to 
		//  binexpop
		//  /
		// Rt

		if( !( childExpr instanceof BinaryExpression ) ) {
		     logInternalErrorAndSetFlag();
	         return;
		}
		// child's lhs operand
		LogicalExpression leftChild = 
			((BinaryExpression)childExpr).getLhs();
		// child's rhs operand
		LogicalExpression rightChild = 
			((BinaryExpression)childExpr).getRhs();

		plan.disconnect( childExpr, leftChild );
		plan.disconnect( childExpr, rightChild );

		if(replaceSide == Side.LEFT) {
			// remove left child and replace childExpr with its right child
			remove( leftChild );
			replace(childExpr, rightChild);
		} else if(replaceSide == Side.RIGHT){
			// remove right child and replace childExpr with its left child
			remove(rightChild);
			replace(childExpr, leftChild);
		}else {
		     logInternalErrorAndSetFlag();
	         return;
		}
		//reset 
		replaceSide = Side.NONE;
		sawKey = false;

	}
	
	private void replace(Operator oldOp, Operator newOp) throws FrontendException {
		List<Operator> grandParents = plan.getPredecessors( oldOp );
		if( grandParents == null || grandParents.size() == 0 ) {
			plan.remove( oldOp );
			return;
		}
		Operator grandParent = plan.getPredecessors( oldOp ).get( 0 );
		Pair<Integer, Integer> pair = plan.disconnect( grandParent, oldOp );
		plan.add( newOp );
		plan.connect( grandParent, pair.first, newOp, pair.second );
		plan.remove( oldOp );
	}

	/**
	 * @param op
	 * @throws IOException 
	 * @throws IOException 
	 * @throws IOException 
	 */
	private void remove(LogicalExpression op) throws FrontendException {
		pColConditions.add( getExpression( op ) );
		removeTree( op );
	}
	
	/**
	 * Assume that the given operator is already disconnected from its predecessors.
	 * @param op
	 * @throws FrontendException 
	 */
	private void removeTree(Operator op) throws FrontendException {
		List<Operator> succs = plan.getSuccessors( op );
		if( succs == null ) {
			plan.remove( op );
			return;
		}
		
		Operator[] children = new Operator[succs.size()];
		for( int i = 0; i < succs.size(); i++ ) {
			children[i] = succs.get(i);
		}
		
		for( Operator succ : children ) {
			plan.disconnect( op, succ );
			removeTree( succ );
		}
		
		plan.remove( op );
	}

	public Expression getExpression(LogicalExpression op) throws FrontendException
	 {
		if(op instanceof ConstantExpression) {
			ConstantExpression constExpr =(ConstantExpression)op ;
			return new Expression.Const( constExpr.getValue() );
		} else if (op instanceof ProjectExpression) {
			ProjectExpression projExpr = (ProjectExpression)op;
			String fieldName = projExpr.getFieldSchema().alias;
            return new Expression.Column(fieldName);
        } else {
			if( !( op instanceof BinaryExpression ) ) {
            logInternalErrorAndSetFlag();
            return null;
			}
			BinaryExpression binOp = (BinaryExpression)op;
			if(binOp instanceof AddExpression) {
				return getExpression( binOp, OpType.OP_PLUS );
			} else if(binOp instanceof SubtractExpression) {
				return getExpression(binOp, OpType.OP_MINUS);
			} else if(binOp instanceof MultiplyExpression) {
				return getExpression(binOp, OpType.OP_TIMES);
			} else if(binOp instanceof DivideExpression) {
				return getExpression(binOp, OpType.OP_DIV);
			} else if(binOp instanceof ModExpression) {
				return getExpression(binOp, OpType.OP_MOD);
			} else if(binOp instanceof AndExpression) {
				return getExpression(binOp, OpType.OP_AND);
			} else if(binOp instanceof OrExpression) {
				return getExpression(binOp, OpType.OP_OR);
			} else if(binOp instanceof EqualExpression) {
				return getExpression(binOp, OpType.OP_EQ);
			} else if(binOp instanceof NotEqualExpression) {
				return getExpression(binOp, OpType.OP_NE);
			} else if(binOp instanceof GreaterThanExpression) {
				return getExpression(binOp, OpType.OP_GT);
			} else if(binOp instanceof GreaterThanEqualExpression) {
				return getExpression(binOp, OpType.OP_GE);
			} else if(binOp instanceof LessThanExpression) {
				return getExpression(binOp, OpType.OP_LT);
			} else if(binOp instanceof LessThanEqualExpression) {
				return getExpression(binOp, OpType.OP_LE);
			} else {
            logInternalErrorAndSetFlag();
			}
		}
		return null;
	}

    private Expression getExpression(BinaryExpression binOp, OpType 
            opType) throws FrontendException {
        return new Expression.BinaryExpression(getExpression(binOp.getLhs())
                ,getExpression(binOp.getRhs()), opType);
    }
    
    private void logInternalErrorAndSetFlag() throws FrontendException {
        LOG.warn("No partition filter push down: "
                + "Internal error while processing any partition filter "
                + "conditions in the filter after the load");
        canPushDown = false;
    }

	// this might get called from some visit() - in that case, delegate to
	// the other visit()s which we have defined here 
	private void visit(LogicalExpression op) throws FrontendException {
		if(op instanceof ProjectExpression) {
			visit((ProjectExpression)op);
		} else if (op instanceof BinaryExpression) {
			visit((BinaryExpression)op);
		} else if (op instanceof CastExpression) {
			visit((CastExpression)op);
		} else if (op instanceof BinCondExpression) {
			visit((BinCondExpression)op);
		} else if (op instanceof UserFuncExpression) {
			visit((UserFuncExpression)op);
		} else if (op instanceof IsNullExpression) {
			visit((IsNullExpression)op);
		} else if( op instanceof NotExpression ) {
			visit( (NotExpression)op );
		} else if( op instanceof RegexExpression ) {
			visit( (RegexExpression)op );
		}
	}

	// some specific operators which are of interest to catch some
	// unsupported scenarios
	private void visit(CastExpression cast) throws FrontendException {
		visit(cast.getExpression());
	}

	private void visit(NotExpression not) throws FrontendException {
		visit(not.getExpression());   
	}

	private void visit(RegexExpression regexp) throws FrontendException {
		visit((BinaryExpression)regexp);    
	}

	private void visit(BinCondExpression binCond) throws FrontendException {
		visit(binCond.getCondition());
		visit(binCond.getLhs());
		visit(binCond.getRhs());
	}

	private void visit(UserFuncExpression udf) throws FrontendException {
		for (LogicalExpression op : udf.getArguments()) {
			visit(op);
		}
	}

	private void visit(IsNullExpression isNull) throws FrontendException {
		visit(isNull.getExpression());
	}

    public boolean canPushDown() {
        return canPushDown;
    }

}
