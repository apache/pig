package org.apache.pig.impl.physicalLayer.plans;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ConstantExpression;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.POProject;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.EqualToExpr;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GTOrEqualToExpr;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.GreaterThanExpr;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.LTOrEqualToExpr;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.LessThanExpr;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.NotEqualToExpr;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators.NotEqualToExpr;

/**
 * The visitor to be used for visiting expression plans.
 * Create the visitor with the ExpressionPlan to be visited.
 * Call the visit() method for a depth first traversal.
 *
 */
public abstract class ExprPlanVisitor extends PhyPlanVisitor<ExpressionOperator, ExprPlan> {

	private final Log log = LogFactory.getLog(getClass());
	
	public ExprPlanVisitor(ExprPlan plan) {
		super(plan);
		// TODO Auto-generated constructor stub
	}
	
	public void visitConstant(ConstantExpression cnst){
		//do nothing
	}
	
	public void visitProject(POProject proj){
		//do nothing
	}
	
	public void visitGreaterThan(GreaterThanExpr grt){
		//do nothing
	}
	
	/*public void visitLessThan(LessThanExpr lt){
		//do nothing
	}
	
	public void visitGTOrEqual(GTOrEqualToExpr gte){
		//do nothing
	}
	
	public void visiLTOrEqual(LTOrEqualToExpr lte){
		//do nothing
	}
	
	public void visitEqualTo(EqualToExpr eq){
		//do nothing
	}
	
	public void visitNotEqualTo(NotEqualToExpr eq){
		//do nothing
	}*/

}
