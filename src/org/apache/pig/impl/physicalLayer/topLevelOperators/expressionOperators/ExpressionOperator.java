package org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators;


import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.physicalLayer.plans.ExprPlanVisitor;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;

/**
 * A base class for all types of expressions. All expression
 * operators must extend this class.
 *
 */

public abstract class ExpressionOperator extends PhysicalOperator<ExprPlanVisitor> {
	
	public ExpressionOperator(OperatorKey k) {
		this(k,-1);
	}

	public ExpressionOperator(OperatorKey k, int rp) {
		super(k, rp);
	}
	
	@Override
	public boolean supportsMultipleOutputs() {
		return false;
	}
	
	public abstract void visit(ExprPlanVisitor v) throws ParseException;
	
	
}
