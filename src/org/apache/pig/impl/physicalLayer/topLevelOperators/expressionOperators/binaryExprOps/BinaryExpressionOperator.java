package org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps;

import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ExpressionOperator;

/**
 * A base class for all Binary expression operators.
 * Supports the lhs and rhs operators which are used
 * to fetch the inputs and apply the appropriate operation
 * with the appropriate type.
 *
 */
public abstract class BinaryExpressionOperator extends ExpressionOperator {
	protected ExpressionOperator lhs;
	protected ExpressionOperator rhs;
	
	public BinaryExpressionOperator(OperatorKey k) {
		this(k,-1);
	}

	public BinaryExpressionOperator(OperatorKey k, int rp) {
		super(k, rp);
	}

	public ExpressionOperator getLhs() {
		return lhs;
	}
	
	@Override
	public boolean supportsMultipleInputs() {
		return true;
	}

	public void setLhs(ExpressionOperator lhs) {
		this.lhs = lhs;
	}

	public ExpressionOperator getRhs() {
		return rhs;
	}

	public void setRhs(ExpressionOperator rhs) {
		this.rhs = rhs;
	}
}
