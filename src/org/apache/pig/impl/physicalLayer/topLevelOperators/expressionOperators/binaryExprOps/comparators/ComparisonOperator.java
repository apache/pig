package org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.comparators;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.binaryExprOps.BinaryExpressionOperator;

/**
 * This is a base class for all comparison operators. Supports the
 * use of operand type instead of result type as the result type is
 * always boolean.
 * 
 * All comparison operators fetch the lhs and rhs operands and compare
 * them for each type using different comparison methods based on what
 * comparison is being implemented.
 *
 */
public abstract class ComparisonOperator extends BinaryExpressionOperator {
	//The result type for comparison operators is always
	//Boolean. So the plans evaluating these should consider
	//the type of the operands instead of the result.
	//The result will be comunicated using the Status object.
	//This is a slight abuse of the status object.
	protected byte operandType;
	
	public ComparisonOperator(OperatorKey k) {
		this(k,-1);
	}

	public ComparisonOperator(OperatorKey k, int rp) {
		super(k, rp);
	}

	public byte getOperandType() {
		return operandType;
	}

	public void setOperandType(byte operandType) {
		this.operandType = operandType;
	}
}
