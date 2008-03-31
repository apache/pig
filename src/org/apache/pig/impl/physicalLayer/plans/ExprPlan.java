package org.apache.pig.impl.physicalLayer.plans;

import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ExpressionOperator;

/**
 * A plan that supports expressions by combining the
 * expression operators. This will be used by top level
 * operators like filter and foreach that have Expression Plans
 * as their attribute plan.
 *
 */
public class ExprPlan extends PhysicalPlan<ExpressionOperator> {
	public ExprPlan() {
		super();
	}
}