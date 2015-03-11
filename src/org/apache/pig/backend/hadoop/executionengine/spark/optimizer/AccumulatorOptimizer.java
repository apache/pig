package org.apache.pig.backend.hadoop.executionengine.spark.optimizer;

import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.backend.hadoop.executionengine.util.AccumulatorOptimizerUtil;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to optimize plans that determines if a vertex plan can run in
 * accumulative mode.
 */
public class AccumulatorOptimizer extends SparkOpPlanVisitor {

    public AccumulatorOptimizer(SparkOperPlan plan) {
		super(plan, new DepthFirstWalker<SparkOperator, SparkOperPlan>(plan));
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOperator) throws
			VisitorException {
        AccumulatorOptimizerUtil.addAccumulatorSpark(sparkOperator
                .physicalPlan);
    }
}