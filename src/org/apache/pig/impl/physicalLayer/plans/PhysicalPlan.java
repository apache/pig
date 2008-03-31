package org.apache.pig.impl.physicalLayer.plans;

import java.io.OutputStream;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.plan.OperatorPlan;

/**
 * 
 * The base class for all types of physical plans. 
 * This extends the Operator Plan.
 *
 * @param <E>
 */
public abstract class PhysicalPlan<E extends PhysicalOperator> extends OperatorPlan<E> {

	public PhysicalPlan() {
		super();
	}
	
	public void attachInput(Tuple t){
		List<E> roots = getRoots();
		for (E operator : roots)
			operator.attachInput(t);
	}
	
	/**
	 * Write a visual representation of the Physical Plan
	 * into the given output stream
	 * @param out : OutputStream to which the visual representation is written
	 */
	public void explain(OutputStream out){
		//Use a plan visitor and output the current physical
		//plan into out
	}
}
