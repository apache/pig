package org.apache.pig.impl.physicalLayer.plans;

import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POGlobalRearrange;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POLoad;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POLocalRearrange;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POPackage;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POStore;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.StartMap;
import org.apache.pig.impl.plan.PlanVisitor;

/**
 * The visitor class for the Physical Plan. To use this,
 * create the visitor with the plan to be visited. Call 
 * the visit() method to traverse the plan in a depth first
 * fashion.
 * 
 * This class can be used to visit only the top level 
 * relational operators. Classes extending this should
 * use the specific plans to visit the attribute plans
 * of each of the top level operators.
 *
 * @param <O>
 * @param <P>
 */
public abstract class PhyPlanVisitor<O extends PhysicalOperator, P extends PhysicalPlan<O>> extends PlanVisitor<O,P> {

	public PhyPlanVisitor(P plan) {
		super(plan);
	}

	@Override
	public void visit() throws ParseException {
		depthFirst();
	}
	
//	public void visitLoad(POLoad ld){
//		//do nothing
//	}
//	
//	public void visitStore(POStore st){
//		//do nothing
//	}
//	
	public void visitFilter(POFilter fl){
		//do nothing
	}
//	
//	public void visitLocalRearrange(POLocalRearrange lr){
//		//do nothing
//	}
//	
//	public void visitGlobalRearrange(POGlobalRearrange gr){
//		//do nothing
//	}
//	
//	public void visitStartMap(StartMap sm){
//		//do nothing
//	}
//	
//	public void visitPackage(POPackage pkg){
//		//do nothing
//	}

}
