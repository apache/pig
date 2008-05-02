package org.apache.pig.impl.plan;

/***
 * This is an abstract of classes for plan validation executor
 * 
 */
public interface PlanValidationExecutor<P extends OperatorPlan> {
   void validate(P plan, CompilationMessageCollector msgCollector) ;
}
