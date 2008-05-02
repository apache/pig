package org.apache.pig.impl.plan;

import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;

/***
 * Master of all plan validation classes.
 * @param <P>
 */
public abstract class PlanValidator<O extends Operator, P extends OperatorPlan<O>> {
   
    /***
     * If there are errors during validation, all of the errors have
     * to be collected in the supplied messageCollector. The exception should
     * be thrown only when the validation logic finds something too bad 
     * that the other validation logics should not try to do more work.
     * 
     */
    public abstract void validate(P plan, CompilationMessageCollector messageCollector) 
                                                    throws PlanValidationException ;
    
    /**
     * This convenient method is used when: 
     * - if an exception is thrown from the current validation logic, 
     * the whole validation pipeline should stop.
     * @param visitor
     * @param messageCollector
     * @throws PlanValidationException
     */
    protected void validate(PlanVisitor<O, P> visitor, 
                            CompilationMessageCollector messageCollector) 
                                             throws PlanValidationException {
        try {
            visitor.visit() ;
        } 
        catch(VisitorException ve) {
            messageCollector.collect("Unexpected exception in " 
                                      + this.getClass().getSimpleName(),
                                      MessageType.Error) ;
            throw new PlanValidationException("An unexpected exception caused " 
                                              + "the validation to stop", ve) ;
        }
    }
    
    /**
     * This convenient method is used when: 
     * - if an exception is thrown from the current validation logic, 
     * the whole validation pipeline should keep going by continuing
     * with the next validation logic in the pipeline
     * @param visitor
     * @param messageCollector
     * @throws PlanValidationException
     */
    protected void validateTolerateException(PlanVisitor<O, P> visitor, 
                            CompilationMessageCollector messageCollector) 
                                             throws PlanValidationException {
        try {
            visitor.visit() ;
        } 
        catch(VisitorException ve) {
            messageCollector.collect("Unexpected exception in " 
                                  + this.getClass().getSimpleName(),
                                  MessageType.Error) ;
        }
    }

}
