package org.apache.pig.impl.logicalLayer.validators;

import java.util.List; 
import java.util.ArrayList; 

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.* ;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.PigContext ;

/**
 * This class is responsible for all logical plan validations
 * after the parsing stage. 
 * All the validation logics should be added in the constructor.
 *
 */

public class LogicalPlanValidationExecutor 
                        implements PlanValidationExecutor<LogicalPlan> {
    
    private List<PlanValidator<LogicalOperator, LogicalPlan>> validatorList 
                            = new ArrayList<PlanValidator<LogicalOperator, LogicalPlan>>();
    
    /**
     * All the necessary validation logics can be plugged-in here.
     * Logics are executed from head to tail of the List.
     * 
     * In the PIG-111 Configuration patch, we can call 
     * pigContext.getProperties which holds current configuration
     * set. This allows us to let users enable/disable some validation
     * logics. The code will look like this:-
     * 
     * if ((Boolean)pigContext.getProperties().getProperty("pig.compiler.validationX") ) {
     *      validatorList.add(new ValidationLogicX(plan)) ;
     * }
     * 
     */
    
    public LogicalPlanValidationExecutor(LogicalPlan plan,
                                         PigContext pigContext) {
   
        // Default validations
        validatorList.add(new InputOutputFileValidator(pigContext)) ;
        validatorList.add(new TypeCheckingValidator()) ;
        
       
    }    

    public void validate(LogicalPlan plan,
            CompilationMessageCollector msgCollector) {
        if (msgCollector == null) {
            throw new AssertionError(" messageCollector in " 
                         + "LogicalPlanValidationExecutor cannot be null") ;
        }
        
        try {
            for(PlanValidator<LogicalOperator, LogicalPlan> validator : validatorList) {
                validator.validate(plan, msgCollector) ;
            }
        } 
        catch(PlanValidationException pve) {
            msgCollector.collect("Severe problem found during validation"
                                 + pve.toString() ,
                                 MessageType.Error) ;
        }     
    }
}
