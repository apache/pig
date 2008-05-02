package org.apache.pig.impl.logicalLayer.validators;

import org.apache.pig.impl.PigContext ;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.DepthFirstWalker ;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.plan.PlanValidator ;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.omg.CORBA.LocalObject;

/**
 * This validator does check
 * 1) Input files referred to by all LOLoads exist
 * 2) Output files referred to by all LOStores do not exist
 */
public class InputOutputFileValidator extends PlanValidator<LogicalOperator, LogicalPlan>{
    
    private PigContext pigCtx = null ;
    
    public InputOutputFileValidator(PigContext pigContext) {  
        pigCtx = pigContext ;
    }

    public void validate(LogicalPlan plan,
                         CompilationMessageCollector messageCollector) 
                                            throws PlanValidationException {
        
        super.validate(new InputOutputFileVisitor(plan, 
                                                  messageCollector, 
                                                  pigCtx),
                       messageCollector);
    
    }
    
    
    
}
