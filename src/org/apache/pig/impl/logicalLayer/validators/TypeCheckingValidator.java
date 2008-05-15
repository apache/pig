package org.apache.pig.impl.logicalLayer.validators;

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.plan.PlanValidator;
import org.apache.pig.impl.plan.VisitorException;

public class TypeCheckingValidator extends PlanValidator<LogicalOperator, LogicalPlan> {
    
    public TypeCheckingValidator() {   
    }
    
    public void validate(LogicalPlan plan,
                         CompilationMessageCollector msgCollector) 
                                        throws PlanValidationException {
        // The first msgCollector is used in visitor
        // The second msgCollector is used by the validator
        super.validateSkipCollectException(new TypeCheckingVisitor(plan,  msgCollector),
                                           msgCollector) ;

    }
}
