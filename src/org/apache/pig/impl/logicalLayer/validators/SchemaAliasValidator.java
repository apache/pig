package org.apache.pig.impl.logicalLayer.validators;

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.plan.PlanValidator;

public class SchemaAliasValidator extends PlanValidator<LogicalOperator, LogicalPlan> {
    public SchemaAliasValidator() {   
    }
    
    public void validate(LogicalPlan plan,
                         CompilationMessageCollector msgCollector) 
                                        throws PlanValidationException {
        // The first msgCollector is used in visitor
        // The second msgCollector is used by the validator
        super.validateSkipCollectException(new SchemaAliasVisitor(plan),
                                           msgCollector) ;

    }
}
