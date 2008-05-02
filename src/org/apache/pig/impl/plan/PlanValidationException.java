package org.apache.pig.impl.plan;

import org.apache.pig.impl.logicalLayer.FrontendException;

public class PlanValidationException extends FrontendException {
    
    private static final long serialVersionUID = 1L;

    public PlanValidationException (String message, Throwable cause) {
        super(message, cause);
    }

    public PlanValidationException() {
        this(null, null);
    }
    
    public PlanValidationException(String message) {
        this(message, null);
    }
    
    public PlanValidationException(Throwable cause) {
        this(null, cause);
    }
    
}
