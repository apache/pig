package org.apache.pig.backend.executionengine;

public class ExecutionEngineException extends Throwable {
    static final long serialVersionUID = 1;
    
    public ExecutionEngineException (String message, Throwable cause) {
        super(message, cause);
    }

    public ExecutionEngineException() {
        this(null, null);
    }
    
    public ExecutionEngineException(String message) {
        this(message, null);
    }
    
    public ExecutionEngineException(Throwable cause) {
        this(null, cause);
    }
}
