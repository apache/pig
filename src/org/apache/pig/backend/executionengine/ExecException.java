package org.apache.pig.backend.executionengine;

public class ExecException extends Throwable {
    static final long serialVersionUID = 1;
    
    public ExecException (String message, Throwable cause) {
        super(message, cause);
    }

    public ExecException() {
        this(null, null);
    }
    
    public ExecException(String message) {
        this(message, null);
    }
    
    public ExecException(Throwable cause) {
        this(null, cause);
    }
}
