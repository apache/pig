package org.apache.pig.backend.executionengine;

public class ExecException extends Exception {
    static final long serialVersionUID = 1;
    
    public ExecException (String message, Throwable cause) {
        super(message, cause);
    }

    public ExecException() {
        super();
    }
    
    public ExecException(String message) {
        super(message);
    }
    
    public ExecException(Throwable cause) {
        super(cause);
    }
}
