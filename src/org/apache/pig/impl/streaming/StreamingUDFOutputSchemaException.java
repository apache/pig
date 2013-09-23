package org.apache.pig.impl.streaming;

public class StreamingUDFOutputSchemaException extends Exception {
    private static final long serialVersionUID = 1L;
    
    private int lineNumber;

    public StreamingUDFOutputSchemaException(String message, int lineNumber) {
        super(message);
        this.lineNumber = lineNumber;
    }
    
    public int getLineNumber() {
        return lineNumber;
    }
}
