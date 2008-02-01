package org.apache.pig.backend.datastorage;

public class DataStorageException extends Throwable {

    static final long serialVersionUID = 1;
    
    public DataStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataStorageException() {
        this(null, null);
    }
    
    public DataStorageException(String message) {
        this(message, null);
    }
    
    public DataStorageException(Throwable cause) {
        this(null, cause);
    }
}
