package org.apache.pig.backend.datastorage;

import java.io.IOException;

public class DataStorageException extends IOException {

    static final long serialVersionUID = 1;
    
    public DataStorageException(String message, Throwable cause) {
        super(message);
        initCause(cause);
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
