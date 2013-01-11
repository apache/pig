package org.apache.pig.data;

import org.apache.pig.backend.executionengine.ExecException;

public class FieldIsNullException extends ExecException {
    public FieldIsNullException() {
    }

    public FieldIsNullException(String msg) {
        super(msg);
    }
}
