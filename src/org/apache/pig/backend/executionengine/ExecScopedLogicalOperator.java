package org.apache.pig.backend.executionengine;

import java.io.Serializable;

import org.apache.pig.impl.logicalLayer.OperatorKey;

public interface ExecScopedLogicalOperator extends Serializable {
    
    public String getScope();

    public long getId();
    
    public OperatorKey getOperatorKey();
}
