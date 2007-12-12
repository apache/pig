package org.apache.pig.backend.executionengine;

import java.io.Serializable;

public interface ExecutionEngineLogicalOperator extends Serializable {

    // catenation of group and name is a GUId for the operator
    public String getScope();
    public long getId();
    
    public ExecutionEngineLogicalOperator childAt(int position);
}
