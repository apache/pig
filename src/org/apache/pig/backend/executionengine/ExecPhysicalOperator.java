package org.apache.pig.backend.executionengine;

public interface ExecPhysicalOperator {

    public String getScope();
    
    public long getId();
}
