package org.apache.pig.backend.executionengine;

import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.OperatorKey;

/**
 * A logical plan has a root of the operator tree. 
 * A table of operators collects the set of operators making up a logical tree.
 * It is possible to navigate a logical tree by looking up the input operator 
 * keys of the logical operator in the operator table.
 *
 */
public interface ExecLogicalPlan extends  Serializable {
    
    public OperatorKey getRoot();
    
    public Map<OperatorKey, LogicalOperator> getOpTable();
    
    public void explain(OutputStream out);
}
