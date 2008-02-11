package org.apache.pig.backend.hadoop.executionengine;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.executionengine.ExecPhysicalPlan;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POPrinter;
import org.apache.pig.impl.physicalLayer.POVisitor;
import org.apache.pig.impl.logicalLayer.OperatorKey;

public class MapRedPhysicalPlan implements ExecPhysicalPlan {
    private static final long serialVersionUID = 1;
    
    protected OperatorKey root;
    protected Map<OperatorKey, ExecPhysicalOperator> opTable;    
    
    MapRedPhysicalPlan(OperatorKey root,
                       Map<OperatorKey, ExecPhysicalOperator> opTable) {
        this.root = root;
        this.opTable = opTable;
    }
    
    public Properties getConfiguration() {
        // TODO
        return null;
    }

    public void updateConfiguration(Properties configuration)
        throws ExecException {
        // TODO
    }
             
    public void explain(OutputStream out) {
        POVisitor lprinter = new POPrinter(opTable, new PrintStream(out));
        
        ((PhysicalOperator)opTable.get(root)).visit(lprinter);
    }
    
    public Map<OperatorKey, ExecPhysicalOperator> getOpTable() {
        return opTable;
    }
    
    public OperatorKey getRoot() {
        return this.root;
    }
}

