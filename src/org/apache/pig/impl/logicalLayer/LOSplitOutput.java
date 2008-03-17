package org.apache.pig.impl.logicalLayer;

import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;


public class LOSplitOutput extends LogicalOperator {
    private static final long serialVersionUID = 1L;

    protected int index;
    
    public LOSplitOutput(Map<OperatorKey, LogicalOperator> opTable,
                         String scope,
                         long id,
                         OperatorKey splitOpInput,
                         int index) {
        super(opTable, scope, id, splitOpInput);
        this.index = index;
    }
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder(super.toString());
        result.append(" (index: ");
        result.append(index);
        result.append(')');
        return result.toString();
    }


    @Override
    public String name() {
        StringBuilder sb = new StringBuilder();
        sb.append("SplitOutput ");
        sb.append(scope);
        sb.append("-");
        sb.append(id);
        return sb.toString();
    }

    @Override
    public TupleSchema outputSchema() {
        return opTable.get(inputs.get(index)).outputSchema();
    }

    @Override
    public int getOutputType() {
        return opTable.get(getInputs().get(0)).getOutputType();
    }

    public void visit(LOVisitor v) {
        v.visitSplitOutput(this);
    }

    public int getReadFrom() {
        return index;
    }
}