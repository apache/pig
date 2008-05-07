/**
 * 
 */
package org.apache.pig.backend.local.executionengine;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.POMapreduce;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.POVisitor;

/**
 * POVisitor calling instantiateFunc on all EvalSpec members of visited nodes.
 */
public class InstantiateFuncCallerPOVisitor extends POVisitor {

    private FunctionInstantiator instantiator;

    protected InstantiateFuncCallerPOVisitor(FunctionInstantiator instantiator,
            Map<OperatorKey, ExecPhysicalOperator> opTable) {
        super(opTable);
        this.instantiator = instantiator;
    }

    private void callInstantiateFunc(EvalSpec spec) {
        try {
            spec.instantiateFunc(instantiator);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void visitCogroup(POCogroup g) {
        super.visitCogroup(g);
        for (EvalSpec es : g.specs)
            callInstantiateFunc(es);
    }

    @Override
    public void visitMapreduce(POMapreduce mr) {
        super.visitMapreduce(mr);
        for (EvalSpec es : mr.groupFuncs)
            callInstantiateFunc(es);
    }

    @Override
    public void visitSort(POSort s) {
        super.visitSort(s);
        callInstantiateFunc(s.sortSpec);
    }

    @Override
    public void visitEval(POEval e) {
        super.visitEval(e);
        callInstantiateFunc(e.spec);
    }
}
