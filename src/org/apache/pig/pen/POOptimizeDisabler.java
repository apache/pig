package org.apache.pig.pen;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class POOptimizeDisabler extends LogicalRelationalNodesVisitor {

    public POOptimizeDisabler(OperatorPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOJoin join) throws FrontendException {
        // use HASH join only
        join.resetJoinType();
    }

    @Override
    public void visit(LOCogroup cg) throws FrontendException {
        // use regular group only
        cg.resetGroupType();
    }
}
