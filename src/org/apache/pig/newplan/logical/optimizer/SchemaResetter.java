package org.apache.pig.newplan.logical.optimizer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.AllSameExpressionVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class SchemaResetter extends LogicalRelationalNodesVisitor {

    public SchemaResetter(OperatorPlan plan) {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOLoad load) throws IOException {
        load.resetSchema();
        load.getSchema();
    }

    @Override
    public void visit(LOFilter filter) throws IOException {
        filter.resetSchema();
        FieldSchemaResetter fsResetter = new FieldSchemaResetter(filter.getFilterPlan());
        fsResetter.visit();
        filter.getSchema();
    }
    
    @Override
    public void visit(LOStore store) throws IOException {
        store.resetSchema();
        store.getSchema();
    }
    
    @Override
    public void visit(LOJoin join) throws IOException {
        join.resetSchema();
        Collection<LogicalExpressionPlan> joinPlans = join.getExpressionPlans();
        for (LogicalExpressionPlan joinPlan : joinPlans) {
            FieldSchemaResetter fsResetter = new FieldSchemaResetter(joinPlan);
            fsResetter.visit();
        }
        join.getSchema();
    }
    
    @Override
    public void visit(LOForEach foreach) throws IOException {
        foreach.resetSchema();
        OperatorPlan innerPlan = foreach.getInnerPlan();
        PlanWalker newWalker = currentWalker.spawnChildWalker(innerPlan);
        pushWalker(newWalker);
        currentWalker.walk(this);
        popWalker();
        foreach.getSchema();
    }
    
    @Override
    public void visit(LOGenerate gen) throws IOException {
        gen.resetSchema();
        List<LogicalExpressionPlan> genPlans = gen.getOutputPlans();
        for (LogicalExpressionPlan genPlan : genPlans) {
            FieldSchemaResetter fsResetter = new FieldSchemaResetter(genPlan);
            fsResetter.visit();
        }
        gen.getSchema();
    }
    
    @Override
    public void visit(LOInnerLoad load) throws IOException {
        load.resetSchema();
        load.getProjection().resetFieldSchema();
        load.getSchema();
    }

    @Override
    public void visit(LOCogroup loCogroup) throws IOException {
        loCogroup.resetSchema();
        MultiMap<Integer, LogicalExpressionPlan> expPlans = loCogroup.getExpressionPlans();
        for (LogicalExpressionPlan expPlan : expPlans.values()) {
            FieldSchemaResetter fsResetter = new FieldSchemaResetter(expPlan);
            fsResetter.visit();
        }
        loCogroup.getSchema();
    }
    
    @Override
    public void visit(LOSplit loSplit) throws IOException {
        loSplit.resetSchema();
        loSplit.getSchema();
    }
    
    @Override
    public void visit(LOSplitOutput loSplitOutput) throws IOException {
        loSplitOutput.resetSchema();
        FieldSchemaResetter fsResetter = new FieldSchemaResetter(loSplitOutput.getFilterPlan());
        fsResetter.visit();
        loSplitOutput.getSchema();
    }
    
    @Override
    public void visit(LOUnion loUnion) throws IOException {
        loUnion.resetSchema();
        loUnion.getSchema();
    }
    
    @Override
    public void visit(LOSort loSort) throws IOException {
        loSort.resetSchema();
        List<LogicalExpressionPlan> sortPlans = loSort.getSortColPlans();
        for (LogicalExpressionPlan sortPlan : sortPlans) {
            FieldSchemaResetter fsResetter = new FieldSchemaResetter(sortPlan);
            fsResetter.visit();
        }
        loSort.getSchema();
    }
    
    @Override
    public void visit(LODistinct loDistinct) throws IOException {
        loDistinct.resetSchema();
        loDistinct.getSchema();
    }
    
    @Override
    public void visit(LOLimit loLimit) throws IOException {
        loLimit.resetSchema();
        loLimit.getSchema();
    }
    
    @Override
    public void visit(LOCross loCross) throws IOException {
        loCross.resetSchema();
        loCross.getSchema();
    }
    
    @Override
    public void visit(LOStream loStream) throws IOException {
        loStream.resetSchema();
        loStream.getSchema();
    }
}

class FieldSchemaResetter extends AllSameExpressionVisitor {

    protected FieldSchemaResetter(OperatorPlan p) {
        super(p, new DependencyOrderWalker(p));
    }

    @Override
    protected void execute(LogicalExpression op) throws IOException {
        op.resetFieldSchema();
        op.getFieldSchema();
    }

}