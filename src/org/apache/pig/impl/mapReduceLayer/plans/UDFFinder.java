package org.apache.pig.impl.mapReduceLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.physicalLayer.plans.ExprPlan;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POGenerate;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POSort;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class UDFFinder extends PhyPlanVisitor {
    List<String> UDFs;
    DepthFirstWalker<PhysicalOperator, PhysicalPlan<PhysicalOperator>> dfw;
    UDFFinderForExpr udfFinderForExpr;
    
    public UDFFinder(){
        this(null, null);
    }
    
    public UDFFinder(ExprPlan plan,
            PlanWalker<ExpressionOperator, ExprPlan> walker) {
        super(plan, walker);
        UDFs = new ArrayList<String>();
        dfw = new DepthFirstWalker<PhysicalOperator, PhysicalPlan<PhysicalOperator>>(null);
        udfFinderForExpr = new UDFFinderForExpr();
    }

    public List<String> getUDFs() {
        return UDFs;
    }
    
    public void setPlan(PhysicalPlan<PhysicalOperator> plan){
        mPlan = plan;
        dfw.setPlan(plan);
        mCurrentWalker = dfw;
        UDFs.clear();
    }
    
    private void addUDFsIn(ExprPlan ep) throws VisitorException{
        udfFinderForExpr.setPlan(ep);
        udfFinderForExpr.visit();
        UDFs.addAll(udfFinderForExpr.getUDFs());
    }

    @Override
    public void visitFilter(POFilter op) throws VisitorException {
        addUDFsIn(op.getPlan());
    }

    @Override
    public void visitGenerate(POGenerate op) throws VisitorException {
        List<ExprPlan> eps = op.getInputPlans();
        for (ExprPlan ep : eps) {
            addUDFsIn(ep);
        }
    }

    @Override
    public void visitSort(POSort op) throws VisitorException {
        if(op.getMSortFunc()!=null)
            UDFs.add(op.getMSortFunc().getFuncSpec());
    }
    
    
}
