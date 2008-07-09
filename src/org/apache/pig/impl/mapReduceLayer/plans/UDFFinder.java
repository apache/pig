package org.apache.pig.impl.mapReduceLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.impl.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.impl.physicalLayer.relationalOperators.POSort;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class UDFFinder extends PhyPlanVisitor {
    List<String> UDFs;
    DepthFirstWalker<PhysicalOperator, PhysicalPlan> dfw;
    
    public UDFFinder(){
        this(null, null);
    }
    
    public UDFFinder(PhysicalPlan plan, PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
        super(plan, walker);
        UDFs = new ArrayList<String>();
        dfw = new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(null);
    }

    public List<String> getUDFs() {
        return UDFs;
    }
    
    public void setPlan(PhysicalPlan plan){
        mPlan = plan;
        dfw.setPlan(plan);
        mCurrentWalker = dfw;
        UDFs.clear();
    }
    
    /*private void addUDFsIn(PhysicalPlan ep) throws VisitorException{
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
        List<PhysicalPlan> eps = op.getInputPlans();
        for (PhysicalPlan ep : eps) {
            addUDFsIn(ep);
        }
    }*/

    @Override
    public void visitSort(POSort op) throws VisitorException {
        if(op.getMSortFunc()!=null)
            UDFs.add(op.getMSortFunc().getFuncSpec());
    }
    
    @Override
    public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
        UDFs.add(userFunc.getFuncSpec());
    }

    @Override
    public void visitComparisonFunc(POUserComparisonFunc compFunc) throws VisitorException {
        UDFs.add(compFunc.getFuncSpec());
    }
    
    
}
