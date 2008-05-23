package org.apache.pig.impl.mapReduceLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.physicalLayer.plans.ExprPlan;
import org.apache.pig.impl.physicalLayer.plans.ExprPlanVisitor;
import org.apache.pig.impl.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class UDFFinderForExpr extends ExprPlanVisitor {
    List<String> UDFs;
    DepthFirstWalker<ExpressionOperator, ExprPlan> dfw;
    
    public UDFFinderForExpr(){
        this(null, null);
    }
    
    public UDFFinderForExpr(ExprPlan plan,
            PlanWalker<ExpressionOperator, ExprPlan> walker) {
        super(plan, walker);
        UDFs = new ArrayList<String>();
        dfw = new DepthFirstWalker<ExpressionOperator, ExprPlan>(null);
    }

    @Override
    public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
        UDFs.add(userFunc.getFuncSpec());
    }

    public List<String> getUDFs() {
        return UDFs;
    }
    
    public void setPlan(ExprPlan plan){
        mPlan = plan;
        dfw.setPlan(plan);
        mCurrentWalker = dfw;
        UDFs.clear();
    }
}
