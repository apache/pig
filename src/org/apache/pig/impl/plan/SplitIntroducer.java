package org.apache.pig.impl.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOConst;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;

public class SplitIntroducer extends PlanWalker<LogicalOperator, LogicalPlan> {
    private NodeIdGenerator nodeIdGen;
    
    public SplitIntroducer(LogicalPlan plan) {
        super(plan);
        nodeIdGen = NodeIdGenerator.getGenerator();
    }
    
    private long getNextId(String scope) {
        return nodeIdGen.getNextNodeId(scope);
    }

    @Override
    public PlanWalker<LogicalOperator, LogicalPlan> spawnChildWalker(LogicalPlan plan) {
        return new SplitIntroducer(plan);
    }
    
    public void introduceImplSplits() throws VisitorException {
        List<LogicalOperator> roots = copySucs(mPlan.getRoots());
        if(roots == null) return;
        for (LogicalOperator root : roots) {
            processNode(root);
        }
    }

    @Override
    /**
     * This method is to conform to the interface.
     */
    public void walk(PlanVisitor visitor) throws VisitorException {
        throw new VisitorException(
                "This method is not to be used. This Walker does not call any visit() methods. It only alters the plan by introducing implicit splits if necessary.");
    }
    
    private void processNode(LogicalOperator root) throws VisitorException {
        if(root instanceof LOSplit || root instanceof LOSplitOutput) return;
        List<LogicalOperator> sucs = mPlan.getSuccessors(root);
        if(sucs==null) return;
        int size = sucs.size();
        if(size==0 || size==1) return;
        sucs = copySucs(mPlan.getSuccessors(root));
        disconnect(root,sucs);
        String scope = root.getOperatorKey().scope;
        LOSplit splitOp = new LOSplit(mPlan, new OperatorKey(scope, getNextId(scope)), new ArrayList<LogicalOperator>());
        mPlan.add(splitOp);
        try {
            mPlan.connect(root, splitOp);
            int index = -1;
            for (LogicalOperator operator : sucs) {
                LogicalPlan condPlan = new LogicalPlan();
                LOConst cnst = new LOConst(mPlan,new OperatorKey(scope, getNextId(scope)), true);
                cnst.setType(DataType.BOOLEAN);
                condPlan.add(cnst);
                LOSplitOutput splitOutput = new LOSplitOutput(mPlan, new OperatorKey(scope, getNextId(scope)), ++index, condPlan);
                splitOp.addOutput(splitOutput);
                mPlan.add(splitOutput);
                mPlan.connect(splitOp, splitOutput);
                mPlan.connect(splitOutput, operator);
            }
        } catch (PlanException e) {
            throw new VisitorException(e);
        }
    }
    private List<LogicalOperator> copySucs(List<LogicalOperator> successors){
        ArrayList<LogicalOperator> ret = new ArrayList<LogicalOperator>();
        for (LogicalOperator operator : successors) {
            ret.add(operator);
        }
        return ret;
    }
    
    private void disconnect(LogicalOperator from, List<LogicalOperator> successors) {
        for (LogicalOperator operator : successors) {
            mPlan.disconnect(from, operator);
        }
    }
    
   /* public static void main(String[] args) throws ExecException, IOException, FrontendException {
        PigServer ser = new PigServer(ExecType.LOCAL);
        ser.registerQuery("A = load 'file:/etc/passwd' using PigStorage(':');");
        ser.registerQuery("B = foreach A generate $0;");
        ser.registerQuery("C = foreach A generate $1;");
        ser.registerQuery("D = group B by $0, C by $0;");
        LogicalPlan lp = ser.getPlanFromAlias("D", "Testing");
        lp.explain(System.out, System.err);
        LogicalPlan fixedLp = ser.compileLp(lp, "Testing");
        System.out.println();
        fixedLp.explain(System.out, System.err);
    }*/
}
