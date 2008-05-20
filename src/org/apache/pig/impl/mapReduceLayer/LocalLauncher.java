package org.apache.pig.impl.mapReduceLayer;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;


public class LocalLauncher extends Launcher{
    @Override
    public boolean launchPig(PhysicalPlan<PhysicalOperator> php,
                             String grpName,
                             PigContext pc) throws PlanException,
                                                   VisitorException,
                                                   IOException,
                                                   ExecException,
                                                   JobCreationException {
        return super.launchPig(php, grpName, pc);
    }
}
