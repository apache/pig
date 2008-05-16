package org.apache.pig.impl.mapReduceLayer;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Main class that launches pig for Map Reduce
 *
 */
public class MapReduceLauncher extends Launcher{

    @Override
    public void launchPig(PhysicalPlan<PhysicalOperator> php, String grpName, PigContext pc) throws PlanException, VisitorException, IOException, ExecException, JobCreationException {
        super.launchPig(php, grpName, pc);
    }
}
