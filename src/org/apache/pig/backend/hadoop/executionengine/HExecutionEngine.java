package org.apache.pig.backend.hadoop.executionengine;

import java.util.Collection;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.executionengine.ExecutionEngineException;
import org.apache.pig.backend.executionengine.ExecutionEngineLogicalPlan;
import org.apache.pig.backend.executionengine.ExecutionEnginePhysicalPlan;

import org.apache.pig.impl.physicalLayer.IntermedResult;

public class HExecutionEngine implements ExecutionEngine {

    protected Map<String, IntermedResult> aliases;
    
    public HExecutionEngine() {
        aliases = new HashMap<String, IntermedResult>();
    }
    
    public void init() {
        // nothing to do
    }

    public void close() {
        // TODO:
        // check for running jobs and kill them all?
    }

    public Properties getConfiguration() {
        // TODO
        return null;
    }
        
    public void updateConfiguration(Properties newConfiguration) 
        throws ExecutionEngineException {
        // TODO
    }
        
    public Properties getStatistics() throws ExecutionEngineException {
        // TODO
        Properties stats = new Properties();
        return stats;
    }

    public ExecutionEnginePhysicalPlan compile(ExecutionEngineLogicalPlan plan,
                                               Properties properties)
            throws ExecutionEngineException {
        // TODO
        // first step is to visit the logical plan:
        //      if LOSplit: need to add more aliases in the table
        //
        // compilation process uses the alias table
        //
        // emit the physical representation, instead of finding the intermed results in LORead
        // we need to look into the table.
        //
        return null;
    }
    
    public Collection<ExecutionEnginePhysicalPlan> physicalPlans () 
        throws ExecutionEngineException {
        // TODO
        return null;
    }
}
