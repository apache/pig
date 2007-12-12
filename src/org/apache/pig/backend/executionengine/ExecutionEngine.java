package org.apache.pig.backend.executionengine;

import java.util.Collection;
import java.util.Properties;

/**
 * This is the main interface that various execution engines
 * need to support and it is also the main interface that Pig
 * will need to use to submit jobs for execution, retrieve information
 * about their progress and possibly terminate them.
 *
 */
public interface ExecutionEngine {
    /**
     * Place holder for possible initialization activities.
     */
    public void init();

    /**
     * Clean-up and releasing of resources.
     */
    public void close();

        
    /**
     * Provides configuration information about the execution engine itself.
     * 
     * @return - information about the configuration used to connect to execution engine
     */
    public Properties getConfiguration();
        
    /**
     * Provides a way to dynamically change configuration parameters
     * at the Execution Engine level.
     * 
     * @param newConfiguration - the new configuration settings
     * @throws when configuration conflicts are detected
     * 
     */
    public void updateConfiguration(Properties newConfiguration) 
        throws ExecutionEngineException;
        
    /**
     * Provides statistics on the Execution Engine: number of nodes,
     * node failure rates, average load, average job wait time...
     * @return ExecutionEngineProperties
     */
    public Properties getStatistics() throws ExecutionEngineException;

    /**
     * Compiles a logical plan into a physical plan, given a set of configuration
     * properties that apply at the plan-level. For instance desired degree of 
     * parallelism for this plan, which could be different from the "default"
     * one set at the execution engine level.
     * 
     * @param logical plan
     * @param properties
     * @return physical plan
     */
    public ExecutionEnginePhysicalPlan compile(ExecutionEngineLogicalPlan plan,
                                               Properties properties)
        throws ExecutionEngineException;
        
    /**
     * This may be useful to support admin functionalities.
     * 
     * @return a collection of jobs "known" to the execution engine,
     * say jobs currently queued up or running (this can be determined 
     * by the obtaining the properties of the job)
     * 
     * @throws ExecutionEngineException maybe the user does not have privileges
     * to obtain this information...
     */
    public Collection<ExecutionEnginePhysicalPlan> physicalPlans () 
        throws ExecutionEngineException;
}
