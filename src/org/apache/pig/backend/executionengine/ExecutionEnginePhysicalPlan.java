package org.apache.pig.backend.executionengine;

import java.util.Properties;
import java.util.Iterator;

import org.apache.pig.data.Tuple;

public interface ExecutionEnginePhysicalPlan {

    /*
     * As the fron end is not really supposed to do much with the physical representation
     * the handle to the physical plan can just be an GUId in the back-end state, which
     * brings up the problem is persistency...
     */
    
    
    /**
     * Execute the physical plan.
     * This is non-blocking. See getStatistics to pull information
     * about the job.
     * 
     * @throws
     */
    public void execute() throws ExecutionEngineException;

    public void submit() throws ExecutionEngineException;
    
    /**
     * A job may have properties, like a priority, degree of parallelism...
     * Some of such properties may be inherited from the ExecutionEngine
     * configuration, other may have been set specifically for this job.
     * For instance, a job scheduler may attribute low priority to
     * jobs automatically started for maintenance purpose.
     * 
     * @return set of properties
     */
    public Properties getConfiguration();
    
    /**
     * true is the physical plan has executed successfully and results are ready
     * to be retireved
     * 
     * @return
     * @throws ExecutionEngineException
     */
    public boolean hasExecuted() throws ExecutionEngineException;
    
    /**
     * if query has executed successfully we want to retireve the results
     * via iterating over them. 
     * 
     * @return
     * @throws ExecutionEngineException
     */
    public Iterator<Tuple> getResults() throws ExecutionEngineException;
    
    /**
     * Some properties of the job may be changed, say the priority...
     * 
     * @param configuration
     * @throws some changes may not be allowed, for instance the some
     * job-level properties cannot override Execution-Engine-level properties
     * or maybe some properties can only be changes only in certain states of the
     * job, say, once the job is started, parallelism level may not be changed...
     */
    public void updateConfiguration(Properties configuration)
        throws ExecutionEngineException;
    
    /**
     * Hook to provide asynchronous notifications.
     * 
     */
    public void notify(ExecutionEngineNotificationEvent event);
        
    /**
     * Kills current job.
     * 
     * @throws ExecutionEngineException
     */
    public void kill() throws ExecutionEngineException;
        
    /**
     * Can be information about the state (not submitted, e.g. the execute method
     * has not been called yet; not running, e.g. execute has been issued, 
     * but job is waiting; running...; completed; aborted...; progress information
     * 
     * @return
     */
    public Properties getStatistics();
    
    /**    
     * To provide an "explanation" about how the physical plan has been constructed
     * 
     */
    public void explain();
}
