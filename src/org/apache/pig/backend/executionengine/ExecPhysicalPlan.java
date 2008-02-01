package org.apache.pig.backend.executionengine;

import java.util.Map;
import java.util.Properties;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.pig.impl.logicalLayer.OperatorKey;

/**
 * 
 *
 */

public interface ExecPhysicalPlan extends Serializable {

    /*
     * As the fron end is not really supposed to do much with the physical representation
     * the handle to the physical plan can just be an GUId in the back-end state, which
     * brings up the problem is persistency...
     */
        
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
     * Some properties of the job may be changed, say the priority...
     * 
     * @param configuration
     * @throws some changes may not be allowed, for instance the some
     * job-level properties cannot override Execution-Engine-level properties
     * or maybe some properties can only be changes only in certain states of the
     * job, say, once the job is started, parallelism level may not be changed...
     */
    public void updateConfiguration(Properties configuration)
        throws ExecException;
                
    /**    
     * To provide an "explanation" about how the physical plan has been constructed
     * 
     */
    public void explain(OutputStream out);
    
    public Map<OperatorKey, ExecPhysicalOperator> getOpTable();
    
    public OperatorKey getRoot();
}
