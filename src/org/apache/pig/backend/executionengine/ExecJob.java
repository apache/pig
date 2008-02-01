package org.apache.pig.backend.executionengine;

import java.util.Iterator;
import java.util.Properties;
import java.util.Map;
import java.io.OutputStream;

import org.apache.pig.data.Tuple;

/**
 * Abstraction on a job that the execution engine runs. It allows the front-end to
 * retrieve information on job status and manage a running job.
 *
 */

public interface ExecJob {

    public enum JOB_STATUS {
        QUEUED,
        RUNNING,
        SUSPENDED,
        TERMINATED,
        FAILED,
        COMPLETED,
    }

    public static final String PROGRESS_KEY = "job.progress";
    
    public JOB_STATUS getStatus();

    /**
     * true is the physical plan has executed successfully and results are ready
     * to be retrieved
     * 
     * @return
     * @throws ExecException
     */
    public boolean hasCompleted() throws ExecException;
    
    /**
     * if query has executed successfully we want to retrieve the results
     * via iterating over them. 
     * 
     * @return
     * @throws ExecException
     */
    public Iterator<Tuple> getResults() throws ExecException;

    /**
     * Get configuration information
     * 
     * @return
     */    
    public Properties getContiguration();

    /**
     * Can be information about the state (not submitted, e.g. the execute method
     * has not been called yet; not running, e.g. execute has been issued, 
     * but job is waiting; running...; completed; aborted...; progress information
     * 
     * @return
     */
    public Map<String, Object> getStatistics();

    /**
     * hook for asynchronous notification of job completion pushed from the back-end
     */
    public void completionNotification(Object cookie);
    
    /**
     * Kills current job.
     * 
     * @throws ExecException
     */
    public void kill() throws ExecException;
    
    /**
     * Collecting various forms of outputs
     */
    public void getLogs(OutputStream log) throws ExecException;
    
    public void getSTDOut(OutputStream out) throws ExecException;
    
    public void getSTDError(OutputStream error) throws ExecException;
}
