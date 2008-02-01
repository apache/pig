package org.apache.pig.backend.local.executionengine;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;

public class LocalJob implements ExecJob {

    protected DataBag results;
    protected JOB_STATUS status;
    
    public LocalJob(DataBag results, JOB_STATUS status) {
        this.results = results;
        this.status = status;
    }
    
    @Override
    public JOB_STATUS getStatus() {
        return status;
    }
    
    @Override
    public boolean hasCompleted() throws ExecException {
        return true;
    }
    
    @Override
    public Iterator<Tuple> getResults() throws ExecException {
        return this.results.content();
    }

    @Override
    public Properties getContiguration() {
        Properties props = new Properties();
        return props;
    }

    @Override
    public Map<String, Object> getStatistics() {
    	throw new UnsupportedOperationException();
    }

    @Override
    public void completionNotification(Object cookie) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void kill() throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void getLogs(OutputStream log) throws ExecException {
    	throw new UnsupportedOperationException();
    }
    
    @Override
    public void getSTDOut(OutputStream out) throws ExecException {
    	throw new UnsupportedOperationException();
    }
    
    @Override
    public void getSTDError(OutputStream error) throws ExecException {
    	throw new UnsupportedOperationException();
    }
}
