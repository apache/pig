/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;


import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.pig.PigException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;

public class POStream extends PhysicalOperator {
    private static final long serialVersionUID = 2L;
    
    private static final Result EOP_RESULT = new Result(POStatus.STATUS_EOP, null);

    private String executableManagerStr;            // String representing ExecutableManager to use
    transient private ExecutableManager executableManager;    // ExecutableManager to use 
    private StreamingCommand command;               // Actual command to be run
    private Properties properties;

    private boolean initialized = false;

    protected BlockingQueue<Result> binaryOutputQueue = new ArrayBlockingQueue<Result>(1);

    protected BlockingQueue<Result> binaryInputQueue = new ArrayBlockingQueue<Result>(1);

    protected boolean allInputFromPredecessorConsumed = false;

    protected boolean allOutputFromBinaryProcessed = false;

    /**
     * This flag indicates whether streaming is done through fetching. If set,
     * {@link FetchLauncher} pulls out the data from the pipeline. Therefore we need to
     * skip the case in {@link #getNextTuple()} which is called by map() or reduce() when
     * processing the next tuple.
     */
    private boolean isFetchable;

    public POStream(OperatorKey k, ExecutableManager executableManager, 
                      StreamingCommand command, Properties properties) {
        super(k);
        this.executableManagerStr = executableManager.getClass().getName();
        this.command = command;
        this.properties = properties;

        // Setup streaming-specific properties
        if (command.getShipFiles()) {
            parseShipCacheSpecs(command.getShipSpecs(), 
                                properties, "pig.streaming.ship.files");
        }
        parseShipCacheSpecs(command.getCacheSpecs(), 
                            properties, "pig.streaming.cache.files");
    }
    
    private static void parseShipCacheSpecs(List<String> specs, 
            Properties properties, String property) {
        
        String existingValue = properties.getProperty(property, "");
        if (specs == null || specs.size() == 0) {
            return;
        }
        
        // Setup streaming-specific properties
        StringBuffer sb = new StringBuffer();
        Iterator<String> i = specs.iterator();
        // first append any existing value
        if(!existingValue.equals("")) {
            sb.append(existingValue);
            if (i.hasNext()) {
                sb.append(", ");
            }
        }
        while (i.hasNext()) {
            sb.append(i.next());
            if (i.hasNext()) {
                sb.append(", ");
            }
        }
        properties.setProperty(property, sb.toString());        
    }

    public Properties getShipCacheProperties() {
        return properties;
    }
    
    /**
     * Get the {@link StreamingCommand} for this <code>StreamSpec</code>.
     * @return the {@link StreamingCommand} for this <code>StreamSpec</code>
     */
    public StreamingCommand getCommand() {
        return command;
    }
    
    
    /* (non-Javadoc)
     * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator#getNext(org.apache.pig.data.Tuple)
     */
    @Override
    public Result getNextTuple() throws ExecException {
        // The POStream Operator works with ExecutableManager to
        // send input to the streaming binary and to get output
        // from it. To achieve a tuple oriented behavior, two queues
        // are used - one for output from the binary and one for 
        // input to the binary. In each getNext() call:
        // 1) If there is no more output expected from the binary, an EOP is
        // sent to successor
        // 2) If there is any output from the binary in the queue, it is passed
        // down to the successor
        // 3) if neither of these two are true and if it is possible to
        // send input to the binary, then the next tuple from the
        // predecessor is got and passed to the binary
        try {
            // if we are being called AFTER all output from the streaming 
            // binary has already been sent to us then just return EOP
            // The "allOutputFromBinaryProcessed" flag is set when we see
            // an EOS (End of Stream output) from streaming binary
            if(allOutputFromBinaryProcessed) {
                return new Result(POStatus.STATUS_EOP, null);
            }
            
            // if we are here AFTER all map() calls have been completed
            // AND AFTER we process all possible input to be sent to the
            // streaming binary, then all we want to do is read output from
            // the streaming binary
            if(allInputFromPredecessorConsumed) {
                Result r = binaryOutputQueue.take();
                if(r.returnStatus == POStatus.STATUS_EOS) {
                    // If we received EOS, it means all output
                    // from the streaming binary has been sent to us
                    // So we can send an EOP to the successor in
                    // the pipeline. Also since we are being called
                    // after all input from predecessor has been processed
                    // it means we got here from a call from close() in
                    // map or reduce. So once we send this EOP down, 
                    // getNext() in POStream should never be called. So
                    // we don't need to set any flag noting we saw all output
                    // from binary
                    r = EOP_RESULT;
                } else if (r.returnStatus == POStatus.STATUS_OK)
                    illustratorMarkup(r.result, r.result, 0);
                return(r);
            }
            
            // if we are here, we haven't consumed all input to be sent
            // to the streaming binary - check if we are being called
            // from close() on the map or reduce
            if(isFetchable || this.parentPlan.endOfAllInput) {
                Result r = getNextHelper((Tuple)null);
                if(r.returnStatus == POStatus.STATUS_EOP) {
                    // we have now seen *ALL* possible input
                    // check if we ever had any real input
                    // in the course of the map/reduce - if we did
                    // then "initialized" will be true. If not, just
                    // send EOP down.
                    if(getInitialized()) {
                        // signal End of ALL input to the Executable Manager's 
                        // Input handler thread
                        binaryInputQueue.put(r);
                        // note this state for future calls
                        allInputFromPredecessorConsumed  = true;
                        // look for output from binary
                        r = binaryOutputQueue.take();
                        if(r.returnStatus == POStatus.STATUS_EOS) {
                            // If we received EOS, it means all output
                            // from the streaming binary has been sent to us
                            // So we can send an EOP to the successor in
                            // the pipeline. Also since we are being called
                            // after all input from predecessor has been processed
                            // it means we got here from a call from close() in
                            // map or reduce. So once we send this EOP down, 
                            // getNext() in POStream should never be called. So
                            // we don't need to set any flag noting we saw all output
                            // from binary
                            r = EOP_RESULT;
                        }
                    }
                    
                } else if(r.returnStatus == POStatus.STATUS_EOS) {
                    // If we received EOS, it means all output
                    // from the streaming binary has been sent to us
                    // So we can send an EOP to the successor in
                    // the pipeline. Also we are being called
                    // from close() in map or reduce (this is so because
                    // only then this.parentPlan.endOfAllInput is true).
                    // So once we send this EOP down, getNext() in POStream
                    // should never be called. So we don't need to set any 
                    // flag noting we saw all output from binary
                    r = EOP_RESULT;
                } else if (r.returnStatus == POStatus.STATUS_OK)
                  illustratorMarkup(r.result, r.result, 0);
                return r;
            } else {
                // we are not being called from close() - so
                // we must be called from either map() or reduce()
                // get the next Result from helper
                Result r = getNextHelper((Tuple)null);
                if(r.returnStatus == POStatus.STATUS_EOS) {
                    // If we received EOS, it means all output
                    // from the streaming binary has been sent to us
                    // So we can send an EOP to the successor in
                    // the pipeline and also note this condition
                    // for future calls
                    r = EOP_RESULT;
                    allOutputFromBinaryProcessed  = true;
                } else if (r.returnStatus == POStatus.STATUS_OK)
                    illustratorMarkup(r.result, r.result, 0);
                return r;
            }
            
        } catch(Exception e) {
            int errCode = 2083;
            String msg = "Error while trying to get next result in POStream.";
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
            
        
    }

    public synchronized boolean getInitialized() {
        return initialized;
    }

    public synchronized void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    public Result getNextHelper(Tuple t) throws ExecException {
        try {
            synchronized(this) {
                while(true) {
                    // if there is something in binary output Queue
                    // return it
                    if(!binaryOutputQueue.isEmpty()) {
                        Result res = binaryOutputQueue.take();
                        return res;
                    }
                    
                    // check if we can write tuples to 
                    // input of the process
                    if(binaryInputQueue.remainingCapacity() > 0) {
                        
                        Result input = processInput();
                        if(input.returnStatus == POStatus.STATUS_EOP || 
                                input.returnStatus == POStatus.STATUS_ERR) {
                            return input;
                        } else {
                            // we have a tuple to send as input
                            // Only when we see the first tuple which can
                            // be sent as input to the binary we want
                            // to initialize the ExecutableManager and set
                            // up the streaming binary - this is required in 
                            // Unions due to a JOIN where there may never be
                            // any input to send to the binary in one of the map
                            // tasks - so we initialize only if we have to.
                            // initialize the ExecutableManager once
                            if(!initialized) {
                                // set up the executableManager
                                executableManager = 
                                    (ExecutableManager)PigContext.instantiateFuncFromSpec(executableManagerStr);
                                
                                try {
                                    executableManager.configure(this);
                                    executableManager.run();
                                } catch (IOException ioe) {
                                    int errCode = 2084;
                                    String msg = "Error while running streaming binary.";
                                    throw new ExecException(msg, errCode, PigException.BUG, ioe);
                                }            
                                initialized = true;
                            }
                            
                            // send this input to the streaming
                            // process
                            binaryInputQueue.put(input);
                        }
                        
                    } else {
                        
                        // wait for either input to be available
                        // or output to be consumed
                        while(binaryOutputQueue.isEmpty() && !binaryInputQueue.isEmpty())
                            wait();
                        
                    }
                }
            }
        } catch (Exception e) {
            int errCode = 2083;
            String msg = "Error while trying to get next result in POStream.";
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }
    
    public String toString() {
        return getAliasString() + "POStream" + "[" + command.toString() + "]"
                + " - " + mKey.toString();
    }
 
    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitStream(this);
        
    }

    @Override
    public String name() {
       return toString(); 
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    /**
     * 
     */
    public void finish() throws IOException {
        executableManager.close();
    }

    /**
     * @return the Queue which has input to binary
     */
    public BlockingQueue<Result> getBinaryInputQueue() {
        return binaryInputQueue;
    }

    /**
     * @return the Queue which has output from binary
     */
    public BlockingQueue<Result> getBinaryOutputQueue() {
        return binaryOutputQueue;
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
      if(illustrator != null) {
          ExampleTuple tIn = (ExampleTuple) in;
          illustrator.getEquivalenceClasses().get(eqClassIndex).add(tIn);
            illustrator.addData((Tuple) out);
      }
      return (Tuple) out;
    }

    /**
     * @return true if streaming is done through fetching
     */
    public boolean isFetchable() {
        return isFetchable;
    }

    /**
     * @param isFetchable - whether fetching is applied on POStream
     */
    public void setFetchable(boolean isFetchable) {
        this.isFetchable = isFetchable;
    }

}
