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
package org.apache.pig.backend.local.executionengine.physicalLayer.relationalOperators;

import java.util.Properties;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;

public class POStreamLocal extends POStream {

    /**
     * 
     */
    private static final long serialVersionUID = 2L;

    public POStreamLocal(OperatorKey k, ExecutableManager executableManager,
            StreamingCommand command, Properties properties) {
        super(k, executableManager, command, properties);
        // TODO Auto-generated constructor stub
    }
    
    
    /**
     * This is different from the Map-Reduce implementation of the POStream since there is no
     * push model here. POStatus_EOP signals the end of input and can be used to decide when 
     * to stop the stdin to the process
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
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
                    r.returnStatus = POStatus.STATUS_EOP;
                }
                return(r);
            }
            
            // if we are here, we haven't consumed all input to be sent
            // to the streaming binary - check if we are being called
            // from close() on the map or reduce
            //if(this.parentPlan.endOfAllInput) {
                Result r = getNextHelper(t);
                if(r.returnStatus == POStatus.STATUS_EOP) {
                    // we have now seen *ALL* possible input
                    // check if we ever had any real input
                    // in the course of the map/reduce - if we did
                    // then "initialized" will be true. If not, just
                    // send EOP down.
                    if(initialized) {
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
                            r.returnStatus = POStatus.STATUS_EOP;
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
                    r.returnStatus = POStatus.STATUS_EOP;
                }
                return r;
//            } else {
//                // we are not being called from close() - so
//                // we must be called from either map() or reduce()
//                // get the next Result from helper
//                Result r = getNextHelper(t);
//                if(r.returnStatus == POStatus.STATUS_EOS) {
//                    // If we received EOS, it means all output
//                    // from the streaming binary has been sent to us
//                    // So we can send an EOP to the successor in
//                    // the pipeline and also note this condition
//                    // for future calls
//                    r.returnStatus = POStatus.STATUS_EOP;
//                    allOutputFromBinaryProcessed  = true;
//                }
//                return r;
//            }
            
        } catch(Exception e) {
            throw new ExecException("Error while trying to get next result in POStream", e);
        }
            
        
    }

    

}
