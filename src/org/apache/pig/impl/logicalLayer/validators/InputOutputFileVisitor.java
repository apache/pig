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
package org.apache.pig.impl.logicalLayer.validators;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.PigException;
import org.apache.pig.StoreFunc;
import org.apache.pig.impl.PigContext ;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;

/***
 * Visitor for checking input/output files
 * Exceptions in here do not affect later operations
 * so we don't throw any exception but log all of 
 * them in msgCollector.
 * 
 * We assume input/output files can exist only in the top level plan.
 *
 */
public class InputOutputFileVisitor extends LOVisitor {
    
    private PigContext pigCtx = null ;
    private CompilationMessageCollector msgCollector = null ;
    
    public InputOutputFileVisitor(LogicalPlan plan,
                                CompilationMessageCollector messageCollector,
                                PigContext pigContext) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
        pigCtx = pigContext ;
        msgCollector = messageCollector ;
        
    }
   
    /***
     * The logic here is to delegate the validation of output specification
     * to output format implementation.
     */
    @Override
    protected void visit(LOStore store) throws PlanValidationException{

        StoreFunc sf = store.getStoreFunc();
        String outLoc = store.getOutputFile().getFileName();
        Job dummyJob;
        String errMsg = "Unexpected error. Could not validate the output " +
        		"specification for: "+outLoc;
        int errCode = 2116;
        
        try {
            dummyJob = new Job(ConfigurationUtil.toConfiguration(pigCtx.getProperties()));
            sf.setStoreLocation(outLoc, dummyJob);
        } catch (IOException ioe) {
            msgCollector.collect(errMsg, MessageType.Error) ;
            throw new PlanValidationException(errMsg, errCode, pigCtx.getErrorSource(), ioe);
        }
        try {
            sf.getOutputFormat().checkOutputSpecs(dummyJob);
        } catch (IOException ioe) {
            byte errSrc = pigCtx.getErrorSource();
            errCode = 0;
            switch(errSrc) {
            case PigException.BUG:
                errCode = 2002;
                break;
            case PigException.REMOTE_ENVIRONMENT:
                errCode = 6000;
                break;
            case PigException.USER_ENVIRONMENT:
                errCode = 4000;
                break;
            }
            errMsg = "Output specification '"+outLoc+"' is invalid or already exists";
            msgCollector.collect(errMsg, MessageType.Error) ;
            throw new PlanValidationException(errMsg, errCode, errSrc, ioe);
        } catch (InterruptedException ie) {
            msgCollector.collect(errMsg, MessageType.Error) ;
            throw new PlanValidationException(errMsg, errCode, pigCtx.getErrorSource(), ie);
        }
    }
}
