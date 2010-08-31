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
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
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
 * Visitor for checking output specification
 * In addition to throwing exception we also log them in msgCollector.
 * 
 * We assume input/output files can exist only in the top level plan.
 *
 */
public class InputOutputFileVisitor extends LOVisitor {
    
    private PigContext pigCtx;
    private CompilationMessageCollector msgCollector;
    
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

        StoreFuncInterface sf = store.getStoreFunc();
        String outLoc = store.getOutputFile().getFileName();
        int errCode = 2116;
        String validationErrStr ="Output Location Validation Failed for: '" + outLoc ;
        Job dummyJob;
        
        try {
            if(store.getSchema() != null){
                sf.checkSchema(new ResourceSchema(store.getSchema(), store.getSortInfo()));                
            }
            dummyJob = new Job(ConfigurationUtil.toConfiguration(pigCtx.getProperties()));
            sf.setStoreLocation(outLoc, dummyJob);
        } catch (IOException ioe) {
           	if(ioe instanceof PigException){
        		errCode = ((PigException)ioe).getErrorCode();
        	} 
        	String exceptionMsg = ioe.getMessage();
        	validationErrStr += (exceptionMsg == null) ? "" : " More info to follow:\n" +exceptionMsg;
        	msgCollector.collect(validationErrStr, MessageType.Error) ;
            throw new PlanValidationException(validationErrStr, errCode, pigCtx.getErrorSource(), ioe);
        }
        
        validationErrStr += " More info to follow:\n";
        try {
            sf.getOutputFormat().checkOutputSpecs(dummyJob);
        } catch (IOException ioe) {
            byte errSrc = pigCtx.getErrorSource();
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
        	validationErrStr  += ioe.getMessage();
        	msgCollector.collect(validationErrStr, MessageType.Error) ;
            throw new PlanValidationException(validationErrStr, errCode, errSrc, ioe);
        } catch (InterruptedException ie) {
        	validationErrStr += ie.getMessage();
        	msgCollector.collect(validationErrStr, MessageType.Error) ;
            throw new PlanValidationException(validationErrStr, errCode, pigCtx.getErrorSource(), ie);
        }
    }
}
