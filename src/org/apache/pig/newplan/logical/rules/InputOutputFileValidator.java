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
package org.apache.pig.newplan.logical.rules;

import java.io.IOException;

import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.OverwritableStoreFunc;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class InputOutputFileValidator implements LogicalRelationalNodeValidator {
    private PigContext pigCtx;
    OperatorPlan plan;
    public InputOutputFileValidator(OperatorPlan plan, PigContext pigContext) {
        pigCtx = pigContext;
        this.plan = plan;
    }

    public void validate() throws FrontendException {
        InputOutputFileVisitor visitor = new InputOutputFileVisitor(plan);
        visitor.visit();
    }
    
    class InputOutputFileVisitor extends LogicalRelationalNodesVisitor {

        protected InputOutputFileVisitor(OperatorPlan plan)
                throws FrontendException {
            super(plan, new DepthFirstWalker(plan));
        }

        @Override
        public void visit(LOStore store) throws FrontendException {
            StoreFuncInterface sf = store.getStoreFunc();
            String outLoc = store.getOutputSpec().getFileName();
            int errCode = 2116;
            String validationErrStr ="Output Location Validation Failed for: '" + outLoc ;
            Job dummyJob;
            
            try {
                if(store.getSchema() != null){
                    sf.checkSchema(new ResourceSchema(store.getSchema(), store.getSortInfo()));                
                }
                dummyJob = new Job(ConfigurationUtil.toConfiguration(pigCtx.getProperties()));
                sf.setStoreLocation(outLoc, dummyJob);
            } catch (Exception ioe) {
                if(ioe instanceof PigException){
                    errCode = ((PigException)ioe).getErrorCode();
                } 
                String exceptionMsg = ioe.getMessage();
                validationErrStr += (exceptionMsg == null) ? "" : " More info to follow:\n" +exceptionMsg;
                throw new VisitorException(store, validationErrStr, errCode, pigCtx.getErrorSource(), ioe);
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
                
                boolean shouldThrowException = true;
                if (sf instanceof OverwritableStoreFunc) {
                    if (((OverwritableStoreFunc) sf).shouldOverwrite()) {
                        if (ioe instanceof FileAlreadyExistsException
                                || ioe instanceof org.apache.hadoop.fs.FileAlreadyExistsException) {
                            shouldThrowException = false;
                        }
                    }
                }
                if (shouldThrowException) {
                    validationErrStr += ioe.getMessage();
                    throw new VisitorException(store, validationErrStr,
                            errCode, errSrc, ioe);
                }

            } catch (InterruptedException ie) {
                validationErrStr += ie.getMessage();
                throw new VisitorException(store, validationErrStr, errCode, pigCtx.getErrorSource(), ie);
            }
        }
    }
}
