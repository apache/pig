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

import org.apache.pig.ExecType; 
import org.apache.pig.PigException;
import org.apache.pig.impl.PigContext ;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.impl.plan.PlanValidationException;

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
     * The logic here is just to check that the file(s) do not exist
     */
    @Override
    protected void visit(LOStore store) throws PlanValidationException{
        // make sure that the file doesn't exist
        String filename = store.getOutputFile().getFileName() ;
        
        try {
            if (checkFileExists(filename)) {
                byte errSrc = pigCtx.getErrorSource();
                int errCode = 0;
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
                String msg = "The output file(s): " + filename 
                + " already exists";
                msgCollector.collect(msg, MessageType.Error) ;
                throw new PlanValidationException(msg, errCode, errSrc);                
            }
        } catch (PlanValidationException pve) {
            throw pve;
        } catch (IOException ioe) {
            byte errSrc = pigCtx.getErrorSource();
            int errCode = 0;
            switch(errSrc) {
            case PigException.BUG:
                errCode = 2003;
                break;
            case PigException.REMOTE_ENVIRONMENT:
                errCode = 6001;
                break;
            case PigException.USER_ENVIRONMENT:
                errCode = 4001;
                break;
            }

            String msg = "Cannot read from the storage where the output " 
                    + filename + " will be stored ";
            msgCollector.collect(msg, MessageType.Error) ;
            throw new PlanValidationException(msg, errCode, errSrc, ioe);
        } catch (Exception e) {
            int errCode = 2116;
            String msg = "Unexpected error. Could not check for the existence of the file(s): " + filename;
            msgCollector.collect(msg, MessageType.Error) ;
            throw new PlanValidationException(msg, errCode, PigException.BUG, e);
        }
    }

    /***
     * Check if the file(s) exist. There are two cases :-
     * 1) Exact match
     * 2) Globbing match
     * TODO: Add globbing support in local execution engine 
     * and then make this check for local FS support too
     */
    private boolean checkFileExists(String filename) throws IOException {
        if (pigCtx.getExecType() == ExecType.LOCAL) {
            ElementDescriptor elem = pigCtx.getLfs().asElement(filename) ;
            return elem.exists() ;
        }
        else if (pigCtx.getExecType() == ExecType.MAPREDUCE) {
            // TODO: Have to put the staging from local to HDFS somewhere else
            // This does actual file check + glob check
            return FileLocalizer.fileExists(filename, pigCtx) ;
        }
        else { // if ExecType is something else) 
            throw new RuntimeException("Undefined state in " + this.getClass()) ;
        }
    }

}
