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
package org.apache.pig.impl.logicalLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.StoreFunc; 
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOStore extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private FileSpec mOutputFile;

    // If we know how to reload the store, here's how. The lFile
    // FileSpec is set in PigServer.postProcess. It can be used to
    // reload this store, if the optimizer has the need.
    private FileSpec mInputSpec;

    transient private StoreFunc mStoreFunc;
    private static Log log = LogFactory.getLog(LOStore.class);

    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param outputFileSpec
     *            the file to be stored
     */
    public LOStore(LogicalPlan plan, OperatorKey key,
            FileSpec outputFileSpec) throws IOException {
        super(plan, key);

        mOutputFile = outputFileSpec;

        // TODO
        // The code below is commented out as PigContext pulls in
        // HExecutionEngine which in turn is completely commented out
        // Also remove the commented out import org.apache.pig.impl.PigContext

        try { 
             mStoreFunc = (StoreFunc) PigContext.instantiateFuncFromSpec(outputFileSpec.getFuncSpec()); 
        } catch (Exception e) { 
            IOException ioe = new IOException(e.getMessage()); 
            ioe.setStackTrace(e.getStackTrace());
            throw ioe; 
        }
    }

    public FileSpec getOutputFile() {
        return mOutputFile;
    }
    
    public void setOutputFile(FileSpec outputFileSpec) throws IOException {
        try { 
            mStoreFunc = (StoreFunc) PigContext.instantiateFuncFromSpec(outputFileSpec.getFuncSpec()); 
       } catch (Exception e) { 
           IOException ioe = new IOException(e.getMessage()); 
           ioe.setStackTrace(e.getStackTrace());
           throw ioe; 
       }
       mOutputFile = outputFileSpec;
    }

    public StoreFunc getStoreFunc() {
        return mStoreFunc;
    }

    @Override
    public String name() {
        return "Store " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        //throw new RuntimeException("Internal error: Requested schema of a "
         //       + "store operator.");
        return mPlan.getPredecessors(this).get(0).getSchema();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    public void setInputSpec(FileSpec in) {
        mInputSpec = in;
    }

    public FileSpec getInputSpec() {
        return mInputSpec;
    }
    
    @Override
    public ProjectionMap getProjectionMap() {
        Schema outputSchema;
        try {
            outputSchema = getSchema();
        } catch (FrontendException fee) {
            return null;
        }
        if(outputSchema == null) {
            return null;
        }
        
        Schema inputSchema = null;        
        
        List<LogicalOperator> predecessors = (ArrayList<LogicalOperator>)mPlan.getPredecessors(this);
        if(predecessors != null) {
            try {
                inputSchema = predecessors.get(0).getSchema();
            } catch (FrontendException fee) {
                return null;
            }
        }
        
        if(inputSchema == null) {
            return null;
        }
        
        if(Schema.equals(inputSchema, outputSchema, false, true)) {
            //there is a one is to one mapping between input and output schemas
            return new ProjectionMap(false);
        } else {
            //problem - input and output schemas for a store have to match!
            return null;
        }
    }

    @Override
    public List<RequiredFields> getRequiredFields() {
        List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();
        requiredFields.add(new RequiredFields(false, true));
        return requiredFields;
    }

}
