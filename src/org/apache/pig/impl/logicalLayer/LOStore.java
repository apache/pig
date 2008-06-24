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
import java.util.List;
import java.util.Map;

import org.apache.pig.StoreFunc; 
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOStore extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private FileSpec mOutputFile;
    private StoreFunc mStoreFunc;
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

    public StoreFunc getStoreFunc() {
        return mStoreFunc;
    }

    @Override
    public String name() {
        return "Store " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public Schema getSchema() throws RuntimeException {
        throw new RuntimeException("Internal error: Requested schema of a "
                + "store operator.");
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }
}
