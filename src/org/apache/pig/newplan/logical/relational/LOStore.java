/**
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
package org.apache.pig.newplan.logical.relational;

import org.apache.pig.SortInfo;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;

public class LOStore extends LogicalRelationalOperator {

    private final FileSpec output;

    // If we know how to reload the store, here's how. The lFile
    // FileSpec is set in PigServer.postProcess. It can be used to
    // reload this store, if the optimizer has the need.
    private FileSpec mInputSpec;
    private final String signature;
    private boolean isTmpStore;
    private SortInfo sortInfo;
    private final StoreFuncInterface storeFunc;

    public LOStore(LogicalPlan plan, FileSpec outputFileSpec, StoreFuncInterface storeFunc, String signature) {
        super("LOStore", plan);
        this.output = outputFileSpec;
        this.storeFunc = storeFunc;
        this.signature = signature;
    }
    
    public FileSpec getOutputSpec() {
        return output;
    }
    
    public StoreFuncInterface getStoreFunc() {
        return storeFunc;
    }
    
    @Override
    public LogicalSchema getSchema() throws FrontendException {
        schema = ((LogicalRelationalOperator)plan.getPredecessors(this).get(0)).getSchema();
        return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LOStore) {
            LOStore os = (LOStore)other;
            if (!checkEquality(os)) return false;
            // No need to test that storeFunc is equal, since it's
            // being instantiated from output
            if (output == null && os.output == null) {
                return true;
            } else if (output == null || os.output == null) {
                return false;
            } else {
                return output.equals(os.output);
            }
        } else {
            return false;
        }
    }
    
    public SortInfo getSortInfo() {
        return sortInfo;
    }

    public void setSortInfo(SortInfo sortInfo) {
        this.sortInfo = sortInfo;
    }
    
    public boolean isTmpStore() {
        return isTmpStore;
    }

    public void setTmpStore(boolean isTmpStore) {
        this.isTmpStore = isTmpStore;
    }
    
    public void setInputSpec(FileSpec in) {
        mInputSpec = in;
    }

    public FileSpec getInputSpec() {
        return mInputSpec;
    }
    
    public String getSignature() {
        return signature;
    }

    public FileSpec getFileSpec() {
        return output;
    }

}
