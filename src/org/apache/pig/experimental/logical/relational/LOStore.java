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
package org.apache.pig.experimental.logical.relational;

import java.io.IOException;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;

public class LOStore extends LogicalRelationalOperator {
    private static final long serialVersionUID = 2L;

    private FileSpec output;  
    transient private StoreFuncInterface storeFunc;
    
    //private static Log log = LogFactory.getLog(LOStore.class);
    
    public LOStore(LogicalPlan plan) {
        super("LOStore", plan);
    }
    
    public LOStore(LogicalPlan plan, FileSpec outputFileSpec) {
        super("LOStore", plan);

        output = outputFileSpec;
      
        try { 
             storeFunc = (StoreFuncInterface) PigContext.instantiateFuncFromSpec(outputFileSpec.getFuncSpec()); 
        } catch (Exception e) { 
            throw new RuntimeException("Failed to instantiate StoreFunc.", e);
        }
    }
    
    public FileSpec getOutputSpec() {
        return output;
    }
    
    public StoreFuncInterface getStoreFunc() {
        return storeFunc;
    }
    
    @Override
    public LogicalSchema getSchema() {
        try {
            return ((LogicalRelationalOperator)plan.getPredecessors(this).get(0)).getSchema();
        }catch(Exception e) {
            throw new RuntimeException("Unable to get predecessor of LOStore.", e);
        }
    }

    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalPlanVisitor)) {
            throw new IOException("Expected LogicalPlanVisitor");
        }
        ((LogicalPlanVisitor)v).visitLOStore(this);
    }

    @Override
    public boolean isEqual(Operator other) {
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
}
